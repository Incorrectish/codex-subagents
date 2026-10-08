"""SQLite projections, receipt-backed commands, and a leased notification outbox.

The backend serializes access. Nested transactions share the outer transaction;
there are no hidden commits between projection/event/outbox/receipt writes.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable


class SQLiteStore:
    MIGRATIONS = (
        """
        CREATE TABLE IF NOT EXISTS worker_state(id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(
          seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
          worker_id TEXT NOT NULL, owner TEXT NOT NULL, status TEXT NOT NULL,
          answer TEXT, error TEXT, payload_json TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(
          id INTEGER PRIMARY KEY AUTOINCREMENT, event_seq INTEGER NOT NULL UNIQUE,
          consumer TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
          lease_owner TEXT, lease_until REAL, attempts INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL, FOREIGN KEY(event_seq) REFERENCES events(seq));
        CREATE INDEX IF NOT EXISTS events_owner_seq ON events(owner,seq);
        """,
        """
        CREATE TABLE IF NOT EXISTS commands(
          owner TEXT NOT NULL, command_id TEXT NOT NULL, operation TEXT NOT NULL,
          fingerprint TEXT NOT NULL, state TEXT NOT NULL, response TEXT, error TEXT,
          created_at INTEGER NOT NULL, PRIMARY KEY(owner,command_id));
        """,
        """
        CREATE TABLE IF NOT EXISTS requests(
          id TEXT PRIMARY KEY, worker_id TEXT NOT NULL, runtime TEXT NOT NULL,
          provider_id TEXT NOT NULL, method TEXT NOT NULL, payload TEXT NOT NULL,
          status TEXT NOT NULL, answer TEXT, created_at INTEGER NOT NULL);
        """,
    )

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.depth = 0
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version > len(self.MIGRATIONS):
            self.db.close()
            raise RuntimeError('database was created by a newer daemon')
        for index in range(version, len(self.MIGRATIONS)):
            self.db.executescript('BEGIN IMMEDIATE;\n' + self.MIGRATIONS[index] + f'\nPRAGMA user_version={index+1};\nCOMMIT;')
        # The v0 store referenced the old workers table. Preserve cursors and
        # leases while removing that foreign key from the new projection model.
        if any(row['table']=='workers' for row in self.db.execute('PRAGMA foreign_key_list(events)')):
            self.db.execute('PRAGMA foreign_keys=OFF')
            self.db.executescript("""
                BEGIN IMMEDIATE;
                ALTER TABLE outbox RENAME TO v0_outbox;
                ALTER TABLE events RENAME TO v0_events;
                DROP INDEX IF EXISTS events_owner_seq;
                """ + self.MIGRATIONS[0] + """
                INSERT INTO events SELECT * FROM v0_events;
                INSERT INTO outbox SELECT * FROM v0_outbox;
                DROP TABLE v0_outbox;
                DROP TABLE v0_events;
                COMMIT;
            """)
            self.db.execute('PRAGMA foreign_keys=ON')
        # Import the short-lived v0 column store used before ordered migrations.
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE name='workers'").fetchone():
            with self.transaction():
                for row in self.db.execute('SELECT * FROM workers').fetchall():
                    w = dict(row)
                    for field in ('history', 'token_usage', 'rate_limits'):
                        w[field] = json.loads(w.pop(field + '_json', '[]' if field == 'history' else '{}'))
                    self.db.execute('INSERT OR IGNORE INTO worker_state VALUES(?,?)', (w['id'], json.dumps(w)))

    @contextmanager
    def transaction(self):
        outer = self.depth == 0
        if outer:
            self.db.execute('BEGIN IMMEDIATE')
        self.depth += 1
        try:
            yield self
            if outer:
                self.db.execute('COMMIT')
        except BaseException:
            if outer:
                self.db.execute('ROLLBACK')
            raise
        finally:
            self.depth -= 1

    def load_workers(self) -> list[dict[str, Any]]:
        return [json.loads(row[0]) for row in self.db.execute('SELECT data FROM worker_state ORDER BY id')]

    def upsert_workers(self, workers: Iterable[dict[str, Any]]) -> None:
        with self.transaction():
            for w in workers:
                self.db.execute('INSERT INTO worker_state VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data', (w['id'], json.dumps(w, sort_keys=True)))

    @staticmethod
    def _event(row):
        event = json.loads(row['payload_json'])
        event.update(eventId=row['event_id'], workerId=row['worker_id'], owner=row['owner'], seq=row['seq'], createdAt=row['created_at'])
        return event

    def load_events(self, owner: str | None = None, after: int = 0, limit: int = 100):
        query = 'SELECT * FROM events WHERE seq>?'
        args: list[Any] = [after]
        if owner is not None:
            query += ' AND owner=?'
            args.append(owner)
        args.append(limit)
        return [self._event(row) for row in self.db.execute(query + ' ORDER BY seq LIMIT ?', args)]

    def cursor(self) -> int:
        return self.db.execute('SELECT COALESCE(MAX(seq),0) FROM events').fetchone()[0]

    def append_event(self, event: dict[str, Any], notify: bool = True) -> int:
        with self.transaction():
            self.db.execute('INSERT OR IGNORE INTO events(event_id,worker_id,owner,status,answer,error,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)',
                            (event['eventId'], event['workerId'], event['owner'], event.get('status',''), event.get('answer'), event.get('error'), json.dumps(event), event['createdAt']))
            seq = self.db.execute('SELECT seq FROM events WHERE event_id=?', (event['eventId'],)).fetchone()[0]
            if notify:
                self.db.execute('INSERT OR IGNORE INTO outbox(event_seq,consumer,created_at) VALUES(?,?,?)', (seq, event['owner'], event['createdAt']))
            return seq

    def claim_outbox(self, consumer: str, lease_owner: str, lease_seconds: float = 30, limit: int = 100):
        now = time.time()
        with self.transaction():
            rows = self.db.execute("SELECT e.* FROM outbox o JOIN events e ON e.seq=o.event_seq WHERE o.consumer=? AND (o.state='pending' OR (o.state='leased' AND o.lease_until<=?)) ORDER BY e.seq LIMIT ?", (consumer, now, limit)).fetchall()
            events = []
            for row in rows:
                # A fresh fencing token prevents a late ACK from an expired claim.
                import uuid
                lease = str(uuid.uuid4())
                self.db.execute("UPDATE outbox SET state='leased',lease_owner=?,lease_until=?,attempts=attempts+1 WHERE event_seq=?", (lease, now+lease_seconds, row['seq']))
                event = self._event(row)
                event.update(leaseId=lease, leaseUntil=now+lease_seconds)
                events.append(event)
            return events

    def ack_outbox(self, consumer: str, claims: list[dict[str, str]], lease_owner: str = ''):
        with self.transaction():
            for claim in claims:
                self.db.execute("UPDATE outbox SET state='acked',lease_owner=NULL,lease_until=NULL WHERE consumer=? AND state='leased' AND lease_owner=? AND event_seq IN(SELECT seq FROM events WHERE event_id=?)", (consumer, claim['leaseId'], claim['eventId']))

    def reserve_command(self, owner: str, command_id: str, operation: str, fingerprint: str):
        row = self.db.execute('SELECT * FROM commands WHERE owner=? AND command_id=?', (owner, command_id)).fetchone()
        if row:
            if row['operation'] != operation or row['fingerprint'] != fingerprint:
                raise ValueError('clientRequestId was already used with different arguments')
            if row['state'] == 'completed':
                return json.loads(row['response'])
            raise ValueError('command is indeterminate; inspect status before submitting new work')
        with self.transaction():
            self.db.execute('INSERT INTO commands(owner,command_id,operation,fingerprint,state,created_at) VALUES(?,?,?,?,?,?)', (owner, command_id, operation, fingerprint, 'dispatching', int(time.time()*1000)))
        return None

    def finish_command(self, owner, command_id, response):
        self.db.execute("UPDATE commands SET state='completed',response=? WHERE owner=? AND command_id=?", (json.dumps(response), owner, command_id))

    def fail_command(self, owner, command_id, error):
        with self.transaction():
            self.db.execute("UPDATE commands SET state='indeterminate',error=? WHERE owner=? AND command_id=?", (str(error), owner, command_id))

    def close(self):
        self.db.close()
