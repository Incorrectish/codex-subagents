import json
import sqlite3
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from backend.codex_subagents import Backend,Worker
from backend.store import SQLiteStore
from tests.test_backend import FakeAppServer


class StoreTests(unittest.TestCase):
    def test_json_import_preserves_ack_and_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            worker=Worker('w','native',directory,'model','low','completed',1,2,owner='a')
            (root/'workers.json').write_text(json.dumps({'w':asdict(worker)}))
            (root/'events.json').write_text(json.dumps([{'eventId':'e','workerId':'w','status':'completed','createdAt':2,'owner':'a','acked':['a']}]))
            backend=Backend(root,FakeAppServer())
            try:
                self.assertEqual(backend.status('w','a')['owner'],'a')
                self.assertEqual(backend.claim_events('a'),[])
                self.assertEqual(backend.read_events('a')['events'][0]['eventId'],'e')
            finally: backend.close()

    def test_v0_column_store_preserves_sequences_and_removes_old_fk(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'state.sqlite3'
            db=sqlite3.connect(path)
            db.executescript('''CREATE TABLE workers(id TEXT PRIMARY KEY,thread_id TEXT,cwd TEXT,model TEXT,effort TEXT,status TEXT,created_at INTEGER,updated_at INTEGER,owner TEXT);
                INSERT INTO workers VALUES('w','native','/tmp','model','low','completed',1,2,'a');
                CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE,worker_id TEXT,owner TEXT,status TEXT,answer TEXT,error TEXT,payload_json TEXT,created_at INTEGER,FOREIGN KEY(worker_id) REFERENCES workers(id));
                CREATE TABLE outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,event_seq INTEGER UNIQUE,consumer TEXT,state TEXT,lease_owner TEXT,lease_until REAL,attempts INTEGER,created_at INTEGER,FOREIGN KEY(event_seq) REFERENCES events(seq));
                INSERT INTO events VALUES(42,'e','w','a','completed',NULL,NULL,'{}',2);
                INSERT INTO outbox VALUES(1,42,'a','pending',NULL,NULL,0,2);
            ''')
            db.commit(); db.close()
            backend=Backend(Path(directory),FakeAppServer())
            try:
                self.assertEqual(backend.store.cursor(),42)
                backend.spawn('new',directory,owner='a')
                self.assertGreater(backend.store.cursor(),42)
                self.assertEqual(backend.store.db.execute('PRAGMA foreign_key_check').fetchall(),[])
                self.assertEqual(backend.claim_events('a')[0]['seq'],42)
            finally: backend.close()

    def test_migration_refuses_downgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'state.sqlite3'
            db=sqlite3.connect(path); db.execute('PRAGMA user_version=999'); db.close()
            with self.assertRaisesRegex(RuntimeError,'newer'): SQLiteStore(path)

    def test_event_pages_return_last_delivered_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            backend=Backend(Path(directory),FakeAppServer())
            try:
                w=backend.spawn('p',directory,owner='a')
                with backend.lock,backend.store.transaction():
                    for i in range(105): backend._emit(backend.workers[w.id],{'kind':'test','index':i},notify=False)
                first=backend.read_events('a')
                self.assertEqual(len(first['events']),100)
                self.assertEqual(first['cursor'],first['events'][-1]['seq'])
                self.assertLess(first['cursor'],first['highWatermark'])
                second=backend.read_events('a',first['cursor'])
                self.assertEqual(len(second['events']),6)
                self.assertEqual(second['cursor'],second['highWatermark'])
            finally: backend.close()

    def test_busy_legacy_daemon_port_is_reserved_before_recovery(self):
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from backend.codex_subagents import run_server
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            path=root/'workers.json'; path.write_text('live legacy state; must not be read')
            server=HTTPServer(('127.0.0.1',0),BaseHTTPRequestHandler)
            try:
                run_server('127.0.0.1',server.server_port,root)
                self.assertFalse((root/'state.sqlite3').exists())
                self.assertEqual(path.read_text(),'live legacy state; must not be read')
            finally: server.server_close()
