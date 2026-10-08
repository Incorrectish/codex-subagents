# codex-subagents

`codex-subagents` gives a Claude Code session a durable MCP interface for spawning, steering, inspecting, waiting on, and stopping Codex CLI workers. It runs a small local Python daemon with SQLite state. The default provider is the user's normal Codex configuration; no router, account service, or extra daemon is required.

## Install

From Claude Code, add this checkout as a marketplace and install the plugin:

```text
/plugin marketplace add /path/to/codex-subagents
/plugin install codex-subagents@codex-subagents
```

For a remote Git checkout, replace the path with its repository URL or local checkout path. The only runtime dependency is an installed `codex` command on `PATH` (plus Python 3.10+ and Claude Code's plugin support).

## Configuration

The daemon stores private state in `~/.codex-subagents`; set `CODEX_SUBAGENTS_STATE_DIR` for a disposable or project-specific directory. `CODEX_SUBAGENTS_PROVIDER`, `CODEX_SUBAGENTS_ROUTER_URL`, and `CODEX_SUBAGENTS_ROUTER_API_KEY` optionally select an OpenAI-compatible external router. Router lease admission is disabled by default; enable it with `CODEX_SUBAGENTS_ROUTER_LEASES_ENABLED=1` when the configured router implements `POST /leases`, `GET /leases/tickets/{ticket}`, `POST /leases/{id}/renew`, and `DELETE /leases/{id}`. Lease responses use `{granted, lease_id, pending, ticket}`. The plugin never reads credential files or refreshes credentials.

An optional SSH worker host is enabled by passing `host="remote"` and setting `CODEX_SUBAGENTS_REMOTE_SSH_HOST`. `CODEX_SUBAGENTS_REMOTE_BASE_URL` and `CODEX_SUBAGENTS_REMOTE_KEY_FILE` are optional remote settings. The remote driver launches only the exact Codex child it creates and does not copy local credentials.

## Tools

| Tool | Semantics |
| --- | --- |
| `spawn` | Start a worker for an absolute `cwd`; returns its stable id. |
| `send` | If the worker is running, steer the active turn with `turn/steer`; otherwise start a new turn on the same native thread. |
| `wait` | Wait for selected workers or `any`; wakes for completion or pending approval/question requests. |
| `status` / `result` / `list` | Read worker state, final output, and the caller's inventory. |
| `stop` | Interrupt the worker and known native descendants; a timeout reports `stopping`. |
| `pending_requests` / `answer_request` | Inspect and answer approval or structured-input requests. |
| `fork` / `rollback` | Fork a settled native thread or revert conversation history. Neither changes files or starts inference automatically. |
| `capabilities` | Inspect the selected Codex app-server schema. |
| `events` | Read ordered, session-scoped lifecycle events. |

## Safety and limitations

Workers are owner-scoped to the Claude session that created them. Use an explicit session id when an MCP process will be replaced and must retain ownership. Commands with `clientRequestId` are durable and retry-safe; an uncertain transport result is reported as indeterminate and is never replayed automatically. Delivery of event notifications is at-least-once. Native process loss marks active work interrupted; send explicitly to resume. The plugin cannot undo filesystem changes, discover unrelated shell processes, or guarantee exactly-once model execution after a lost provider connection. Approvals and questions remain pending until answered or closed by the provider.

## Development and verification

```sh
python3 -m unittest discover -s tests -v
claude plugin validate /path/to/codex-subagents
```

The opt-in native test uses a disposable Codex home and a local faux Responses endpoint:

```sh
CODEX_SUBAGENTS_NATIVE_TESTS=1 python3 -m unittest tests.test_native_protocol -v
```
