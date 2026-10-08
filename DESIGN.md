# Design

The plugin is an MCP ingress into a small Python runtime. A single daemon owns one long-lived Codex app-server per configured host. SQLite is authoritative for worker projections, ordered events, command receipts, pending runtime requests, and the notification outbox.

Commands reserve a caller-scoped receipt before dispatch. Matching retries return the original receipt; conflicting arguments are refused. A lost provider response is indeterminate and is never replayed automatically. Native thread ids are persisted before turn submission so an explicit later `send`, `fork`, or `rollback` can resume the same thread.

Outbox claims use an expiry and fencing token. Delivery is at-least-once. The daemon uses an owner-only Unix socket, private state files, bounded HTTP bodies, and exact-child process cleanup. Provider output and request payloads are kept out of notification messages; read tools and `pending_requests` are authoritative.

The default provider is the normal Codex CLI configuration. An external OpenAI-compatible router and lease admission are optional environment-configured integrations. The optional SSH runtime uses an explicitly configured host and never transfers local credentials. Unsupported app-server methods fail clearly based on the schema installed on that host.

## Queued sends and steer races

`send(mode:"queue")` appends a durable FIFO message to the worker projection. One queued message is submitted as one turn after each terminal turn, including failed or interrupted turns. Explicit stop moves remaining messages to a durable dropped record with its reason. A steer rejected because its expected turn ended is drained and submitted once as a fresh turn; the message stays queued if the replacement turn is still completing.
