import assert from "node:assert/strict";
import { test } from "node:test";
import { McpStdioClient, loadToolSchemas, mcpEnvironment } from "../pi/codex-subagents.mjs";

const root = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

test("Pi extension schemas cover the MCP server tools", async () => {
  const schemas = loadToolSchemas(root);
  assert.ok(schemas.length >= 13);
  assert.deepEqual(schemas.slice(0, 7).map((tool) => tool.name), ["spawn", "send", "wait", "status", "result", "stop", "list"]);
  assert.ok(schemas.some((tool) => tool.name === "fork"));
  assert.ok(schemas.every((tool) => tool.inputSchema?.type === "object"));
});

test("Pi MCP client passes its native session as the spawn parent", () => {
  const env = mcpEnvironment("pi-session-1");
  assert.equal(env.PI_SESSION_ID, "pi-session-1");
  assert.equal(env.CLAUDE_SESSION_ID, "pi-session-1");
});

test("Pi MCP client forwards every tools.json argument unchanged", async () => {
  const client = new McpStdioClient({ root, consumer: "pi-test-session", onNotice() {} });
  const schemas = loadToolSchemas(root);
  const calls = [];
  client.start = async () => {};
  client.request = async (method, params) => {
    calls.push({ method, params });
    return { content: [{ type: "text", text: "{}" }], isError: false };
  };
  for (const schema of schemas) {
    const args = Object.fromEntries(Object.keys(schema.inputSchema.properties || {}).map((key) => [key, `value-${key}`]));
    await client.callTool(schema.name, args);
    assert.deepEqual(calls.at(-1), { method: "tools/call", params: { name: schema.name, arguments: args } });
  }
});

test("real MCP server initializes and lists tools over stdio", async () => {
  const client = new McpStdioClient({ root, consumer: "pi-test-session", onNotice() {} });
  try {
    await client.start();
    const listed = await client.request("tools/list", {});
    const names = new Set(listed.tools.map((tool) => tool.name));
    assert.ok(names.has("spawn"));
    assert.ok(names.has("pending_requests"));
    assert.ok(names.has("fork"));
  } finally {
    await client.close();
  }
});

test("mock Pi API registers every tool and closes the client on shutdown", async () => {
  const extension = await import("../pi/codex-subagents.mjs");
  const registered = [];
  const handlers = new Map();
  const pi = {
    registerTool(tool) { registered.push(tool); },
    on(event, handler) { handlers.set(event, handler); },
  };
  extension.default(pi);
  const schemas = loadToolSchemas(root);
  assert.deepEqual(registered.map((tool) => tool.name), schemas.map((tool) => tool.name));
  assert.ok(handlers.has("session_start"));
  assert.ok(handlers.has("session_shutdown"));
});
