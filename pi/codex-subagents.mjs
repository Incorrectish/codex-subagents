import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Pi has no built-in MCP client. This is the small stdio JSON-RPC subset needed
// by MCP (initialize, tools/list, tools/call, and server notifications).
function mcpEnvironment(consumer) {
  return { ...process.env, CLAUDE_SESSION_ID: consumer, CLAUDE_CODE_SESSION_ID: consumer, PI_SESSION_ID: consumer };
}

class McpStdioClient {
  constructor({ root, consumer, onNotice }) {
    this.root = root;
    this.consumer = consumer;
    this.onNotice = onNotice;
    this.nextId = 1;
    this.pending = new Map();
    this.buffer = "";
    this.process = undefined;
    this.startPromise = undefined;
    this.closed = false;
    this.tools = [];
  }

  async start() {
    if (this.closed) throw new Error("codex-subagents MCP client is closed");
    if (this.process) return;
    if (this.startPromise) return this.startPromise;
    this.startPromise = (async () => {
      const python = process.env.CODEX_SUBAGENTS_PYTHON || "python3";
      const child = spawn(python, [join(this.root, "mcp_server.py")], {
        cwd: this.root,
        env: mcpEnvironment(this.consumer),
        stdio: ["pipe", "pipe", "pipe"],
      });
      this.process = child;
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => this.read(chunk));
      child.stderr.resume();
      child.on("error", (error) => this.fail(error));
      child.on("exit", (code, signal) => {
        if (!this.closed && (code !== 0 || signal)) this.fail(new Error(`codex-subagents MCP exited (${code ?? signal})`));
      });
      await this.request("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "pi-codex-subagents", version: "1.0.0" } });
      this.notify("notifications/initialized", {});
      this.tools = (await this.request("tools/list", {})).tools || [];
    })();
    try { await this.startPromise; } finally { this.startPromise = undefined; }
  }

  read(chunk) {
    this.buffer += chunk;
    for (;;) {
      const newline = this.buffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.buffer.slice(0, newline).trim();
      this.buffer = this.buffer.slice(newline + 1);
      if (!line) continue;
      let message;
      try { message = JSON.parse(line); } catch { continue; }
      if (message.id !== undefined && (message.result !== undefined || message.error !== undefined)) {
        const waiter = this.pending.get(message.id);
        if (!waiter) continue;
        this.pending.delete(message.id);
        if (message.error) waiter.reject(new Error(message.error.message || JSON.stringify(message.error)));
        else waiter.resolve(message.result);
      } else if (message.method === "notifications/message") {
        this.onNotice?.(message.params?.data);
      }
    }
  }

  request(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try { this.write({ jsonrpc: "2.0", id, method, params }); }
      catch (error) { this.pending.delete(id); reject(error); }
    });
  }

  notify(method, params) { this.write({ jsonrpc: "2.0", method, params }); }

  write(message) {
    if (!this.process?.stdin || this.process.killed) throw new Error("codex-subagents MCP process is unavailable");
    this.process.stdin.write(`${JSON.stringify(message)}\n`);
  }

  async callTool(name, args, signal) {
    await this.start();
    if (signal?.aborted) throw new Error("tool call aborted");
    const result = await this.request("tools/call", { name, arguments: args });
    if (result?.isError) throw new Error(result.content?.map((item) => item.text || "").join("\n") || `${name} failed`);
    return result?.content || [{ type: "text", text: JSON.stringify(result) }];
  }

  fail(error) {
    for (const waiter of this.pending.values()) waiter.reject(error);
    this.pending.clear();
  }

  async close() {
    this.closed = true;
    this.fail(new Error("codex-subagents MCP client closed"));
    const child = this.process;
    this.process = undefined;
    if (!child) return;
    child.stdin?.end();
    if (!child.killed) child.kill();
  }
}

function findRoot() {
  const here = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    process.env.CODEX_SUBAGENTS_ROOT,
    join(here, ".."),
  ].filter(Boolean);
  const root = candidates.find((candidate) => existsSync(join(candidate, "mcp_server.py")));
  if (!root) throw new Error("codex-subagents root not found; set CODEX_SUBAGENTS_ROOT");
  return root;
}

function loadToolSchemas(root) {
  return JSON.parse(readFileSync(join(root, "tools.json"), "utf8"));
}

function asPiContent(content) {
  return (content || []).map((item) => {
    if (item.type === "text") return item;
    if (item.type === "image") return { type: "image", data: item.data, mimeType: item.mimeType };
    return { type: "text", text: JSON.stringify(item) };
  });
}

export default function codexSubagentsExtension(pi) {
  const root = findRoot();
  const schemas = loadToolSchemas(root);
  const registeredNames = new Set();
  let client;
  let context;
  let startPromise;

  const ensureClient = async (ctx) => {
    context = ctx;
    if (!client) {
      client = new McpStdioClient({
        root,
        consumer: ctx.sessionManager.getSessionId(),
        onNotice: (data) => {
          if (data?.kind === "completion" && data.workerId && data.status) {
            ctx.ui.notify(`worker ${data.workerId} ${data.status}, read with result(${data.workerId})`, "info");
          }
        },
      });
    }
    if (!startPromise) startPromise = client.start().finally(() => { startPromise = undefined; });
    await startPromise;
    for (const tool of client.tools) {
      if (registeredNames.has(tool.name)) continue;
      register(tool);
    }
  };

  const register = (schema) => {
    registeredNames.add(schema.name);
    pi.registerTool({
      name: schema.name,
      label: schema.name,
      description: schema.description,
      parameters: schema.inputSchema || schema.parameters,
      async execute(_toolCallId, args, signal, _onUpdate, ctx) {
        await ensureClient(ctx);
        return { content: asPiContent(await client.callTool(schema.name, args, signal)), details: { mcp: true } };
      },
    });
  };

  for (const schema of schemas) register(schema);

  pi.on("session_start", async (_event, ctx) => {
    try { await ensureClient(ctx); }
    catch (error) { ctx.ui.notify(`codex-subagents unavailable: ${error instanceof Error ? error.message : String(error)}`, "warning"); }
  });
  pi.on("session_shutdown", async () => { await client?.close(); client = undefined; context = undefined; });
}

export { McpStdioClient, asPiContent, findRoot, loadToolSchemas, mcpEnvironment };
