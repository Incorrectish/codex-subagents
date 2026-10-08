import { addSpawnParent, buildToolPayload, forwardToolArguments } from "../hooks/register.ts";

Deno.test("Claude hook sends its native session as spawn parent", () => {
  const actual = addSpawnParent({ prompt: "p", cwd: "/tmp" }, "claude-session-1");
  if (JSON.stringify(actual) !== JSON.stringify({ prompt: "p", cwd: "/tmp", parentId: "claude-session-1" })) {
    throw new Error(`unexpected payload: ${JSON.stringify(actual)}`);
  }
});

Deno.test("Claude hook forwards every tools.json argument, including queued send mode", async () => {
  const tools = JSON.parse(await Deno.readTextFile(new URL("../tools.json", import.meta.url)));
  const send = tools.find((tool: any) => tool.name === "send");
  const sendEvent = { id: "worker-1", message: "queued", mode: "queue", allowOtherSession: true, clientRequestId: "request-1" };
  if (JSON.stringify(buildToolPayload(send, sendEvent, "claude-session-1")) !== JSON.stringify({ session: "claude-session-1", ...sendEvent })) {
    throw new Error(`send payload dropped an argument: ${JSON.stringify(buildToolPayload(send, sendEvent, "claude-session-1"))}`);
  }
  for (const tool of tools) {
    const event = Object.fromEntries(Object.keys(tool.inputSchema.properties ?? {}).map((key) => [key, `value-${key}`]));
    const actual = forwardToolArguments(tool, event);
    if (JSON.stringify(actual) !== JSON.stringify(event)) {
      throw new Error(`${tool.name} payload dropped an argument: ${JSON.stringify(actual)}`);
    }
  }
});
