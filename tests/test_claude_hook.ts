import { addSpawnParent } from "../hooks/register.ts";

Deno.test("Claude hook sends its native session as spawn parent", () => {
  const actual = addSpawnParent({ prompt: "p", cwd: "/tmp" }, "claude-session-1");
  if (JSON.stringify(actual) !== JSON.stringify({ prompt: "p", cwd: "/tmp", parentId: "claude-session-1" })) {
    throw new Error(`unexpected payload: ${JSON.stringify(actual)}`);
  }
});
