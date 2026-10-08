type Register = (on: any) => void

type Api = { consumer: string; endpoint: string; fetch(path: string, init?: { method?: string; body?: string }): Promise<any> }

export const addSpawnParent = (payload: Record<string, unknown>, consumer: string) => ({ ...payload, parentId: consumer })

async function makeApi($: any): Promise<Api> {
  const result = await $.process.run(['python3', '-c', 'import os, pathlib; print(pathlib.Path(os.environ.get("CODEX_SUBAGENTS_STATE_DIR", pathlib.Path.home()/".codex-subagents"))/"token")'])
  const token = (await $.fs.read(result.stdout.trim())).trim()
  const endpointResult = await $.process.run(['python3', '-c', 'import json, os, pathlib; print(pathlib.Path(os.environ.get("CODEX_SUBAGENTS_STATE_DIR", pathlib.Path.home()/".codex-subagents"))/"endpoint.json")'])
  const endpoint = JSON.parse(await $.fs.read(endpointResult.stdout.trim()))
  if (!Number.isInteger(endpoint.httpPort) || endpoint.httpPort < 1) throw new Error('codex-subagents endpoint has no hook port')
  const consumer = await $.session.id()
  const base = `http://127.0.0.1:${endpoint.httpPort}`
  return {
    consumer,
    endpoint: base,
    fetch: async (path, init = {}) => {
      const response = await $.http.fetch(`${base}${path}`, {
        method: init.method ?? 'GET', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: init.body,
      })
      if (!response.ok) throw new Error(`codex-subagents ${response.status}: ${response.text}`)
      return JSON.parse(response.text)
    },
  }
}

export const register: Register = (on: any) => {
  let api: Api | undefined
  let delivering = false
  on('session.start', async ($: any, e: any, next: any) => {
    const started = await next(e)
    await $.process.run(['python3', `${$.plugin.root}/backend/codex_subagents.py`, '--detach'], { timeoutMs: 5000 })
    for (let i = 0; i < 50; i += 1) {
      try { api = await makeApi($); const state = await api.fetch(`/v1/list?session=${encodeURIComponent(api.consumer)}`); if (state.version !== '0.2.0') throw new Error('Older daemon owns the codex-subagents endpoint; finish its workers and restart that daemon'); break }
      catch { api = undefined; await $.clock.sleep(100) }
    }
    if (!api) throw new Error('codex-subagents backend did not start')
    const tools = JSON.parse(await $.fs.read(`${$.plugin.root}/tools.json`))
    await Promise.all(tools.map((tool: any) => $.tool.register(tool)))
    $.clock.every(500, async () => {
      if (!api || delivering) return
      delivering = true
      try {
        const data = await api.fetch(`/v1/events?consumer=${encodeURIComponent(api.consumer)}`)
        for (const event of data.events ?? []) {
          // Provider text stays in tool/resource results, never prompt text.
          await $.prompt.submit({ text: event.kind === 'request'
            ? `[codex-subagents notification, automated] worker ${event.workerId} has a pending request. Call pending_requests({workerId: "${event.workerId}"}) to inspect it and answer_request to reply.`
            : `[codex-subagents notification, automated] worker ${event.workerId} ${event.status}. Call result("${event.workerId}") to read its output.` })
          await api.fetch('/v1/ack-events', { method: 'POST', body: JSON.stringify({ consumer: api.consumer, claims: [{ eventId: event.eventId, leaseId: event.leaseId }] }) })
        }
      } catch (error) { $.ui.log(`codex-subagents notification delivery: ${String(error)}`, { to: 'debug' }) }
      finally { delivering = false }
    })
    return started
  })

  const names = ['spawn', 'send', 'wait', 'status', 'result', 'stop', 'list', 'pending_requests', 'answer_request', 'fork', 'rollback', 'capabilities', 'events']
  for (const name of names) {
    on('tool.call', { tool: `mcp__codex-subagents__${name}` }, async ($: any, e: any) => {
      if (!api) return { deny: 'codex-subagents is not ready' }
      const { consumer } = api
      let value: unknown
      if (name === 'list' || name === 'pending_requests' || name === 'events') {
        const path = name === 'list' ? 'list' : name === 'events' ? 'event-log' : 'requests'
        const query = `session=${encodeURIComponent(consumer)}&all=${e.all === true}&after=${e.after ?? 0}${e.workerId ? `&workerId=${encodeURIComponent(e.workerId)}` : ''}`
        value = await api.fetch(`/v1/${path}?${query}`)
      } else {
        const path = name === 'answer_request' ? 'answer-request' : name
        const fields = ['prompt', 'cwd', 'model', 'effort', 'host', 'approvalPolicy', 'clientRequestId', 'id', 'message', 'ids', 'timeoutSec', 'all', 'allowOtherSession', 'requestId', 'answer', 'lastTurnId', 'beforeTurnId', 'contextHandoff']
        const payload: Record<string, unknown> = { session: consumer }
        if (name === 'spawn') Object.assign(payload, addSpawnParent(payload, consumer))
        for (const key of fields) if (e[key] !== undefined) payload[key] = e[key]
        value = await api.fetch(`/v1/${path}`, { method: 'POST', body: JSON.stringify(payload) })
      }
      return { result: value, text: JSON.stringify(value) }
    })
  }
}
