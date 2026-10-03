import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'

// Run the production reducer without adding a browser or test framework dependency.
const source = readFileSync(new URL('../lib/research-message.ts', import.meta.url), 'utf8')
const { outputText } = ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
})
const { applyResearchEvent: apply } = await import(`data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`)
const fresh = () => ({ id: 'answer', role: 'assistant', content: '', pending: true, steps: [], reasoningOpen: true, answerStarted: false })

test('first answer token collapses progress; reopening survives remaining tokens and completion', () => {
  let message = apply(fresh(), { event: 'reasoning', data: { id: 'r1', text: 'Check ' } })
  message = apply(message, { event: 'reasoning', data: { id: 'r1', text: 'sources.' } })
  assert.equal(message.steps[0].text, 'Check sources.')
  assert.equal(message.reasoningOpen, true)
  message = apply(message, { event: 'delta', data: { text: '' } })
  assert.equal(message.reasoningOpen, true)
  message = apply(message, { event: 'delta', data: { text: 'Final' } })
  assert.equal(message.reasoningOpen, false)
  message = { ...message, reasoningOpen: true }
  message = apply(message, { event: 'delta', data: { text: ' answer.' } })
  message = apply(message, { event: 'done', data: {} })
  assert.equal(message.content, 'Final answer.')
  assert.equal(message.reasoningOpen, true)
  assert.equal(message.pending, false)
  assert.equal(message.steps[0].text, 'Check sources.')
})

test('parallel calls with the same name are matched by ID, not completion order', () => {
  let message = fresh()
  for (const id of ['a', 'b']) message = apply(message, {
    event: 'tool_call', data: { id, tool: 'search_articles', input: id },
  })
  message = apply(message, { event: 'tool_result', data: { id: 'b', status: 'error', output: 'Failed' } })
  message = apply(message, { event: 'tool_result', data: { id: 'a', status: 'completed', output: 'Found' } })
  assert.deepEqual(message.steps.map(({ id, status }) => [id, status]), [['a', 'completed'], ['b', 'error']])
  assert.equal(message.reasoningOpen, true)
})

test('late tool calls reopen progress; validation reset retains steps and manual toggle choice', () => {
  let message = apply(fresh(), { event: 'delta', data: { text: 'Checking…' } })
  message = apply(message, { event: 'reasoning', data: { id: 'preamble', text: message.content } })
  message = apply(message, { event: 'reset', data: { reason: 'tool_call' } })
  assert.equal(message.reasoningOpen, true)
  assert.equal(message.answerStarted, false)
  assert.equal(message.content, '')
  message = apply(message, { event: 'delta', data: { text: 'Unsupported [A99]' } })
  assert.equal(message.reasoningOpen, false)
  message = { ...message, reasoningOpen: true }
  message = apply(message, { event: 'reset', data: {} })
  message = apply(message, { event: 'error', data: { reason: 'Invalid citation', request_id: 'req' } })
  assert.equal(message.content, '')
  assert.equal(message.steps.length, 1)
  assert.equal(message.reasoningOpen, true)
  assert.equal(message.pending, false)
})

test('old saved messages and answer-only models remain compatible', () => {
  const message = apply({ id: 'old', role: 'assistant', content: '' }, { event: 'delta', data: { text: 'Answer' } })
  assert.equal(message.reasoningOpen, false)
  assert.equal(message.content, 'Answer')
})

test('citation repair clears the rejected draft and reopens progress until corrected answer starts', () => {
  let message = apply(fresh(), { event: 'delta', data: { text: 'Unverified [A99]' } })
  message = apply(message, { event: 'reset', data: { reason: 'citation_repair' } })
  message = apply(message, { event: 'status', data: { phase: 'verifying' } })
  assert.equal(message.content, '')
  assert.equal(message.reasoningOpen, true)
  assert.equal(message.answerStarted, false)
  assert.equal(message.status, 'verifying')
  message = apply(message, { event: 'delta', data: { text: 'Verified [A1]' } })
  assert.equal(message.reasoningOpen, false)
  assert.equal(message.content, 'Verified [A1]')
})

test('citation failure explains verification problem without showing raw internal diagnostic', () => {
  const message = apply(fresh(), { event: 'error', data: {
    error_type: 'InvalidResearchCitation', reason: 'Citation A99 is not present in retrieved evidence', request_id: 'req',
  } })
  assert.match(message.error, /could not be verified/)
  assert.match(message.error, /Request req/)
  assert.doesNotMatch(message.error, /A99/)
})
