import type { ResearchCitation, ResearchStreamEvent } from './types'

export type ResearchStep =
  | { kind: 'reasoning'; id: string; text: string }
  | {
      kind: 'tool'
      id: string
      tool: string
      input: string
      status: 'running' | 'completed' | 'error'
      output?: string
    }

export type ChatMessage = {
  id: string
  role: 'user' | 'assistant'
  content: string
  query?: string
  citations?: ResearchCitation[]
  status?: 'searching' | 'reading' | 'synthesizing' | 'verifying'
  pending?: boolean
  error?: string
  steps?: ResearchStep[]
  reasoningOpen?: boolean
  answerStarted?: boolean
}

export function applyResearchEvent(
  message: ChatMessage,
  event: ResearchStreamEvent,
): ChatMessage {
  switch (event.event) {
    case 'status':
      return { ...message, status: event.data.phase }
    case 'reasoning': {
      if (!event.data.text) return message
      const steps = [...(message.steps ?? [])]
      const index = steps.findIndex((step) => step.kind === 'reasoning' && step.id === event.data.id)
      const previous = steps[index]
      if (previous?.kind === 'reasoning') {
        steps[index] = { ...previous, text: previous.text + event.data.text }
      } else {
        steps.push({ kind: 'reasoning', ...event.data })
      }
      return { ...message, steps }
    }
    case 'tool_call': {
      const steps = [...(message.steps ?? [])]
      const index = steps.findIndex((step) => step.kind === 'tool' && step.id === event.data.id)
      const previous = steps[index]
      const step: ResearchStep = {
        kind: 'tool', status: 'running',
        ...(previous?.kind === 'tool' ? previous : {}), ...event.data,
      }
      if (index < 0) steps.push(step)
      else steps[index] = step
      return { ...message, steps }
    }
    case 'tool_result': {
      const steps = [...(message.steps ?? [])]
      const index = steps.findIndex((step) => step.kind === 'tool' && step.id === event.data.id)
      const previous = steps[index]
      const step: ResearchStep = {
        kind: 'tool', tool: 'Tool', input: '',
        ...(previous?.kind === 'tool' ? previous : {}), ...event.data,
      }
      if (index < 0) steps.push(step)
      else steps[index] = step
      return { ...message, steps }
    }
    case 'delta':
      if (!event.data.text) return message
      return {
        ...message,
        content: message.content + event.data.text,
        status: undefined,
        answerStarted: true,
        reasoningOpen: message.answerStarted ? message.reasoningOpen : false,
      }
    case 'reset':
      return {
        ...message, content: '', citations: [],
        ...(event.data.reason === 'tool_call' || event.data.reason === 'citation_repair'
          ? { answerStarted: false, reasoningOpen: true }
          : {}),
      }
    case 'citations':
      return { ...message, citations: event.data.citations }
    case 'done':
      return { ...message, pending: false, status: undefined }
    case 'error':
      return {
        ...message, pending: false, status: undefined,
        error: `${event.data.error_type === 'InvalidResearchCitation'
          ? 'Some cited sources could not be verified. Please retry or start a new chat.'
          : event.data.reason} · Request ${event.data.request_id}`,
      }
    default:
      return message
  }
}
