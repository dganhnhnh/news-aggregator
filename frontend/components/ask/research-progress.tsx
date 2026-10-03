'use client'

import { Check, ChevronDown, LoaderCircle, Wrench, X } from 'lucide-react'
import type { ChatMessage } from '@/lib/research-message'

export function ResearchProgress({
  message, onToggle,
}: {
  message: ChatMessage
  onToggle: () => void
}) {
  const steps = message.steps ?? []
  if (!steps.length && !message.pending) return null
  const open = message.reasoningOpen ?? !message.answerStarted
  const active = message.pending && !message.answerStarted
  const panelId = `research-progress-${message.id}`

  return (
    <div className="mb-4 rounded-lg border border-border bg-muted/20 text-sm">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        onClick={onToggle}
        className="flex w-full items-center gap-2 rounded-lg px-3 py-2.5 text-left text-muted-foreground hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {active && <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />}
        <span className="flex-1 font-medium">Reasoning & tools{steps.length ? ` · ${steps.length}` : ''}</span>
        <ChevronDown className={`size-4 transition-transform ${open ? 'rotate-180' : ''}`} aria-hidden="true" />
      </button>
      <div id={panelId} hidden={!open} className="max-h-80 overflow-y-auto border-t border-border px-3 py-3 scrollbar-thin">
        {!steps.length && <p className="text-muted-foreground">Thinking…</p>}
        <ol className="space-y-4">
          {steps.map((step) => (
            <li key={`${step.kind}:${step.id}`}>
              {step.kind === 'reasoning' ? (
                <p className="whitespace-pre-wrap break-words leading-relaxed text-muted-foreground">{step.text}</p>
              ) : (
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <Wrench className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                    <span className="min-w-0 flex-1 break-all font-mono text-xs">{step.tool}</span>
                    {step.status === 'completed' ? <Check className="size-3.5 text-primary" aria-hidden="true" />
                      : step.status === 'error' ? <X className="size-3.5 text-destructive" aria-hidden="true" />
                        : message.pending ? <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" /> : null}
                    <span className="text-xs text-muted-foreground">
                      {step.status === 'running' ? (message.pending ? 'Running' : 'Interrupted')
                        : step.status === 'error' ? 'Failed' : 'Completed'}
                    </span>
                  </div>
                  {step.input && <pre className="whitespace-pre-wrap break-all rounded bg-muted/60 p-2 text-xs text-muted-foreground">{step.input}</pre>}
                  {step.output && <pre className="whitespace-pre-wrap break-all border-l-2 border-border pl-3 text-xs leading-relaxed text-muted-foreground">{step.output}</pre>}
                </div>
              )}
            </li>
          ))}
        </ol>
      </div>
    </div>
  )
}
