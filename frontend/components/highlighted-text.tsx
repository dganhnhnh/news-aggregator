import type { TextHighlight } from '@/lib/types'

/** Renders `ts_headline()` segments from the API, falling back to plain text. */
export function HighlightedText({
  segments,
  fallback,
}: {
  segments: TextHighlight[] | null
  fallback: string | null
}) {
  if (!segments || segments.length === 0) return <>{fallback}</>
  return (
    <>
      {segments.map((segment, i) =>
        segment.highlight ? (
          <mark
            key={i}
            className="rounded-sm bg-primary/25 text-inherit box-decoration-clone"
          >
            {segment.text}
          </mark>
        ) : (
          segment.text
        ),
      )}
    </>
  )
}
