'use client'

import { useMemo, useState } from 'react'
import { cn } from '@/lib/utils'
import type { TrendDay } from '@/lib/types'

export type TrendMode = 'count' | 'share'

/**
 * Bars are Tailwind divs rather than SVG: `flex-1` plus a percentage height
 * gets responsive widths with no measurement code, and each bar being a real
 * button means hover, focus and screen-reader access come for free. An SVG
 * would need a ResizeObserver and manual hit-testing to match.
 */
function dayLabel(day: string) {
  // The bucket is a UTC calendar day, so it has to be formatted as one. A bare
  // YYYY-MM-DD parses as UTC midnight, which a local-time formatter would
  // render as the previous day for anyone west of Greenwich.
  return new Date(`${day}T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  })
}

export function TrendBarChart({ days, mode }: { days: TrendDay[]; mode: TrendMode }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null)

  const { values, peak, lowSampleFloor } = useMemo(() => {
    const measured = days.filter((d) => d.articles > 0).map((d) => d.articles)
    const sorted = [...measured].sort((a, b) => a - b)
    const median = sorted.length ? sorted[Math.floor(sorted.length / 2)] : 0
    const values = days.map((d) =>
      mode === 'share' ? (d.articles > 0 ? d.matches / d.articles : 0) : d.matches,
    )
    return {
      values,
      peak: Math.max(...values, mode === 'share' ? 0.0001 : 1),
      // A day with 4 articles and 4 matches is 100% share and is noise. The
      // threshold follows the corpus rather than being a magic number, because
      // daily volume here swings between roughly 30 and 480.
      lowSampleFloor: Math.max(5, Math.round(median * 0.25)),
    }
  }, [days, mode])

  const labelEvery = Math.max(1, Math.ceil(days.length / 8))
  const active = activeIndex === null ? null : days[activeIndex]
  // The card tracks the active column, but near either end it anchors to the
  // plot edge instead. A percentage offset plus a -50% translate depends on the
  // card's own width, so clamping the percentage cannot actually guarantee
  // containment -- at the last bar it pushed the card past the panel.
  const cardCenter = activeIndex === null ? 0 : ((activeIndex + 0.5) / days.length) * 100
  const cardAtStart = cardCenter < 12
  const cardAtEnd = cardCenter > 88

  return (
    <div className="flex flex-col gap-2">
      <div className="relative">
        <div className="pointer-events-none absolute inset-x-0 top-0 border-t border-dashed border-border" />
        <span className="pointer-events-none absolute -top-4 right-0 text-[10px] tabular-nums text-muted-foreground">
          {mode === 'share' ? `${(peak * 100).toFixed(1)}%` : peak}
        </span>

        <div
          role="img"
          aria-label={`Mentions per day across ${days.length} days`}
          className="flex h-64 items-end gap-px"
          onMouseLeave={() => setActiveIndex(null)}
        >
          {days.map((day, index) => {
            const noArticles = day.articles === 0
            const lowSample = !noArticles && day.articles < lowSampleFloor
            const heightPct = (values[index] / peak) * 100
            return (
              <button
                key={day.day}
                type="button"
                // One shared readout instead of a Tooltip per bar, which at a
                // 400-day window would mean 400 Base UI roots. The label
                // carries the same information for assistive tech.
                aria-label={[
                  dayLabel(day.day),
                  noArticles
                    ? 'no articles to search'
                    : `${day.matches} of ${day.articles} articles`,
                  day.partial ? 'partial day' : null,
                ]
                  .filter(Boolean)
                  .join(', ')}
                onMouseEnter={() => setActiveIndex(index)}
                onFocus={() => setActiveIndex(index)}
                onBlur={() => setActiveIndex(null)}
                className={cn(
                  'flex h-full min-w-px flex-1 items-end rounded-sm outline-none',
                  'hover:bg-accent/40 focus-visible:bg-accent/60',
                  activeIndex === index && 'bg-accent/40',
                )}
              >
                {noArticles ? (
                  // Nothing to measure that day, not a measured zero: either
                  // the crawler fetched nothing or the filters leave the day
                  // empty. Drawn as a broken baseline so it can never be
                  // misread as a short bar.
                  <span
                    aria-hidden
                    className="w-full border-t-2 border-dotted border-muted-foreground/60"
                  />
                ) : (
                  <span
                    aria-hidden
                    className={cn(
                      'w-full rounded-t-[2px] bg-chart-1 transition-[height]',
                      day.matches === 0 && 'bg-muted-foreground/30',
                      (day.partial || lowSample) && 'opacity-55',
                    )}
                    style={{
                      height: day.matches === 0 ? '2px' : `max(2px, ${heightPct}%)`,
                    }}
                  />
                )}
              </button>
            )
          })}
        </div>

        {active && (
          <div
            // Anchored inside the plot, not above it: the panel gives the
            // chart only a few pixels of headroom, so a card hung off the top
            // edge is clipped by the scroll container. Overlaying the plot the
            // way every charting tooltip does keeps it whole, and the column
            // highlight is what identifies the bar being read.
            className={cn(
              'pointer-events-none absolute top-0 z-10 w-max max-w-56 rounded-md border border-border bg-popover px-2.5 py-1.5 text-xs text-popover-foreground shadow-md',
              cardAtStart ? 'left-0' : cardAtEnd ? 'right-0' : '-translate-x-1/2',
            )}
            style={cardAtStart || cardAtEnd ? undefined : { left: `${cardCenter}%` }}
          >
            <div className="font-medium">{dayLabel(active.day)}</div>
            {active.articles === 0 ? (
              <div className="text-muted-foreground">No articles to search</div>
            ) : (
              <div className="tabular-nums text-muted-foreground">
                {active.matches} of {active.articles} articles
                {' · '}
                {((active.matches / active.articles) * 100).toFixed(1)}%
              </div>
            )}
            {active.partial && <div className="text-muted-foreground">Partial day</div>}
          </div>
        )}
      </div>

      {/* Labels are absolutely positioned rather than one per column: a
          31-day window gives each column ~24px, and "Aug 19" in a cell that
          narrow truncates to "Au…". Overflowing the column is fine as long as
          the two ends align inwards instead of off the edge. */}
      <div className="relative h-3 text-[10px] text-muted-foreground">
        {days.map((day, index) => {
          if (index % labelEvery !== 0) return null
          const center = ((index + 0.5) / days.length) * 100
          return (
            <span
              key={day.day}
              className={cn(
                'absolute top-0 whitespace-nowrap',
                center < 4 ? 'translate-x-0' : center > 96 ? '-translate-x-full' : '-translate-x-1/2',
              )}
              style={{ left: `${center}%` }}
            >
              {dayLabel(day.day)}
            </span>
          )
        })}
      </div>
    </div>
  )
}
