const UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
  ['year', 1000 * 60 * 60 * 24 * 365],
  ['month', 1000 * 60 * 60 * 24 * 30],
  ['week', 1000 * 60 * 60 * 24 * 7],
  ['day', 1000 * 60 * 60 * 24],
  ['hour', 1000 * 60 * 60],
  ['minute', 1000 * 60],
]

const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' })

export function relativeTime(iso: string, now: Date = new Date()): string {
  const then = new Date(iso).getTime()
  const diffMs = then - now.getTime()
  const absMs = Math.abs(diffMs)

  if (absMs < 60_000) return 'just now'

  for (const [unit, ms] of UNITS) {
    if (absMs >= ms || unit === 'minute') {
      const value = Math.round(diffMs / ms)
      return rtf.format(value, unit)
    }
  }
  return rtf.format(Math.round(diffMs / 60_000), 'minute')
}

export function formatAbsolute(iso: string): string {
  return new Date(iso).toLocaleString('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
  })
}

export const DEFAULT_START_TIME = '2026-01-01T00:00:00.000Z'

export const TIME_RANGE_OPTIONS = [
  { value: '24h', label: 'Last 24 hours', ms: 1000 * 60 * 60 * 24 },
  { value: '7d', label: 'Last 7 days', ms: 1000 * 60 * 60 * 24 * 7 },
  { value: '30d', label: 'Last 30 days', ms: 1000 * 60 * 60 * 24 * 30 },
  { value: 'all', label: 'Since Jan 1, 2026', ms: null },
] as const

export type TimeRangeValue = (typeof TIME_RANGE_OPTIONS)[number]['value']

export function sinceFromRange(range: TimeRangeValue, now: Date = new Date()): string {
  const opt = TIME_RANGE_OPTIONS.find((o) => o.value === range) ?? TIME_RANGE_OPTIONS[2]
  if (opt.ms === null) return DEFAULT_START_TIME
  return new Date(Math.max(now.getTime() - opt.ms, Date.parse(DEFAULT_START_TIME))).toISOString()
}
