'use client'

import { Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { TopicFilter } from '@/components/topics/topic-filter'
import { TIME_RANGE_OPTIONS, type TimeRangeValue } from '@/lib/time'
import { cn } from '@/lib/utils'
import type { TrendMode } from './trend-bar-chart'

// `24h` is dropped: at day granularity it is one or two bars, which is a
// number, not a trend.
const TREND_RANGE_OPTIONS = TIME_RANGE_OPTIONS.filter((opt) => opt.value !== '24h')

const MODES: { value: TrendMode; label: string; title: string }[] = [
  { value: 'count', label: 'Count', title: 'Articles matching the query each day' },
  {
    value: 'share',
    label: '%',
    title: "Matches as a share of that day's articles",
  },
]

export function TrendsToolbar({
  q,
  onQChange,
  range,
  onRangeChange,
  mode,
  onModeChange,
  selectedTopics,
  onSelectedTopicsChange,
  resultLabel,
}: {
  q: string
  onQChange: (q: string) => void
  range: TimeRangeValue
  onRangeChange: (range: TimeRangeValue) => void
  mode: TrendMode
  onModeChange: (mode: TrendMode) => void
  selectedTopics: string[]
  onSelectedTopicsChange: (topics: string[]) => void
  resultLabel?: string
}) {
  return (
    <div className="flex flex-col gap-3 border-b border-border bg-background/95 px-4 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => onQChange(e.target.value)}
            placeholder="Search mentions over time"
            className="pl-8"
          />
        </div>

        <Select value={range} onValueChange={(v) => onRangeChange(v as TimeRangeValue)}>
          <SelectTrigger className="w-[150px] shrink-0">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {TREND_RANGE_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>

        {/* Two buttons rather than a ToggleGroup: none is installed, and a
            segmented pair of `outline`/`ghost` buttons needs no new dependency. */}
        <div
          role="group"
          aria-label="Count mode"
          className="flex shrink-0 items-center rounded-md border border-input p-0.5"
        >
          {MODES.map((option) => (
            <Button
              key={option.value}
              variant="ghost"
              size="sm"
              title={option.title}
              aria-pressed={mode === option.value}
              onClick={() => onModeChange(option.value)}
              className={cn(
                'h-7 px-2.5 text-xs',
                // Same active treatment as the nav rail: `bg-secondary` is
                // almost indistinguishable from the panel in dark mode.
                mode === option.value &&
                  'bg-primary/15 text-primary hover:bg-primary/15 hover:text-primary',
              )}
            >
              {option.label}
            </Button>
          ))}
        </div>

        <TopicFilter
          selectedTopics={selectedTopics}
          onSelectedTopicsChange={onSelectedTopicsChange}
        />
      </div>
      {resultLabel && <p className="text-xs text-muted-foreground">{resultLabel}</p>}
    </div>
  )
}
