'use client'

import { useState } from 'react'
import { ChevronDown, Check, X, Plus, Minus } from 'lucide-react'
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
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { Badge } from '@/components/ui/badge'
import { useSources } from '@/hooks/use-sources'
import { sinceFromRange, TIME_RANGE_OPTIONS, type TimeRangeValue } from '@/lib/time'
import { cn } from '@/lib/utils'

type StoryFiltersBarProps = {
  range: TimeRangeValue
  onRangeChange: (range: TimeRangeValue) => void
  selectedSources: string[]
  onSelectedSourcesChange: (sources: string[]) => void
  minSources: number
  onMinSourcesChange: (count: number) => void
  minArticles: number
  onMinArticlesChange: (count: number) => void
  resultLabel?: string
}

export function StoryFiltersBar({
  range,
  onRangeChange,
  selectedSources,
  onSelectedSourcesChange,
  minSources,
  onMinSourcesChange,
  minArticles,
  onMinArticlesChange,
  resultLabel,
}: StoryFiltersBarProps) {
  const since = sinceFromRange(range)
  const { sources, error: sourcesError } = useSources({ since })
  const [sourcePopoverOpen, setSourcePopoverOpen] = useState(false)

  function handleMinSourcesChange(e: React.ChangeEvent<HTMLInputElement>) {
    const value = parseInt(e.target.value, 10)
    if (!isNaN(value) && value >= 1) {
      onMinSourcesChange(value)
    }
  }

  function handleMinArticlesChange(e: React.ChangeEvent<HTMLInputElement>) {
    const value = parseInt(e.target.value, 10)
    if (!isNaN(value) && value >= 1) {
      onMinArticlesChange(value)
    }
  }

  function toggleSource(slug: string) {
    if (selectedSources.includes(slug)) {
      onSelectedSourcesChange(selectedSources.filter((s) => s !== slug))
    } else {
      onSelectedSourcesChange([...selectedSources, slug])
    }
  }

  return (
    <div className="flex flex-col gap-3 border-b border-border bg-background/95 px-4 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
      <div className="flex flex-wrap items-center gap-2">
        <Select value={range} onValueChange={(v) => onRangeChange(v as TimeRangeValue)}>
          <SelectTrigger className="w-[150px] shrink-0">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {TIME_RANGE_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>

        <div className="flex items-center gap-1 shrink-0 rounded-md border border-input bg-background px-2 py-1.5">
          <span className="text-xs text-muted-foreground">Min sources:</span>
          <button
            type="button"
            onClick={() => onMinSourcesChange(Math.max(1, minSources - 1))}
            className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:text-foreground"
          >
            <Minus className="size-3" />
          </button>
          <input
            type="number"
            min="1"
            max="100"
            value={minSources}
            onChange={handleMinSourcesChange}
            className="w-12 bg-transparent text-center text-sm font-medium focus:outline-none"
          />
          <button
            type="button"
            onClick={() => onMinSourcesChange(minSources + 1)}
            className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:text-foreground"
          >
            <Plus className="size-3" />
          </button>
        </div>

        <div className="flex items-center gap-1 shrink-0 rounded-md border border-input bg-background px-2 py-1.5">
          <span className="text-xs text-muted-foreground">Min articles:</span>
          <button
            type="button"
            onClick={() => onMinArticlesChange(Math.max(1, minArticles - 1))}
            className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:text-foreground"
          >
            <Minus className="size-3" />
          </button>
          <input
            type="number"
            min="1"
            max="100"
            value={minArticles}
            onChange={handleMinArticlesChange}
            className="w-12 bg-transparent text-center text-sm font-medium focus:outline-none"
          />
          <button
            type="button"
            onClick={() => onMinArticlesChange(minArticles + 1)}
            className="inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:text-foreground"
          >
            <Plus className="size-3" />
          </button>
        </div>

        <Popover open={sourcePopoverOpen} onOpenChange={setSourcePopoverOpen}>
          <PopoverTrigger
            render={
              <Button variant="outline" className="shrink-0">
                Sources
                {selectedSources.length > 0 && (
                  <Badge variant="secondary" className="ml-1 rounded-full px-1.5">
                    {selectedSources.length}
                  </Badge>
                )}
                <ChevronDown data-icon="inline-end" />
              </Button>
            }
          />
          <PopoverContent align="end" className="w-[260px] p-0">
            <Command>
              <CommandInput placeholder="Filter sources..." />
              <CommandList>
                <CommandEmpty>
                  {sourcesError ? 'Could not load sources.' : 'No sources found.'}
                </CommandEmpty>
                <CommandGroup>
                  {sources.map((source) => {
                    const active = selectedSources.includes(source.slug)
                    return (
                      <CommandItem
                        key={source.slug}
                        value={source.name}
                        onClick={() => toggleSource(source.slug)}
                      >
                        <span
                          className={cn(
                            'flex size-4 items-center justify-center rounded-sm border border-input',
                            active && 'border-primary bg-primary text-primary-foreground',
                          )}
                        >
                          {active && <Check className="size-3" />}
                        </span>
                        {source.name}
                      </CommandItem>
                    )
                  })}
                </CommandGroup>
              </CommandList>
            </Command>
            {selectedSources.length > 0 && (
              <div className="border-t border-border p-1.5">
                <Button
                  variant="ghost"
                  size="sm"
                  className="w-full justify-center text-xs"
                  onClick={() => onSelectedSourcesChange([])}
                >
                  <X data-icon="inline-start" />
                  Clear sources
                </Button>
              </div>
            )}
          </PopoverContent>
        </Popover>
      </div>
      {resultLabel && <p className="text-xs text-muted-foreground">{resultLabel}</p>}
    </div>
  )
}
