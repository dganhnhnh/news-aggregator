'use client'

import { useState } from 'react'
import { Search, ChevronDown, Check, X } from 'lucide-react'
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

type FiltersBarProps = {
  q: string
  onQChange: (q: string) => void
  range: TimeRangeValue
  onRangeChange: (range: TimeRangeValue) => void
  selectedSources: string[]
  onSelectedSourcesChange: (sources: string[]) => void
  lockedSourceName?: string
  resultLabel?: string
}

export function FiltersBar({
  q,
  onQChange,
  range,
  onRangeChange,
  selectedSources,
  onSelectedSourcesChange,
  lockedSourceName,
  resultLabel,
}: FiltersBarProps) {
  const since = sinceFromRange(range)
  const { sources, error: sourcesError } = useSources({ since })
  const [sourcePopoverOpen, setSourcePopoverOpen] = useState(false)

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
        <div className="relative min-w-[200px] flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => onQChange(e.target.value)}
            placeholder="Search articles"
            className="pl-8"
          />
        </div>

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

        {lockedSourceName ? (
          <Badge variant="secondary" className="h-9 shrink-0 rounded-md px-3 text-xs">
            {lockedSourceName}
          </Badge>
        ) : (
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
        )}
      </div>
      {resultLabel && (
        <p className="text-xs text-muted-foreground">{resultLabel}</p>
      )}
    </div>
  )
}
