'use client'

import { Check, ChevronDown, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { useTopics } from '@/hooks/use-topics'
import { cn } from '@/lib/utils'

export function TopicFilter({
  selectedTopics,
  onSelectedTopicsChange,
}: {
  selectedTopics: string[]
  onSelectedTopicsChange: (topics: string[]) => void
}) {
  const { topics, error } = useTopics()

  function toggleTopic(slug: string) {
    if (selectedTopics.includes(slug)) {
      onSelectedTopicsChange(selectedTopics.filter((topic) => topic !== slug))
    } else {
      onSelectedTopicsChange([...selectedTopics, slug])
    }
  }

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button variant="outline" className="shrink-0">
            Topics
            {selectedTopics.length > 0 && (
              <Badge variant="secondary" className="ml-1 rounded-full px-1.5">
                {selectedTopics.length}
              </Badge>
            )}
            <ChevronDown data-icon="inline-end" />
          </Button>
        }
      />
      <PopoverContent align="end" className="w-[280px] p-0">
        <Command>
          <CommandInput placeholder="Filter topics..." />
          <CommandList>
            <CommandEmpty>{error ? 'Could not load topics.' : 'No topics found.'}</CommandEmpty>
            <CommandGroup>
              {topics.map((topic) => {
                const active = selectedTopics.includes(topic.slug)
                return (
                  <CommandItem
                    key={topic.slug}
                    value={`${topic.name} ${topic.slug}`}
                    onSelect={() => toggleTopic(topic.slug)}
                  >
                    <span
                      className={cn(
                        'flex size-4 items-center justify-center rounded-sm border border-input',
                        active && 'border-primary bg-primary text-primary-foreground',
                      )}
                    >
                      {active && <Check className="size-3" />}
                    </span>
                    <span className="truncate">{topic.name}</span>
                  </CommandItem>
                )
              })}
            </CommandGroup>
          </CommandList>
        </Command>
        {selectedTopics.length > 0 && (
          <div className="border-t border-border p-1.5">
            <Button
              variant="ghost"
              size="sm"
              className="w-full justify-center text-xs"
              onClick={() => onSelectedTopicsChange([])}
            >
              <X data-icon="inline-start" />
              Clear topics
            </Button>
          </div>
        )}
      </PopoverContent>
    </Popover>
  )
}
