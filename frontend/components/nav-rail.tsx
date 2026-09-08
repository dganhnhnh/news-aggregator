'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Rss, Newspaper, BookOpen, TrendingUp, ListPlus } from 'lucide-react'
import { toast } from 'sonner'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { ThemeToggle } from '@/components/theme-toggle'
import { cn } from '@/lib/utils'

const NAV_ITEMS = [
  {
    key: 'articles',
    label: 'Articles',
    icon: Newspaper,
    href: '/',
    available: true,
  },
  {
    key: 'stories',
    label: 'Stories',
    icon: BookOpen,
    href: '/stories',
    available: true,
  },
  {
    key: 'trends',
    label: 'Trends',
    icon: TrendingUp,
    href: null,
    available: false,
  },
  {
    key: 'feeds',
    label: 'Feeds',
    icon: ListPlus,
    href: '/feeds',
    available: true,
  },
] as const

export function NavRail() {
  const pathname = usePathname()
  const isArticlesActive =
    pathname === '/' || pathname.startsWith('/sources') || pathname.startsWith('/articles')
  const isStoriesActive = pathname.startsWith('/stories')
  const isFeedsActive = pathname.startsWith('/feeds')

  return (
    <nav
      aria-label="Primary"
      className="flex h-svh w-14 shrink-0 flex-col items-center justify-between border-r border-sidebar-border bg-sidebar py-3"
    >
      <div className="flex flex-col items-center gap-3">
        <Link
          href="/"
          aria-label="News home"
          className="flex size-8 shrink-0 items-center justify-center rounded-md bg-primary text-primary-foreground"
        >
          <Rss className="size-4" />
        </Link>

        <div className="flex flex-col items-center gap-1">
          {NAV_ITEMS.map((item) => {
            const isActive =
              (item.key === 'articles' && isArticlesActive) ||
              (item.key === 'stories' && isStoriesActive) ||
              (item.key === 'feeds' && isFeedsActive)
            const Icon = item.icon

            const button = (
              <button
                type="button"
                aria-label={item.label}
                aria-current={isActive ? 'page' : undefined}
                onClick={
                  item.available
                    ? undefined
                    : () => toast(`${item.label} — coming soon`)
                }
                className={cn(
                  'flex size-10 items-center justify-center rounded-lg text-sidebar-foreground/60 transition-colors',
                  item.available
                    ? 'hover:bg-sidebar-accent hover:text-sidebar-accent-foreground'
                    : 'hover:bg-sidebar-accent/60 hover:text-sidebar-foreground/80',
                  isActive && 'bg-primary/15 text-primary hover:bg-primary/15 hover:text-primary',
                )}
              >
                <Icon className="size-[18px]" />
              </button>
            )

            return (
              <Tooltip key={item.key}>
                <TooltipTrigger
                  render={
                    item.available && item.href ? (
                      <Link href={item.href} aria-label={item.label} aria-current={isActive ? 'page' : undefined}>
                        <span
                          className={cn(
                            'flex size-10 items-center justify-center rounded-lg text-sidebar-foreground/60 transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground',
                            isActive &&
                              'bg-primary/15 text-primary hover:bg-primary/15 hover:text-primary',
                          )}
                        >
                          <Icon className="size-[18px]" />
                        </span>
                      </Link>
                    ) : (
                      button
                    )
                  }
                />
                <TooltipContent side="right">
                  {item.available ? item.label : `${item.label} — coming soon`}
                </TooltipContent>
              </Tooltip>
            )
          })}
        </div>
      </div>

      <ThemeToggle />
    </nav>
  )
}
