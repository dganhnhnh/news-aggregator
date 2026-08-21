'use client'

import { Fragment, useMemo } from 'react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Newspaper, Radio } from 'lucide-react'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from '@/components/ui/sidebar'
import { useSources } from '@/hooks/use-sources'
import { Skeleton } from '@/components/ui/skeleton'

function sourceIndex(name: string): string {
  const first = name.trim().charAt(0).toUpperCase()
  return /^[A-Z]$/.test(first) ? first : '#'
}

export function AppSidebar() {
  const pathname = usePathname()
  const { sources, isLoading, error } = useSources()
  const sourceGroups = useMemo(() => {
    const groups = new Map<string, typeof sources>()
    const sortedSources = [...sources].sort((a, b) =>
      a.name.localeCompare(b.name, undefined, { sensitivity: 'base', numeric: true }),
    )

    for (const source of sortedSources) {
      const index = sourceIndex(source.name)
      const group = groups.get(index) ?? []
      group.push(source)
      groups.set(index, group)
    }

    return Array.from(groups.entries())
  }, [sources])

  return (
    <Sidebar collapsible="icon" className="data-[side=left]:left-14">
      <SidebarHeader className="border-b border-sidebar-border">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" render={<Link href="/" />}>
              <div className="flex flex-col leading-tight">
                <span className="font-semibold tracking-tight">News</span>
                <span className="text-xs text-muted-foreground">
                  Aggregated feed
                </span>
              </div>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton
                  render={<Link href="/" />}
                  isActive={pathname === '/'}
                  tooltip="All articles"
                >
                  <Newspaper />
                  <span>All articles</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel>Sources</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {isLoading &&
                Array.from({ length: 8 }).map((_, i) => (
                  <SidebarMenuItem key={i}>
                    <div className="flex items-center gap-2 px-2 py-1.5">
                      <Skeleton className="size-4 shrink-0 rounded-full" />
                      <Skeleton className="h-3.5 w-full" />
                    </div>
                  </SidebarMenuItem>
                ))}
              {error && !isLoading && (
                <p className="px-2 py-1.5 text-xs text-destructive">Sources unavailable</p>
              )}
              {sourceGroups.map(([index, group], groupIndex) => (
                <Fragment key={index}>
                  <SidebarMenuItem
                    className={`px-2 pb-1 text-[10px] font-semibold uppercase tracking-widest text-sidebar-foreground/45 group-data-[collapsible=icon]:hidden ${groupIndex === 0 ? 'pt-1' : 'pt-3'}`}
                  >
                    {index}
                  </SidebarMenuItem>
                  {group.map((source) => (
                    <SidebarMenuItem key={source.slug}>
                      <SidebarMenuButton
                        render={<Link href={`/sources/${source.slug}`} />}
                        isActive={pathname === `/sources/${source.slug}`}
                        tooltip={source.name}
                      >
                        <Radio />
                        <span className="truncate">{source.name}</span>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  ))}
                </Fragment>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="border-t border-sidebar-border">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton render={<Link href="/sources" />} isActive={pathname === '/sources'}>
              <span className="text-xs text-muted-foreground">
                Manage sources
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  )
}
