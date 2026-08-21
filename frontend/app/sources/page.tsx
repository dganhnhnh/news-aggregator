import Link from 'next/link'
import { Radio, ChevronRight } from 'lucide-react'
import { SidebarProvider, SidebarTrigger } from '@/components/ui/sidebar'
import { AppSidebar } from '@/components/app-sidebar'
import { NavRail } from '@/components/nav-rail'
import { getSources } from '@/lib/server-api'

export const dynamic = 'force-dynamic'

export default async function SourcesPage() {
  const sources = await getSources()

  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <SidebarProvider className="min-h-0 flex-1">
        <AppSidebar />
        <div className="flex min-w-0 flex-1 flex-col">
          <div className="flex items-center gap-2 border-b border-border px-4 py-3">
            <SidebarTrigger />
            <h1 className="text-sm font-semibold">Sources</h1>
          </div>
          <div className="mx-auto w-full max-w-2xl flex-1 overflow-y-auto px-4 py-6">
            <p className="mb-4 text-sm text-muted-foreground">
              {sources.length} sources have articles since Jan 1, 2026. Select one to view its
              dedicated feed.
            </p>
            <ul className="flex flex-col overflow-hidden rounded-lg border border-border">
              {sources.map((source) => (
                <li key={source.slug} className="border-b border-border last:border-b-0">
                  <Link
                    href={`/sources/${source.slug}`}
                    className="flex items-center gap-3 px-4 py-3 transition-colors hover:bg-accent/60"
                  >
                    <span className="flex size-8 shrink-0 items-center justify-center rounded-full bg-secondary text-secondary-foreground">
                      <Radio className="size-4" />
                    </span>
                    <div className="flex min-w-0 flex-1 flex-col">
                      <span className="truncate text-sm font-medium">{source.name}</span>
                      <span className="truncate text-xs text-muted-foreground">
                        {source.homepage ?? 'No homepage listed'}
                      </span>
                    </div>
                    <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </SidebarProvider>
    </div>
  )
}
