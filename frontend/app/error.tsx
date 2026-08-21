'use client'

import { AlertCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'

export default function ErrorPage({ reset }: { error: Error; reset: () => void }) {
  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-6">
      <div className="flex max-w-sm flex-col items-center gap-3 text-center">
        <span className="flex size-10 items-center justify-center rounded-full bg-destructive/10 text-destructive">
          <AlertCircle className="size-5" />
        </span>
        <h1 className="text-lg font-semibold">Could not load News Radar</h1>
        <p className="text-sm text-muted-foreground">
          The news service is unavailable. Check that the backend is running and try again.
        </p>
        <Button variant="outline" onClick={reset}>
          Try again
        </Button>
      </div>
    </main>
  )
}
