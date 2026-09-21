'use client'

import { useEffect, useState } from 'react'

/**
 * Trails `value` by `delayMs`, resetting the timer on every change.
 *
 * Nothing else in the app debounces — the Articles search fires a request per
 * keystroke and leans on SWR dedupe. That is affordable for a paged feed and
 * not for an aggregate that scans a whole window, hence this hook.
 */
export function useDebouncedValue<T>(value: T, delayMs = 350): T {
  const [debounced, setDebounced] = useState(value)

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs)
    return () => clearTimeout(timer)
  }, [value, delayMs])

  return debounced
}
