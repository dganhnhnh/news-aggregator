'use client'

import { useEffect, useState } from 'react'

const MINUTE_MS = 60_000

function startOfCurrentMinute(): Date {
  return new Date(Math.floor(Date.now() / MINUTE_MS) * MINUTE_MS)
}

export function useMinuteNow(): Date {
  const [now, setNow] = useState(startOfCurrentMinute)

  useEffect(() => {
    let timeoutId: ReturnType<typeof setTimeout>

    function scheduleNextMinute() {
      const delay = MINUTE_MS - (Date.now() % MINUTE_MS)
      timeoutId = setTimeout(() => {
        setNow(startOfCurrentMinute())
        scheduleNextMinute()
      }, delay)
    }

    scheduleNextMinute()
    return () => clearTimeout(timeoutId)
  }, [])

  return now
}
