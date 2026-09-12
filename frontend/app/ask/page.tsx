import { AskView } from '@/components/ask/ask-view'
import { NavRail } from '@/components/nav-rail'

export default function AskPage() {
  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <AskView />
    </div>
  )
}
