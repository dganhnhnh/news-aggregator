import { FeedManagementView } from '@/components/feeds/feed-management-view'
import { NavRail } from '@/components/nav-rail'

export default function FeedsPage() {
  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <FeedManagementView />
    </div>
  )
}
