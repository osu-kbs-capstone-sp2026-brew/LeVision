'use client'

import { useState } from 'react'
import type { Profile } from '@/lib/types'
import type { FootageClip } from '@/lib/footage-library'
import FootageViewTab from '@/components/FootageViewTab'
import UploadTab from '@/components/tabs/UploadTab'
import PastGamesTab from '@/components/tabs/PastGamesTab'

type Tab = 'upload' | 'past' | 'view'

const TABS: { id: Tab; label: string }[] = [
  { id: 'upload', label: 'Upload Footage' },
  { id: 'past',   label: 'Past Games' },
  { id: 'view',   label: 'View Footage' },
]

export default function DashboardTabs({ profile: _ }: { profile: Profile }) {
  const [activeTab, setActiveTab] = useState<Tab>('upload')
  const [reviewClip, setReviewClip] = useState<FootageClip | null>(null)

  const handleReviewClip = (clip: FootageClip) => {
    setReviewClip(clip)
    setActiveTab('view')
  }

  return (
    <main className="flex-1 flex flex-col px-12 pt-10 pb-16 w-full">
      <div className="flex gap-1 border-b border-[rgba(200,136,58,0.15)] mb-10">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            className={`px-5 pb-3.5 pt-1 font-body text-[0.78rem] tracking-[0.12em] uppercase transition-colors duration-200 border-b-[1.5px] -mb-px cursor-pointer ${
              activeTab === t.id
                ? 'text-brand border-brand'
                : 'text-muted border-transparent hover:text-offwhite/70'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="animate-fade-up" key={activeTab}>
        {activeTab === 'upload' && <UploadTab />}
        {activeTab === 'past'   && <PastGamesTab onReviewClip={handleReviewClip} />}
        {activeTab === 'view'   && <FootageViewTab reviewClip={reviewClip} />}
      </div>
    </main>
  )
}
