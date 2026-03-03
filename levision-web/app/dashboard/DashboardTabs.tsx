'use client'

import { useState } from 'react'
import type { Profile } from '@/lib/types'

type Tab = 'upload' | 'search' | 'live'

const TABS: { id: Tab; label: string }[] = [
  { id: 'upload', label: 'Upload Footage' },
  { id: 'search', label: 'Search' },
  { id: 'live',   label: 'Live Games' },
]

export default function DashboardTabs({ profile }: { profile: Profile }) {
  const [activeTab, setActiveTab] = useState<Tab>('upload')

  return (
    <main className="flex-1 flex flex-col px-8 pt-10 pb-16 max-w-[1100px] w-full mx-auto">

      {/* Tab bar */}
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

      {/* Tab panels */}
      <div className="animate-fade-up" key={activeTab}>

        {/* ── Upload ── */}
        {activeTab === 'upload' && (
          <div className="flex flex-col">
            <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
              Upload New Footage
            </h2>
            <p className="text-[0.84rem] text-muted font-light mb-8">
              Drop game film, practice sessions, or highlight cuts.
            </p>

            {/* Upload zone */}
            <div className="border border-dashed border-[rgba(200,136,58,0.28)] rounded-sm p-14 text-center bg-[rgba(200,136,58,0.02)] hover:border-brand hover:bg-[rgba(200,136,58,0.05)] transition-colors duration-200 cursor-default mb-6">
              <div className="font-display text-[1.3rem] tracking-[0.08em] text-offwhite mb-2">
                Drop footage here or click to browse
              </div>
              <div className="text-[0.76rem] text-muted font-light mb-4">
                Game film, practice sessions, highlight cuts
              </div>
              <div className="flex gap-2 justify-center">
                {['MP4', 'MOV', 'AVI', 'MKV'].map((fmt) => (
                  <span
                    key={fmt}
                    className="text-[0.62rem] tracking-[0.12em] px-2.5 py-1 border border-[rgba(200,136,58,0.18)] rounded-sm text-muted"
                  >
                    {fmt}
                  </span>
                ))}
              </div>
            </div>

            {/* Recent uploads placeholder */}
            <p className="text-[0.74rem] text-muted/50 font-light">
              {profile.role === 'coach'
                ? 'No plays saved. Even Phil Jackson wrote things down.'
                : profile.role === 'analyst'
                ? "Nothing here. Emptier than Cleveland's trophy case before 2016."
                : "No footage yet. LeBron didn't become LeBron by skipping film."}
            </p>
          </div>
        )}

        {/* ── Search ── */}
        {activeTab === 'search' && (
          <div className="flex flex-col">
            <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
              Search
            </h2>
            <p className="text-[0.84rem] text-muted font-light mb-8">
              Find plays, players, or moments across your film library.
            </p>

            {/* Search input */}
            <div className="relative mb-8">
              <input
                type="text"
                placeholder="Search footage, plays, players..."
                className="w-full bg-white/[0.03] border border-white/10 focus:border-brand focus:bg-brand/5 rounded-sm pl-5 pr-12 py-3.5 text-offwhite font-body font-light text-sm outline-none transition-colors duration-200 placeholder:text-white/20"
              />
              <span className="absolute right-4 top-1/2 -translate-y-1/2 text-muted/40 text-xs tracking-widest">⌘K</span>
            </div>

            <p className="text-[0.74rem] text-muted/50 font-light">
              Upload footage to start building your searchable library.
            </p>
          </div>
        )}

        {/* ── Live Games ── */}
        {activeTab === 'live' && (
          <div className="flex flex-col">
            <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
              Live Games
            </h2>
            <p className="text-[0.84rem] text-muted font-light mb-8">
              Track in-progress games and pull real-time stats.
            </p>

            {/* Empty state */}
            <div className="border border-[rgba(200,136,58,0.12)] rounded-sm p-10 text-center bg-[rgba(200,136,58,0.015)]">
              <div className="flex items-center justify-center gap-2 mb-3">
                <span className="w-2 h-2 rounded-full bg-muted/30" />
                <span className="text-[0.72rem] tracking-[0.14em] uppercase text-muted/50 font-body">
                  No games live right now
                </span>
              </div>
              <p className="text-[0.78rem] text-muted/40 font-light max-w-[320px] mx-auto">
                Live game tracking will appear here when games are in progress.
              </p>
            </div>
          </div>
        )}

      </div>
    </main>
  )
}
