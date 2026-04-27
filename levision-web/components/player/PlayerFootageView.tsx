'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import { useChatDock } from '@/components/chat/ChatDockProvider'
import PlayerRosterPanel from '@/components/player/PlayerRosterPanel'
import PossessionNav from '@/components/player/PossessionNav'
import { useFootageLibrary } from '@/hooks/useFootageLibrary'
import { useLiveGameState } from '@/hooks/useLiveGameState'
import { usePossessionFilter } from '@/hooks/usePossessionFilter'
import type { FootageClip } from '@/lib/footage-library'

const LAKERS_WARRIORS_CHRISTMAS_PATTERN = /lakers[\s_-]*warriors[\s_-]*christmas/i

function isLakersWarriorsChristmasClip(clip: FootageClip | null): boolean {
  if (!clip) return false
  const haystack = `${clip.title ?? ''} ${clip.playbackUrl ?? ''} ${clip.id ?? ''}`
  return LAKERS_WARRIORS_CHRISTMAS_PATTERN.test(haystack)
}

type Props = {
  reviewClip?: FootageClip | null
}

export default function PlayerFootageView({ reviewClip = null }: Props) {
  const { setFloatingHidden } = useChatDock()
  const { clips, loading, error } = useFootageLibrary()

  const mergedClips = useMemo(() => {
    if (!reviewClip) return clips
    const rest = clips.filter((c) => c.id !== reviewClip.id)
    return [reviewClip, ...rest]
  }, [clips, reviewClip])

  const active = reviewClip
    ? mergedClips.find((c) => c.id === reviewClip.id) ?? mergedClips[0] ?? null
    : mergedClips[0] ?? null

  const isChristmasClip = isLakersWarriorsChristmasClip(active)

  // ── Video element ─────────────────────────────────────────────────────────
  const videoRef = useRef<HTMLVideoElement | null>(null)
  const [videoSecond, setVideoSecond] = useState(0)
  const [trackedClipId, setTrackedClipId] = useState<string | null>(active?.id ?? null)

  if ((active?.id ?? null) !== trackedClipId) {
    setTrackedClipId(active?.id ?? null)
    setVideoSecond(0)
  }

  useEffect(() => {
    setFloatingHidden(false)
    return () => setFloatingHidden(false)
  }, [setFloatingHidden])

  // ── Live game state ────────────────────────────────────────────────────────
  const { timeline } = useLiveGameState({
    enabled: isChristmasClip,
    videoSecond,
  })

  // ── Possession filter + segment navigation ────────────────────────────────
  const {
    roster,
    selectedPids,
    togglePid,
    clearSelection,
    activeSegments,
    currentSegment,
    segIdx,
    hasPrev,
    hasNext,
    prev,
    next,
    seekToSecond,
  } = usePossessionFilter(isChristmasClip ? timeline : null)

  // ── Team names for Away / Home section headers ────────────────────────────
  const { awayTeamName, homeTeamName } = useMemo(() => {
    if (!timeline) return { awayTeamName: undefined, homeTeamName: undefined }
    const firstKey = Object.keys(timeline.snapshots).sort((a, b) => Number(a) - Number(b))[0]
    const firstSnap = firstKey ? timeline.snapshots[firstKey] : undefined
    return {
      awayTeamName: firstSnap?.awayTeam.teamName,
      homeTeamName: firstSnap?.homeTeam.teamName,
    }
  }, [timeline])

  // ── Selected player names for PossessionNav context string ────────────────
  const selectedNames = useMemo(() => {
    return roster
      .filter((e) => selectedPids.has(e.pid))
      .map((e) => e.name)
  }, [roster, selectedPids])

  // ── Auto-seek video when currentSegment changes via Prev / Next ───────────
  const prevSegIdxRef = useRef(segIdx)
  useEffect(() => {
    if (!videoRef.current || !currentSegment) return
    if (prevSegIdxRef.current !== segIdx) {
      videoRef.current.currentTime = currentSegment.start
    }
    prevSegIdxRef.current = segIdx
  }, [segIdx, currentSegment])

  // ── Keep nav pointer in sync when user manually scrubs ────────────────────
  // Runs only on onSeeked (manual scrub), not on every onTimeUpdate tick.
  const handleSeeked = (sec: number) => {
    setVideoSecond(sec)
    if (activeSegments.length > 0) {
      seekToSecond(Math.floor(sec))
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
          View Footage
        </h2>
        <p className="text-[0.84rem] text-muted font-light">
          Filter by player possession — select one or more players to jump between their segments.
        </p>
      </div>

      <div className="grid min-h-[min(60vh,520px)] gap-5 xl:grid-cols-[320px_minmax(0,1fr)_320px]">

        {/* Left Panel — Player Roster */}
        <div className="min-h-[min(60vh,520px)]">
          <PlayerRosterPanel
            roster={roster}
            selectedPids={selectedPids}
            onToggle={togglePid}
            onClear={clearSelection}
            awayTeamName={awayTeamName}
            homeTeamName={homeTeamName}
          />
        </div>

        {/* Center — Video + PossessionNav */}
        <div className="flex min-h-[min(60vh,520px)] flex-col gap-3">
          <div className="flex-1 border border-[rgba(200,136,58,0.15)] rounded-sm bg-black overflow-hidden flex flex-col">
            <div className="aspect-video w-full max-h-[min(56vh,640px)] bg-black flex items-center justify-center relative">
              {loading && (
                <p className="text-[0.8rem] text-muted/60 font-light">Loading…</p>
              )}
              {!loading && error && (
                <p className="text-[0.8rem] text-red-300/80 font-light px-6 text-center">{error}</p>
              )}
              {!loading && !error && active?.playbackUrl ? (
                <video
                  ref={videoRef}
                  key={active.playbackUrl}
                  controls
                  playsInline
                  className="w-full h-full object-contain"
                  src={active.playbackUrl}
                  onTimeUpdate={(e) => setVideoSecond(e.currentTarget.currentTime)}
                  onSeeked={(e) => handleSeeked(e.currentTarget.currentTime)}
                >
                  Your browser does not support video playback.
                </video>
              ) : null}
              {!loading && !error && !active?.playbackUrl && (
                <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 px-8 text-center">
                  <div className="w-12 h-12 rounded-full border border-white/10 flex items-center justify-center">
                    <span className="text-muted/40 text-lg font-light">▶</span>
                  </div>
                  <p className="text-[0.74rem] text-muted/50 font-light max-w-[320px]">
                    No footage yet. LeBron didn't become LeBron by skipping film.
                  </p>
                </div>
              )}
            </div>

            {active?.playbackUrl && (
              <div className="px-4 py-3 border-t border-[rgba(200,136,58,0.1)]">
                <h3 className="font-display text-offwhite text-lg tracking-wide">{active.title}</h3>
              </div>
            )}
          </div>

          {/* Possession navigation bar — always shown for Christmas clip */}
          {isChristmasClip && (
            <PossessionNav
              currentSegment={currentSegment}
              segIdx={segIdx}
              totalSegments={activeSegments.length}
              hasPrev={hasPrev}
              hasNext={hasNext}
              onPrev={prev}
              onNext={next}
              selectedNames={selectedNames}
            />
          )}
        </div>

        {/* Right Panel — reserved for future possession summary */}
        <div className="hidden xl:block" />
      </div>
    </div>
  )
}
