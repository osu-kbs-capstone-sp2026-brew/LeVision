'use client'

import { useEffect, useMemo, useState } from 'react'
import type { LiveGameState, LiveGameTimeline } from '@/lib/types'

type UseLiveGameStateOptions = {
  clipId?: string | null
  enabled?: boolean
  videoSecond?: number
}

export function useLiveGameState({
  clipId,
  enabled = true,
  videoSecond = 0,
}: UseLiveGameStateOptions = {}) {
  const [timeline, setTimeline] = useState<LiveGameTimeline | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Reset when clip changes
  const [trackedClipId, setTrackedClipId] = useState<string | null | undefined>(clipId)
  if (clipId !== trackedClipId) {
    setTrackedClipId(clipId)
    setTimeline(null)
    setError(null)
  }

  useEffect(() => {
    if (!enabled || !clipId || timeline || error) return
    let cancelled = false

    fetch(`/api/live-game-state?clipId=${encodeURIComponent(clipId)}`)
      .then(async (res) => {
        if (!res.ok) throw new Error(`Unable to load live state: ${res.status}`)
        return (await res.json()) as LiveGameTimeline
      })
      .then((payload) => { if (!cancelled) setTimeline(payload) })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Could not load live state')
      })

    return () => { cancelled = true }
  }, [enabled, clipId, timeline, error])

  const loading = enabled && Boolean(clipId) && !timeline && !error

  const liveState = useMemo<LiveGameState | null>(() => {
    if (!timeline) return null
    const { minSecond, maxSecond, snapshots } = timeline
    const requested = Math.floor(videoSecond) + 1
    const clamped = Math.min(Math.max(requested, minSecond), maxSecond)
    return snapshots[String(clamped)] ?? null
  }, [timeline, videoSecond])

  return { liveState, timeline, loading, error }
}
