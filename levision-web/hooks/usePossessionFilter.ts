'use client'

import { useEffect, useMemo, useState } from 'react'
import type {
  LiveGameTimeline,
  PlayerSegmentIndex,
  PossessionSegment,
} from '@/lib/types'

// ── Roster entry returned to UI ───────────────────────────────────────────────

export type RosterEntry = {
  pid: string
  name: string
  teamName: string
  /** Number of distinct possession segments (clips). */
  segmentCount: number
  /** Total video seconds where this player had possession. */
  possessionSeconds: number
}

// ── Segment index builder ─────────────────────────────────────────────────────

function buildSegmentIndex(timeline: LiveGameTimeline): PlayerSegmentIndex {
  const { snapshots } = timeline
  const sortedKeys = Object.keys(snapshots)
    .map(Number)
    .filter((n) => Number.isFinite(n))
    .sort((a, b) => a - b)

  const index: PlayerSegmentIndex = {}

  let runPid: string | null = null
  let runStart: number = 0

  const closeRun = (end: number) => {
    if (runPid !== null) {
      if (!index[runPid]) index[runPid] = []
      index[runPid].push({ start: runStart, end, pid: runPid })
      runPid = null
    }
  }

  for (let i = 0; i < sortedKeys.length; i++) {
    const sec = sortedKeys[i]
    const snap = snapshots[String(sec)]
    const pid = snap.playerPossession ?? null

    const prevSec = i > 0 ? sortedKeys[i - 1] : sec
    // A gap in video_sec (non-consecutive seconds) breaks a run, even when
    // the same player appears on both sides.
    const hasGap = i > 0 && sec - prevSec > 1

    if (pid === null) {
      closeRun(prevSec)
    } else if (pid !== runPid || hasGap) {
      closeRun(prevSec)
      runPid = pid
      runStart = sec
    }
    // Same pid, no gap: run continues
  }

  // Close any trailing run
  if (runPid !== null && sortedKeys.length > 0) {
    closeRun(sortedKeys[sortedKeys.length - 1])
  }

  return index
}

// ── Roster builder ────────────────────────────────────────────────────────────

function buildRoster(
  timeline: LiveGameTimeline,
  index: PlayerSegmentIndex,
): RosterEntry[] {
  const { snapshots } = timeline

  // Collect all player names from all snapshots (later snapshots accumulate more stats)
  const nameMap = new Map<string, { name: string; teamName: string }>()
  for (const snap of Object.values(snapshots)) {
    for (const player of [
      ...snap.homeTeam.playerStats,
      ...snap.awayTeam.playerStats,
    ]) {
      if (!nameMap.has(player.id)) {
        const teamName =
          snap.homeTeam.playerStats.some((p) => p.id === player.id)
            ? snap.homeTeam.teamName
            : snap.awayTeam.teamName
        nameMap.set(player.id, { name: player.name, teamName })
      }
    }
  }

  const entries: RosterEntry[] = []
  for (const [pid, info] of nameMap.entries()) {
    const segs = index[pid] ?? []
    const segmentCount = segs.length
    const possessionSeconds = segs.reduce((sum, s) => sum + (s.end - s.start + 1), 0)
    entries.push({ pid, ...info, segmentCount, possessionSeconds })
  }

  // Sort: most possession clips first, then alpha
  entries.sort((a, b) => {
    if (b.segmentCount !== a.segmentCount) return b.segmentCount - a.segmentCount
    return a.name.localeCompare(b.name)
  })

  return entries
}

// ── Overlap merge (multi-player union) ───────────────────────────────────────

/**
 * Given a sorted array of segments, merge any that overlap or share a boundary.
 * Two players cannot physically both hold the ball simultaneously, so overlaps
 * in real possession data are rare. This function is correct regardless.
 */
function mergeOverlapping(sorted: PossessionSegment[]): PossessionSegment[] {
  if (sorted.length === 0) return []
  const merged: PossessionSegment[] = [{ ...sorted[0] }]
  for (let i = 1; i < sorted.length; i++) {
    const last = merged[merged.length - 1]
    const cur = sorted[i]
    if (cur.start <= last.end) {
      // Overlapping or adjacent — extend the current run
      last.end = Math.max(last.end, cur.end)
    } else {
      merged.push({ ...cur })
    }
  }
  return merged
}

// ── Main hook ─────────────────────────────────────────────────────────────────

export function usePossessionFilter(timeline: LiveGameTimeline | null) {
  // 1. Build segment index — O(n) once per timeline load
  const segmentIndex = useMemo<PlayerSegmentIndex>(
    () => (timeline ? buildSegmentIndex(timeline) : {}),
    [timeline],
  )

  // 2. Roster with possession counts
  const roster = useMemo<RosterEntry[]>(
    () => (timeline ? buildRoster(timeline, segmentIndex) : []),
    [timeline, segmentIndex],
  )

  // 3. Selection state
  const [selectedPids, setSelectedPids] = useState<Set<string>>(new Set())

  function togglePid(pid: string) {
    setSelectedPids((prev) => {
      const next = new Set(prev)
      if (next.has(pid)) next.delete(pid)
      else next.add(pid)
      return next
    })
  }

  function selectOnly(pid: string) {
    setSelectedPids(new Set([pid]))
  }

  function clearSelection() {
    setSelectedPids(new Set())
  }

  // 4. Active segments — union of selected players' segments, sorted + overlap-merged
  const activeSegments = useMemo<PossessionSegment[]>(() => {
    const all: PossessionSegment[] = []
    for (const pid of selectedPids) {
      all.push(...(segmentIndex[pid] ?? []))
    }
    const sorted = all.slice().sort((a, b) => a.start - b.start)
    return mergeOverlapping(sorted)
  }, [segmentIndex, selectedPids])

  // 5. Navigation pointer
  const [segIdx, setSegIdx] = useState(0)

  // Reset pointer whenever the active segment list changes
  useEffect(() => {
    setSegIdx(0)
  }, [activeSegments])

  const currentSegment: PossessionSegment | null = activeSegments[segIdx] ?? null

  function prev() {
    setSegIdx((i) => Math.max(0, i - 1))
  }

  function next() {
    setSegIdx((i) => Math.min(activeSegments.length - 1, i + 1))
  }

  /**
   * Jump to the segment that contains `videoSec` (or the nearest one after).
   * Used when the user manually scrubs the video.
   */
  function seekToSecond(videoSec: number) {
    if (activeSegments.length === 0) return
    // Find first segment whose end >= videoSec
    const idx = activeSegments.findIndex((s) => s.end >= videoSec)
    setSegIdx(idx === -1 ? activeSegments.length - 1 : idx)
  }

  return {
    segmentIndex,
    roster,
    selectedPids,
    togglePid,
    selectOnly,
    clearSelection,
    activeSegments,
    currentSegment,
    segIdx,
    prev,
    next,
    hasPrev: segIdx > 0,
    hasNext: segIdx < activeSegments.length - 1,
    seekToSecond,
  }
}
