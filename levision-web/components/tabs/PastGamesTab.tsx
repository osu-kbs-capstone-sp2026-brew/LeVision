'use client'

import { useEffect, useState, useCallback } from 'react'
import { fetchAllFootage, type FootageClip } from '@/lib/footage-library'
import FootageViewTab from '@/components/FootageViewTab'

type FootageRow = {
  id: string
  filename: string
  r2_url: string | null
  espn_game_id: string | null
  home_team_id: string | null
  away_team_id: string | null
  game_date: string | null
  game_season: string | null
  vision_status: string
  vision_stage: string | null
  vision_results_key: string | null
  created_at: string
}

type VisionUpdateEvent = {
  type: 'vision_update'
  clip_id: string
  event: 'stage_update' | 'completed' | 'failed'
  stage: string | null
  error: string | null
}

// ── Progress ──────────────────────────────────────────────────────────────────

const STAGE_PROGRESS: Record<string, number> = {
  downloading:        8,
  extracting_frames:  28,
  running_ocr:        48,
  fetching_pbp:       63,
  merging:            80,
  uploading_results:  93,
}

const STAGE_LABELS: Record<string, string> = {
  downloading:        'Downloading clip...',
  extracting_frames:  'Extracting frames...',
  running_ocr:        'Reading game clock...',
  fetching_pbp:       'Fetching play-by-play...',
  merging:            'Building game timeline...',
  uploading_results:  'Saving results...',
}

function getProgress(row: FootageRow): number | null {
  if (row.vision_status === 'awaiting_game') return null
  if (row.vision_status === 'failed') return -1
  if (row.vision_status === 'completed') return 100
  if (row.vision_status === 'processing') {
    return row.vision_stage ? (STAGE_PROGRESS[row.vision_stage] ?? 5) : 5
  }
  return null
}

// ── Status badge ──────────────────────────────────────────────────────────────

const STATUS_LABEL: Record<string, string> = {
  awaiting_game: 'Awaiting game link',
  processing:    'Processing',
  completed:     'Ready',
  failed:        'Failed',
}

const STATUS_COLOR: Record<string, string> = {
  awaiting_game: 'text-muted/60 bg-white/[0.04]',
  processing:    'text-brand/70 bg-brand/10',
  completed:     'text-emerald-400 bg-emerald-400/10',
  failed:        'text-accent bg-accent/10',
}

// ── Row → FootageClip ─────────────────────────────────────────────────────────

function rowToClip(row: FootageRow): FootageClip {
  return {
    id:           row.id,
    title:        row.filename,
    createdAt:    row.created_at,
    playbackUrl:  row.r2_url,
    visionStatus:     row.vision_status,
    visionStage:      row.vision_stage,
    visionResultsKey: row.vision_results_key,
    homeTeamId:       row.home_team_id ?? undefined,
    awayTeamId:   row.away_team_id ?? undefined,
    gameDate:     row.game_date,
  }
}

// ── Main component ────────────────────────────────────────────────────────────

export default function PastGamesTab() {
  const [footage, setFootage] = useState<FootageRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedClip, setSelectedClip] = useState<FootageClip | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null)

  // Load footage from Supabase
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchAllFootage()
      .then((rows) => { if (!cancelled) setFootage(rows) })
      .catch((err: unknown) => {
        if (cancelled) return
        setError(err instanceof Error ? err.message : 'Unable to load footage')
      })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [])

  // SSE real-time updates
  useEffect(() => {
    const es = new EventSource('/api/events')
    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data) as { type?: string }
        if (data.type !== 'vision_update') return
        const update = data as VisionUpdateEvent
        setFootage((prev) =>
          prev.map((row) => {
            if (row.id !== update.clip_id) return row
            if (update.event === 'stage_update') {
              return { ...row, vision_stage: update.stage }
            }
            if (update.event === 'completed') {
              return { ...row, vision_status: 'completed', vision_stage: null }
            }
            if (update.event === 'failed') {
              return { ...row, vision_status: 'failed', vision_stage: null }
            }
            return row
          })
        )
      } catch { /* malformed event */ }
    }
    return () => es.close()
  }, [])

  // Delete footage + R2 file
  const handleDelete = useCallback(async (id: string) => {
    setDeletingId(id)
    try {
      const res = await fetch(`/api/footage/${id}`, { method: 'DELETE' })
      if (res.ok) {
        setFootage((prev) => prev.filter((r) => r.id !== id))
        setDeleteConfirmId(null)
      }
    } finally {
      setDeletingId(null)
    }
  }, [])

  // ── Viewer mode ─────────────────────────────────────────────────────────────
  if (selectedClip) {
    return (
      <div>
        <button
          type="button"
          onClick={() => setSelectedClip(null)}
          className="mb-6 text-muted hover:text-offwhite transition-colors duration-200 text-sm tracking-widest uppercase font-body"
        >
          ← Back to footage
        </button>
        <FootageViewTab reviewClip={selectedClip} />
      </div>
    )
  }

  // ── Grid mode ───────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col">
      <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
        View Footage
      </h2>
      <p className="text-[0.84rem] text-muted font-light mb-8">
        Your uploaded footage. Ready clips can be reviewed in the film room.
      </p>

      {loading && (
        <p className="text-[0.74rem] text-muted/70 font-light mb-4">
          Cross-referencing with LeBron&apos;s 11pm film session&hellip;
        </p>
      )}
      {error && (
        <p className="text-[0.74rem] text-accent font-light mb-4">{error}</p>
      )}
      {!loading && footage.length === 0 && !error && (
        <p className="text-[0.74rem] text-muted/50 font-light">
          Nothing here yet. Upload some footage to get started.
        </p>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {footage.map((row) => {
          const isReady = row.vision_status === 'completed'
          const isProcessing = row.vision_status === 'processing'
          const progress = getProgress(row)
          const statusLabel = isProcessing && row.vision_stage
            ? (STAGE_LABELS[row.vision_stage] ?? row.vision_stage.replace(/_/g, ' '))
            : (STATUS_LABEL[row.vision_status] ?? row.vision_status)
          const statusColor = STATUS_COLOR[row.vision_status] ?? 'text-muted/60 bg-white/[0.04]'
          const displayDate = row.game_date
            ? new Date(row.game_date).toLocaleDateString()
            : new Date(row.created_at).toLocaleDateString()
          const isConfirmingDelete = deleteConfirmId === row.id
          const isDeleting = deletingId === row.id

          return (
            <div
              key={row.id}
              onClick={() => { if (isReady && !isConfirmingDelete) setSelectedClip(rowToClip(row)) }}
              className={`border border-[rgba(200,136,58,0.12)] rounded-sm bg-[rgba(200,136,58,0.015)] overflow-hidden flex flex-col relative ${
                isReady ? 'cursor-pointer hover:border-brand/40 hover:bg-[rgba(200,136,58,0.04)] transition-colors duration-150' : ''
              }`}
            >
              <div className="p-4 flex flex-col gap-2 flex-1">
                {/* Header row */}
                <div className="flex items-center justify-between gap-2">
                  <span className="text-[0.7rem] text-muted/70 font-body">{displayDate}</span>
                  <span className={`text-[0.62rem] tracking-[0.08em] uppercase px-2 py-0.5 rounded-sm font-body shrink-0 ${statusColor} ${isProcessing ? 'animate-pulse' : ''}`}>
                    {statusLabel}
                  </span>
                </div>

                {/* Filename */}
                <div className="font-display text-base text-offwhite truncate">
                  {row.filename}
                </div>

                {/* Season */}
                {row.game_season && (
                  <div className="text-[0.68rem] text-muted/60 font-body">
                    Season {row.game_season}
                  </div>
                )}

                {/* Action / confirm row */}
                <div className="mt-auto pt-2" onClick={(e) => e.stopPropagation()}>
                  {isConfirmingDelete ? (
                    <div className="flex items-center gap-3">
                      <span className="text-[0.68rem] text-muted font-body">Delete this clip?</span>
                      <button
                        type="button"
                        disabled={isDeleting}
                        onClick={() => handleDelete(row.id)}
                        className="text-[0.65rem] tracking-[0.1em] uppercase text-accent hover:text-accent/80 font-body disabled:opacity-40 transition-colors"
                      >
                        {isDeleting ? 'Deleting...' : 'Confirm'}
                      </button>
                      <button
                        type="button"
                        onClick={() => setDeleteConfirmId(null)}
                        className="text-[0.65rem] tracking-[0.1em] uppercase text-muted hover:text-offwhite font-body transition-colors"
                      >
                        Cancel
                      </button>
                    </div>
                  ) : (
                    <div className="flex items-center justify-between">
                      {isReady ? (
                        <span className="text-[0.65rem] text-emerald-400/70 font-body tracking-[0.08em] uppercase">
                          Click to review
                        </span>
                      ) : row.vision_status === 'awaiting_game' ? (
                        <span className="text-[0.65rem] text-muted/40 font-body">
                          Link a game to process
                        </span>
                      ) : (
                        <span className="text-[0.65rem] text-muted/40 font-body">
                          &nbsp;
                        </span>
                      )}
                      <button
                        type="button"
                        onClick={() => setDeleteConfirmId(row.id)}
                        className="text-[0.65rem] tracking-[0.08em] uppercase text-muted/50 hover:text-accent font-body transition-colors duration-150"
                      >
                        Delete
                      </button>
                    </div>
                  )}
                </div>
              </div>

              {/* Progress bar track */}
              {progress !== null && (
                <div className="h-0.5 w-full bg-white/[0.05]">
                  <div
                    className={`h-full rounded-full transition-all duration-700 ${
                      progress === -1 ? 'bg-accent w-full' : progress === 100 ? 'bg-emerald-400' : 'bg-brand'
                    }`}
                    style={{ width: progress === -1 ? '100%' : `${progress}%` }}
                  />
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
