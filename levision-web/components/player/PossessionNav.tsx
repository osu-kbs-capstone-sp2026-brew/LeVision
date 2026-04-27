'use client'

import type { PossessionSegment } from '@/lib/types'

type Props = {
  currentSegment: PossessionSegment | null
  segIdx: number
  totalSegments: number
  hasPrev: boolean
  hasNext: boolean
  onPrev: () => void
  onNext: () => void
  /** Names of currently selected players (empty = no selection). */
  selectedNames: string[]
}

function formatTime(sec: number): string {
  const m = Math.floor(sec / 60)
  const s = sec % 60
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
}

/** "LeBron James" → "L. James" */
function abbreviateName(full: string): string {
  const parts = full.trim().split(/\s+/)
  if (parts.length < 2) return full
  return `${parts[0][0]}. ${parts[parts.length - 1]}`
}

function buildContextLabel(selectedNames: string[], totalSegments: number): string {
  if (selectedNames.length === 1) {
    return `${abbreviateName(selectedNames[0])} · ${totalSegments} possession segment${totalSegments !== 1 ? 's' : ''}`
  }
  return `${selectedNames.length} players · ${totalSegments} segment${totalSegments !== 1 ? 's' : ''}`
}

export default function PossessionNav({
  currentSegment,
  segIdx,
  totalSegments,
  hasPrev,
  hasNext,
  onPrev,
  onNext,
  selectedNames,
}: Props) {
  if (selectedNames.length === 0) {
    return (
      <div className="flex items-center justify-center px-4 py-2 border border-[rgba(200,136,58,0.1)] rounded-sm bg-[rgba(255,255,255,0.02)]">
        <p className="text-[0.72rem] text-muted/50 font-light">
          Select a player to navigate their possessions
        </p>
      </div>
    )
  }

  if (totalSegments === 0) {
    return (
      <div className="flex items-center justify-center px-4 py-2 border border-[rgba(200,136,58,0.1)] rounded-sm bg-[rgba(255,255,255,0.02)]">
        <p className="text-[0.72rem] text-muted/50 font-light">
          No possession clips found for selected player{selectedNames.length > 1 ? 's' : ''}
        </p>
      </div>
    )
  }

  const contextLabel = buildContextLabel(selectedNames, totalSegments)

  return (
    <div className="flex items-center gap-3 px-4 py-2 border border-[rgba(200,136,58,0.15)] rounded-sm bg-[rgba(255,255,255,0.02)]">
      {/* Prev */}
      <button
        type="button"
        onClick={onPrev}
        disabled={!hasPrev}
        className={[
          'flex items-center justify-center h-7 w-7 rounded-sm border transition-colors text-[0.8rem] flex-shrink-0',
          hasPrev
            ? 'border-brand/40 text-brand hover:bg-brand/10 hover:border-brand/60'
            : 'border-white/10 text-muted/30 cursor-not-allowed',
        ].join(' ')}
        aria-label="Previous possession"
      >
        ‹
      </button>

      {/* Centre info */}
      <div className="flex-1 text-center min-w-0">
        <p className="text-[0.7rem] text-muted/60 font-light truncate">
          {contextLabel}
        </p>
        {currentSegment && (
          <p className="text-[0.78rem] text-offwhite font-medium font-mono mt-0.5">
            {formatTime(currentSegment.start)}
            <span className="text-muted/50 mx-1">→</span>
            {formatTime(currentSegment.end)}
            <span className="text-muted/40 ml-2 text-[0.62rem] font-sans font-light">
              {segIdx + 1} of {totalSegments}
            </span>
          </p>
        )}
      </div>

      {/* Next */}
      <button
        type="button"
        onClick={onNext}
        disabled={!hasNext}
        className={[
          'flex items-center justify-center h-7 w-7 rounded-sm border transition-colors text-[0.8rem] flex-shrink-0',
          hasNext
            ? 'border-brand/40 text-brand hover:bg-brand/10 hover:border-brand/60'
            : 'border-white/10 text-muted/30 cursor-not-allowed',
        ].join(' ')}
        aria-label="Next possession"
      >
        ›
      </button>
    </div>
  )
}
