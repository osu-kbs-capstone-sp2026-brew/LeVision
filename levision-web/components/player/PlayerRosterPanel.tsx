'use client'

import type { RosterEntry } from '@/hooks/usePossessionFilter'

type Props = {
  roster: RosterEntry[]
  selectedPids: Set<string>
  onToggle: (pid: string) => void
  onClear: () => void
  awayTeamName?: string
  homeTeamName?: string
}

function RosterSection({
  label,
  entries,
  selectedPids,
  onToggle,
}: {
  label: string
  entries: RosterEntry[]
  selectedPids: Set<string>
  onToggle: (pid: string) => void
}) {
  if (entries.length === 0) return null
  return (
    <div>
      <p className="px-4 py-1.5 text-[0.58rem] uppercase tracking-[0.22em] text-muted/50 font-medium border-b border-[rgba(200,136,58,0.06)] bg-[rgba(255,255,255,0.015)]">
        {label}
      </p>
      <ul className="divide-y divide-[rgba(200,136,58,0.05)]">
        {entries.map((entry) => {
          const isSelected = selectedPids.has(entry.pid)
          return (
            <li key={entry.pid}>
              <button
                type="button"
                onClick={() => onToggle(entry.pid)}
                className={[
                  'w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors',
                  isSelected
                    ? 'bg-brand/10 hover:bg-brand/15'
                    : 'hover:bg-white/[0.03]',
                ].join(' ')}
              >
                {/* Checkbox */}
                <span
                  aria-hidden
                  className={[
                    'flex-shrink-0 h-4 w-4 rounded-sm border transition-colors',
                    isSelected
                      ? 'border-brand bg-brand/80'
                      : 'border-white/20 bg-transparent',
                  ].join(' ')}
                >
                  {isSelected && (
                    <svg viewBox="0 0 10 10" fill="none" className="w-full h-full p-[2px]">
                      <path
                        d="M2 5l2.5 2.5L8 3"
                        stroke="white"
                        strokeWidth="1.5"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                  )}
                </span>

                {/* Name */}
                <div className="flex-1 min-w-0">
                  <p
                    className={[
                      'text-[0.8rem] font-medium truncate leading-tight',
                      isSelected ? 'text-offwhite' : 'text-offwhite/80',
                    ].join(' ')}
                  >
                    {entry.name}
                  </p>
                </div>

                {/* Clip count badge */}
                <span
                  className={[
                    'flex-shrink-0 text-[0.6rem] font-mono px-1.5 py-0.5 rounded-sm tabular-nums',
                    entry.segmentCount > 0
                      ? isSelected
                        ? 'bg-brand/20 text-brand'
                        : 'bg-white/5 text-muted/60'
                      : 'text-muted/25',
                  ].join(' ')}
                  title={`${entry.segmentCount} possession clip${entry.segmentCount !== 1 ? 's' : ''} · ${entry.possessionSeconds}s total`}
                >
                  {entry.segmentCount > 0 ? `${entry.segmentCount} clip${entry.segmentCount !== 1 ? 's' : ''}` : '—'}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}

export default function PlayerRosterPanel({
  roster,
  selectedPids,
  onToggle,
  onClear,
  awayTeamName,
  homeTeamName,
}: Props) {
  const totalClips = roster.reduce((sum, e) => sum + e.segmentCount, 0)

  // Partition into Away / Home sections when team names are known
  const awayEntries = awayTeamName
    ? roster.filter((e) => e.teamName === awayTeamName)
    : []
  const homeEntries = homeTeamName
    ? roster.filter((e) => e.teamName === homeTeamName)
    : []
  // Fallback: flat list when team names aren't available
  const flatEntries =
    awayTeamName || homeTeamName ? [] : roster

  return (
    <div className="flex flex-col h-full border border-[rgba(200,136,58,0.15)] rounded-sm bg-[rgba(255,255,255,0.02)] overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-[rgba(200,136,58,0.1)] flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 className="font-display text-offwhite text-[0.9rem] tracking-[0.08em] uppercase">
            Select Player
          </h3>
          <p className="text-[0.65rem] text-muted/55 font-light mt-0.5">
            {totalClips > 0
              ? `${totalClips} possession clip${totalClips !== 1 ? 's' : ''} found`
              : 'No possession data for this clip'}
          </p>
        </div>
        {selectedPids.size > 0 && (
          <button
            type="button"
            onClick={onClear}
            className="flex-shrink-0 text-[0.65rem] text-muted/60 hover:text-brand transition-colors mt-0.5 font-light underline underline-offset-2"
          >
            Clear
          </button>
        )}
      </div>

      {/* Roster */}
      <div className="flex-1 overflow-y-auto">
        {roster.length === 0 ? (
          <div className="flex items-center justify-center h-24 px-4">
            <p className="text-[0.72rem] text-muted/40 font-light text-center">
              No roster data available
            </p>
          </div>
        ) : flatEntries.length > 0 ? (
          // Flat list when team names aren't supplied
          <ul className="divide-y divide-[rgba(200,136,58,0.05)]">
            {flatEntries.map((entry) => {
              const isSelected = selectedPids.has(entry.pid)
              return (
                <li key={entry.pid}>
                  <button
                    type="button"
                    onClick={() => onToggle(entry.pid)}
                    className={[
                      'w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors',
                      isSelected ? 'bg-brand/10 hover:bg-brand/15' : 'hover:bg-white/[0.03]',
                    ].join(' ')}
                  >
                    <span
                      aria-hidden
                      className={[
                        'flex-shrink-0 h-4 w-4 rounded-sm border transition-colors',
                        isSelected ? 'border-brand bg-brand/80' : 'border-white/20 bg-transparent',
                      ].join(' ')}
                    >
                      {isSelected && (
                        <svg viewBox="0 0 10 10" fill="none" className="w-full h-full p-[2px]">
                          <path d="M2 5l2.5 2.5L8 3" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      )}
                    </span>
                    <div className="flex-1 min-w-0">
                      <p className={['text-[0.8rem] font-medium truncate leading-tight', isSelected ? 'text-offwhite' : 'text-offwhite/80'].join(' ')}>
                        {entry.name}
                      </p>
                      <p className="text-[0.65rem] text-muted/50 font-light truncate">{entry.teamName}</p>
                    </div>
                    <span className={['flex-shrink-0 text-[0.6rem] font-mono px-1.5 py-0.5 rounded-sm tabular-nums', entry.segmentCount > 0 ? isSelected ? 'bg-brand/20 text-brand' : 'bg-white/5 text-muted/60' : 'text-muted/25'].join(' ')}>
                      {entry.segmentCount > 0 ? `${entry.segmentCount} clip${entry.segmentCount !== 1 ? 's' : ''}` : '—'}
                    </span>
                  </button>
                </li>
              )
            })}
          </ul>
        ) : (
          // Two-section layout with Away / Home headers
          <>
            <RosterSection
              label={awayTeamName ?? 'Away'}
              entries={awayEntries}
              selectedPids={selectedPids}
              onToggle={onToggle}
            />
            <RosterSection
              label={homeTeamName ?? 'Home'}
              entries={homeEntries}
              selectedPids={selectedPids}
              onToggle={onToggle}
            />
          </>
        )}
      </div>
    </div>
  )
}
