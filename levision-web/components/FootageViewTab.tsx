'use client'

import { useEffect, useMemo, useState } from 'react'
import { RoleSwitch } from '@/components/role-ui'
import { useFootageLibrary } from '@/hooks/useFootageLibrary'
import type { FootageClip } from '@/lib/footage-library'

const PAST_GAME_ID_PREFIX = 'past-game-'

function isPastGameClip(clip: FootageClip | null): boolean {
  return clip != null && clip.id.startsWith(PAST_GAME_ID_PREFIX)
}

type Props = {
  reviewClip?: FootageClip | null
}

export default function FootageViewTab({ reviewClip = null }: Props) {
  const { clips, loading, error } = useFootageLibrary()
  const [activeId, setActiveId] = useState<string | null>(null)

  const mergedClips = useMemo(() => {
    if (!reviewClip) return clips
    const rest = clips.filter((c) => c.id !== reviewClip.id)
    return [reviewClip, ...rest]
  }, [clips, reviewClip])

  const active = mergedClips.find((c) => c.id === activeId) ?? null

  useEffect(() => {
    if (reviewClip) {
      setActiveId(reviewClip.id)
    } else {
      setActiveId(null)
    }
  }, [reviewClip])

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
          View Footage
        </h2>
        <p className="text-[0.84rem] text-muted font-light max-w-[52ch]">
          Watch processed game film from your library. Playback is loaded from the viewing
          pipeline, which is separate from where files are uploaded.
        </p>
      </div>

      <div className="min-h-[min(60vh,520px)] flex flex-col">
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
                key={active.playbackUrl}
                controls
                playsInline
                className="w-full h-full object-contain"
                src={active.playbackUrl}
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
                  <RoleSwitch
                    coach="No plays saved. Even Phil Jackson wrote things down."
                    fan="Nothing here. Emptier than Cleveland's trophy case before 2016."
                    player="No footage yet. LeBron didn't become LeBron by skipping film."
                  />
                </p>
              </div>
            )}
          </div>
          {active?.playbackUrl && (
            <div className="px-4 py-3 border-t border-[rgba(200,136,58,0.1)]">
              <h3 className="font-display text-offwhite text-lg tracking-wide">{active.title}</h3>
              <p className="text-[0.68rem] text-muted/55 font-light mt-1 tracking-wide uppercase">
                {isPastGameClip(active)
                  ? 'Opened from Past Games'
                  : 'Playback source: library pipeline (not upload ingest)'}
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
