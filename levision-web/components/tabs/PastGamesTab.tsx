'use client'

import { useEffect, useState } from 'react'
import type { Game } from '@/lib/types'
import type { FootageClip } from '@/lib/footage-library'

type UploadedVideo = {
  key: string
  name: string
  size: number
  lastModified: string | null
  url: string
}

const MOCK_GAMES: Game[] = [
  {
    id: '1',
    homeTeam: 'Warriors',
    awayTeam: 'Lakers',
    homeScore: 115,
    awayScore: 110,
    date: '2024-03-01',
    videoUrl: 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4',
    stats: {
      homePoints: 115, awayPoints: 110,
      homeRebounds: 38, awayRebounds: 45,
      homeAssists: 25, awayAssists: 28,
      homeSteals: 6, awaySteals: 8,
      homeBlocks: 3, awayBlocks: 5,
      homeTurnovers: 15, awayTurnovers: 12,
      homeFouls: 20, awayFouls: 18,
      homeFgMade: 40, awayFgMade: 42,
      homeFgAttempted: 85, awayFgAttempted: 88,
      homeThreeMade: 15, awayThreeMade: 12,
      homeThreeAttempted: 38, awayThreeAttempted: 32,
      homeFtMade: 15, awayFtMade: 19,
      homeFtAttempted: 20, awayFtAttempted: 24,
      players: {
        home: [
          { name: 'Stephen Curry', points: 32, rebounds: 6, assists: 8, minutes: 36, steals: 3, blocks: 0 },
          { name: 'Klay Thompson', points: 22, rebounds: 4, assists: 3, minutes: 34, steals: 1, blocks: 1 },
          { name: 'Andrew Wiggins', points: 20, rebounds: 8, assists: 2, minutes: 32, steals: 2, blocks: 1 },
          { name: 'Draymond Green', points: 15, rebounds: 7, assists: 6, minutes: 30, steals: 2, blocks: 1 },
          { name: 'Jonathan Kuminga', points: 21, rebounds: 5, assists: 4, minutes: 28, steals: 1, blocks: 0 },
        ],
        away: [
          { name: 'LeBron James', points: 28, rebounds: 10, assists: 8, minutes: 35, steals: 2, blocks: 1 },
          { name: 'Anthony Davis', points: 25, rebounds: 12, assists: 3, minutes: 32, steals: 1, blocks: 3 },
          { name: 'Austin Reaves', points: 18, rebounds: 5, assists: 6, minutes: 28, steals: 1, blocks: 0 },
          { name: "D'Angelo Russell", points: 15, rebounds: 3, assists: 7, minutes: 30, steals: 1, blocks: 0 },
          { name: 'Rui Hachimura', points: 12, rebounds: 8, assists: 2, minutes: 25, steals: 0, blocks: 1 },
        ],
      },
    },
  },
  {
    id: '2',
    homeTeam: 'Celtics',
    awayTeam: 'Heat',
    homeScore: 102,
    awayScore: 98,
    date: '2024-02-28',
    stats: {
      homePoints: 102, awayPoints: 98,
      homeRebounds: 42, awayRebounds: 40,
      homeAssists: 22, awayAssists: 20,
      homeSteals: 7, awaySteals: 9,
      homeBlocks: 4, awayBlocks: 2,
      homeTurnovers: 14, awayTurnovers: 11,
      homeFouls: 22, awayFouls: 19,
      homeFgMade: 38, awayFgMade: 36,
      homeFgAttempted: 82, awayFgAttempted: 80,
      homeThreeMade: 10, awayThreeMade: 8,
      homeThreeAttempted: 28, awayThreeAttempted: 25,
      homeFtMade: 16, awayFtMade: 18,
      homeFtAttempted: 20, awayFtAttempted: 22,
      players: {
        home: [
          { name: 'Jaylen Brown', points: 24, rebounds: 8, assists: 5, minutes: 34, steals: 2, blocks: 1 },
          { name: 'Jayson Tatum', points: 26, rebounds: 9, assists: 7, minutes: 36, steals: 1, blocks: 0 },
          { name: 'Al Horford', points: 12, rebounds: 7, assists: 3, minutes: 28, steals: 0, blocks: 2 },
          { name: 'Jrue Holiday', points: 18, rebounds: 4, assists: 6, minutes: 32, steals: 2, blocks: 0 },
          { name: 'Derrick White', points: 22, rebounds: 3, assists: 1, minutes: 30, steals: 1, blocks: 1 },
        ],
        away: [
          { name: 'Jimmy Butler', points: 28, rebounds: 10, assists: 4, minutes: 35, steals: 2, blocks: 1 },
          { name: 'Bam Adebayo', points: 20, rebounds: 12, assists: 3, minutes: 33, steals: 1, blocks: 2 },
          { name: 'Tyler Herro', points: 18, rebounds: 5, assists: 6, minutes: 31, steals: 1, blocks: 0 },
          { name: 'Kyle Lowry', points: 15, rebounds: 4, assists: 7, minutes: 29, steals: 1, blocks: 0 },
          { name: 'Duncan Robinson', points: 17, rebounds: 2, assists: 0, minutes: 27, steals: 0, blocks: 0 },
        ],
      },
    },
  },
  {
    id: '3',
    homeTeam: 'Nets',
    awayTeam: '76ers',
    homeScore: 88,
    awayScore: 95,
    date: '2024-02-27',
    stats: {
      homePoints: 88, awayPoints: 95,
      homeRebounds: 35, awayRebounds: 48,
      homeAssists: 18, awayAssists: 30,
      homeSteals: 6, awaySteals: 11,
      homeBlocks: 3, awayBlocks: 7,
      homeTurnovers: 16, awayTurnovers: 10,
      homeFouls: 25, awayFouls: 18,
      homeFgMade: 32, awayFgMade: 35,
      homeFgAttempted: 78, awayFgAttempted: 82,
      homeThreeMade: 8, awayThreeMade: 12,
      homeThreeAttempted: 26, awayThreeAttempted: 32,
      homeFtMade: 16, awayFtMade: 13,
      homeFtAttempted: 20, awayFtAttempted: 18,
      players: {
        home: [
          { name: 'Kevin Durant', points: 25, rebounds: 8, assists: 5, minutes: 35 },
          { name: 'Kyrie Irving', points: 22, rebounds: 5, assists: 6, minutes: 33 },
          { name: 'Ben Simmons', points: 8, rebounds: 10, assists: 7, minutes: 28 },
          { name: 'James Harden', points: 18, rebounds: 6, assists: 8, minutes: 32 },
          { name: 'LaMarcus Aldridge', points: 15, rebounds: 6, assists: 2, minutes: 26 },
        ],
        away: [
          { name: 'Joel Embiid', points: 30, rebounds: 15, assists: 4, minutes: 36 },
          { name: 'James Harden', points: 25, rebounds: 8, assists: 10, minutes: 34 },
          { name: 'Tobias Harris', points: 20, rebounds: 7, assists: 3, minutes: 32 },
          { name: 'Tyrese Maxey', points: 12, rebounds: 4, assists: 8, minutes: 30 },
          { name: 'Danny Green', points: 8, rebounds: 3, assists: 5, minutes: 24 },
        ],
      },
    },
  },
]

function gameToReviewClip(game: Game): FootageClip {
  return {
    id: `past-game-${game.id}`,
    title: `${game.awayTeam} @ ${game.homeTeam}`,
    createdAt: game.date,
    playbackUrl: game.videoUrl ?? null,
    game,
  }
}

export default function PastGamesTab({ onReviewClip }: { onReviewClip: (clip: FootageClip) => void }) {
  const [selectedGame, setSelectedGame] = useState<string | null>(null)
  const [uploadedVideos, setUploadedVideos] = useState<UploadedVideo[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch('/api/upload/list')
      .then((r) => r.json())
      .then((payload: { uploads?: UploadedVideo[]; error?: string }) => {
        if (cancelled) return
        if (payload.error) throw new Error(payload.error)
        setUploadedVideos(payload.uploads ?? [])
      })
      .catch((err: unknown) => {
        if (cancelled) return
        setError(err instanceof Error ? err.message : 'Unable to fetch uploaded videos')
        setUploadedVideos([])
      })
      .finally(() => { if (!cancelled) setLoading(false) })

    return () => { cancelled = true }
  }, [])

  const game = selectedGame ? MOCK_GAMES.find((g) => g.id === selectedGame) ?? null : null

  return (
    <div className="flex flex-col">
      <h2 className="font-display text-offwhite text-[clamp(1.6rem,3vw,2.2rem)] tracking-[0.04em] mb-2">
        Past Games
      </h2>
      <p className="text-[0.84rem] text-muted font-light mb-8">
        Track previous games and pull historical stats.
      </p>

      {loading && (
        <p className="text-[0.74rem] text-muted/70 font-light mb-4">
          Cross-referencing with LeBron&apos;s 11pm film session&hellip;
        </p>
      )}
      {error && (
        <p className="text-[0.74rem] text-accent font-light mb-4">{error}</p>
      )}

      {game ? (
        <div>
          <button
            onClick={() => setSelectedGame(null)}
            className="mb-6 text-muted hover:text-offwhite transition-colors duration-200 text-sm tracking-widest uppercase font-body"
          >
            ← Back to Games
          </button>

          <div className="border border-[rgba(200,136,58,0.12)] rounded-sm bg-[rgba(200,136,58,0.015)] p-6">
            <div className="text-center mb-6">
              <div className="text-sm text-muted font-light mb-2">{game.date}</div>
              <div className="font-display text-4xl text-offwhite">
                {game.awayTeam} @ {game.homeTeam}
              </div>
              <div className="font-display text-3xl text-brand mt-1">
                {game.awayScore} – {game.homeScore}
              </div>
            </div>

            <div className="mb-8 flex flex-col items-center gap-3">
              <button
                type="button"
                onClick={() => onReviewClip(gameToReviewClip(game))}
                className="font-body text-[0.78rem] tracking-[0.14em] uppercase px-8 py-3.5 rounded-sm border border-brand/50 bg-brand/15 text-offwhite hover:bg-brand/25 hover:border-brand transition-colors duration-200 cursor-pointer"
              >
                Review video
              </button>
              {!game.videoUrl && (
                <p className="text-[0.72rem] text-muted/60 font-light text-center max-w-sm">
                  No stream linked for this game. You&apos;ll still open View Footage — add a video URL when playback is available.
                </p>
              )}
            </div>

            {game.stats && (
              <div>
                <h4 className="font-display text-offwhite text-lg mb-6 tracking-wider uppercase text-center">
                  Team Statistics
                </h4>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-8">
                  {[
                    { label: game.awayTeam, pts: game.stats.awayPoints, reb: game.stats.awayRebounds, ast: game.stats.awayAssists, stl: game.stats.awaySteals, blk: game.stats.awayBlocks, to: game.stats.awayTurnovers, pf: game.stats.awayFouls, fgm: game.stats.awayFgMade, fga: game.stats.awayFgAttempted, tpm: game.stats.awayThreeMade, tpa: game.stats.awayThreeAttempted, ftm: game.stats.awayFtMade, fta: game.stats.awayFtAttempted },
                    { label: game.homeTeam, pts: game.stats.homePoints, reb: game.stats.homeRebounds, ast: game.stats.homeAssists, stl: game.stats.homeSteals, blk: game.stats.homeBlocks, to: game.stats.homeTurnovers, pf: game.stats.homeFouls, fgm: game.stats.homeFgMade, fga: game.stats.homeFgAttempted, tpm: game.stats.homeThreeMade, tpa: game.stats.homeThreeAttempted, ftm: game.stats.homeFtMade, fta: game.stats.homeFtAttempted },
                  ].map((side) => (
                    <div key={side.label} className="border border-[rgba(200,136,58,0.12)] rounded-sm p-4 bg-[rgba(200,136,58,0.02)]">
                      <h5 className="font-body text-offwhite text-md mb-4 tracking-wider uppercase text-center">{side.label}</h5>
                      <div className="space-y-3">
                        {[
                          ['Points', side.pts],
                          ['Rebounds', side.reb],
                          ['Assists', side.ast],
                          ['Steals', side.stl],
                          ['Blocks', side.blk],
                          ['Turnovers', side.to],
                          ['Fouls', side.pf],
                          [`FG Made/Att`, `${side.fgm}/${side.fga}`],
                          [`FG %`, `${((side.fgm / side.fga) * 100).toFixed(1)}%`],
                          [`3PT Made/Att`, `${side.tpm}/${side.tpa}`],
                          [`3PT %`, `${((side.tpm / side.tpa) * 100).toFixed(1)}%`],
                          [`FT Made/Att`, `${side.ftm}/${side.fta}`],
                          [`FT %`, `${((side.ftm / side.fta) * 100).toFixed(1)}%`],
                        ].map(([k, v]) => (
                          <div key={String(k)} className="flex justify-between">
                            <span className="text-muted text-sm">{k}</span>
                            <span className="font-body text-offwhite">{v}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>

                {game.stats.players && (
                  <div className="space-y-6">
                    {[
                      { label: game.awayTeam, players: game.stats.players.away },
                      { label: game.homeTeam, players: game.stats.players.home },
                    ].map((side) => (
                      <div key={side.label}>
                        <h5 className="font-display text-offwhite text-md mb-3 tracking-wider uppercase">
                          {side.label} Players
                        </h5>
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b border-[rgba(200,136,58,0.12)]">
                                <th className="text-left py-2 text-muted font-light w-1/2">Player</th>
                                <th className="text-center py-2 text-muted font-light">PTS</th>
                                <th className="text-center py-2 text-muted font-light">REB</th>
                                <th className="text-center py-2 text-muted font-light">AST</th>
                                <th className="text-center py-2 text-muted font-light">MIN</th>
                              </tr>
                            </thead>
                            <tbody>
                              {side.players.map((p, i) => (
                                <tr key={i} className="border-b border-[rgba(200,136,58,0.06)]">
                                  <td className="py-2 text-offwhite font-body">{p.name}</td>
                                  <td className="py-2 text-center text-offwhite font-body">{p.points}</td>
                                  <td className="py-2 text-center text-offwhite font-body">{p.rebounds}</td>
                                  <td className="py-2 text-center text-offwhite font-body">{p.assists}</td>
                                  <td className="py-2 text-center text-offwhite font-body">{p.minutes}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {uploadedVideos.map((video) => (
            <div
              key={video.key}
              className="border border-brand/30 rounded-sm bg-brand/5 overflow-hidden cursor-pointer hover:bg-brand/10 transition-colors duration-200"
              onClick={() =>
                onReviewClip({
                  id: `uploaded-${video.key}`,
                  title: video.name,
                  createdAt: video.lastModified ?? new Date().toISOString(),
                  playbackUrl: video.url,
                })
              }
            >
              <div className="p-4">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-sm text-muted font-light">
                    {video.lastModified ? new Date(video.lastModified).toLocaleDateString() : 'Uploaded video'}
                  </span>
                  <span className="text-xs tracking-widest uppercase px-2 py-1 rounded-sm bg-brand/20 text-brand">
                    Review
                  </span>
                </div>
                <div className="font-display text-lg text-offwhite truncate">{video.name}</div>
                <div className="text-xs text-muted mt-1">{(video.size / (1024 * 1024)).toFixed(1)} MB</div>
              </div>
            </div>
          ))}

          {MOCK_GAMES.map((g) => (
            <div
              key={g.id}
              className="border border-[rgba(200,136,58,0.12)] rounded-sm bg-[rgba(200,136,58,0.015)] overflow-hidden cursor-pointer hover:bg-[rgba(200,136,58,0.03)] transition-colors duration-200"
              onClick={() => setSelectedGame(g.id)}
            >
              <div className="p-4">
                <div className="flex justify-between items-center mb-2">
                  <span className="text-sm text-muted font-light">{g.date}</span>
                  <span className="text-xs tracking-widest uppercase px-2 py-1 rounded-sm bg-muted/20 text-muted">
                    View Stats
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <div>
                    <div className={`font-display text-lg ${g.awayScore > g.homeScore ? 'text-offwhite' : 'text-muted'}`}>{g.awayTeam}</div>
                    <div className={`font-display text-lg ${g.awayScore > g.homeScore ? 'text-muted' : 'text-offwhite'}`}>vs {g.homeTeam}</div>
                  </div>
                  <div className="text-right">
                    <div className={`font-display text-lg ${g.awayScore > g.homeScore ? 'text-offwhite' : 'text-muted'}`}>{g.awayScore}</div>
                    <div className={`font-display text-lg ${g.awayScore > g.homeScore ? 'text-muted' : 'text-offwhite'}`}>{g.homeScore}</div>
                  </div>
                </div>
              </div>
            </div>
          ))}

          {uploadedVideos.length === 0 && !loading && (
            <p className="text-[0.74rem] text-muted/50 font-light col-span-full">
              Nothing here. Emptier than Cleveland&apos;s trophy case before 2016.
            </p>
          )}
        </div>
      )}
    </div>
  )
}
