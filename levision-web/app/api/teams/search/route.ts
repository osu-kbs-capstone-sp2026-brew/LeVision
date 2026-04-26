import { NextResponse } from 'next/server'

export const runtime = 'nodejs'

const ESPN_BASE    = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba'
const ESPN_HEADERS = { 'User-Agent': 'LeVision/1.0' }

// ── Types ─────────────────────────────────────────────────────────────────────

export type ESPNTeam = {
  id: string
  abbreviation: string
  displayName: string  // "Golden State Warriors"
  shortName: string    // "Warriors"
  location: string     // "Golden State"
}

// ── ESPN wrapper ──────────────────────────────────────────────────────────────

// Returns all 30 NBA teams. Cached 24h — IDs are static, logos/names rarely change.
async function fetchAllTeams(): Promise<ESPNTeam[]> {
  const res = await fetch(
    `${ESPN_BASE}/teams?limit=100`,
    { headers: ESPN_HEADERS, next: { revalidate: 86400 } },
  )
  if (!res.ok) throw new Error(`ESPN teams failed: ${res.status}`)
  const data = await res.json() as {
    sports?: Array<{ leagues?: Array<{ teams?: Array<{ team: Record<string, string> }> }> }>
  }
  const raw = data.sports?.[0]?.leagues?.[0]?.teams ?? []
  return raw.map((t) => ({
    id:           t.team.id,
    abbreviation: t.team.abbreviation,
    displayName:  t.team.displayName,
    shortName:    t.team.shortDisplayName ?? '',
    location:     t.team.location,
  }))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Returns all teams whose abbreviation, short name, location, or full name contain the query
function searchTeams(query: string, teams: ESPNTeam[]): ESPNTeam[] {
  const q = query.toLowerCase().trim()
  return teams.filter((t) =>
    t.abbreviation.toLowerCase().includes(q) ||
    t.shortName.toLowerCase().includes(q)    ||
    t.location.toLowerCase().includes(q)     ||
    t.displayName.toLowerCase().includes(q)
  )
}

// ── Route ─────────────────────────────────────────────────────────────────────
// Resolves a team name string to an ESPN team object (including its numeric ID).
// The frontend uses this ID to call /api/games/search.
// Required: q (query string, e.g. "warriors", "gsw", "golden state")
// Optional: all=true — returns all 30 teams (for populating a dropdown)

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const q   = searchParams.get('q')
  const all = searchParams.get('all') === 'true'

  try {
    const teams = await fetchAllTeams()

    if (all) {
      return NextResponse.json({ teams })
    }

    if (!q) {
      return NextResponse.json({ error: 'q or all=true is required' }, { status: 400 })
    }

    const matches = searchTeams(q, teams)
    return NextResponse.json({ teams: matches })
  } catch (err) {
    console.error('teams/search failed', err)
    return NextResponse.json({ error: 'Failed to fetch teams' }, { status: 500 })
  }
}
