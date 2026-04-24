import { execFile } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { promisify } from 'node:util'
import type { ChatMessage } from '@/lib/chat/types'

const execFileAsync = promisify(execFile)

const TEAM_HINT_PATTERN =
  /\b(?:atl|hawks|bos|celtics|celts|bkn|nets|cha|hornets|chi|bulls|cle|cavs|cavaliers|dal|mavs|mavericks|den|nuggets|det|pistons|gsw|warriors|dubs|hou|rockets|ind|pacers|lac|clippers|clips|lal|lakers|mem|grizzlies|grizz|mil|bucks|min|timberwolves|twolves|t-wolves|nop|pelicans|pels|nyk|knicks|okc|thunder|phi|sixers|76ers|phx|suns|por|blazers|trail blazers|trailblazers|sac|kings|sas|spurs|tor|raptors|uta|jazz|was|wizards)\b/i
const TEAM_QUERY_PATTERN =
  /\b(?:who|what|when|where|how|did|do|does|won|win|score|scored|play|played|game|games|last|recent|today|yesterday|tonight|stats?|points?|assists?|rebounds?|vs|versus|against)\b/i
const FOLLOW_UP_NBA_PATTERN =
  /\b(?:him|them|that game|that one|that team|that player|what about him|what about them|what about that|how about him|how about them|how about that)\b/i

type CliPayload = {
  matched?: boolean
  answer?: string | null
  error?: string
}

export type NbaToolOutcome =
  | { kind: 'no_match' }
  | { kind: 'answer'; answer: string }
  | { kind: 'error'; error: string }

function toBool(value: string | undefined, defaultValue: boolean): boolean {
  if (value == null) {
    return defaultValue
  }
  const text = value.trim().toLowerCase()
  return ['1', 'true', 'yes', 'on'].includes(text)
}

function latestUserMessage(messages: ChatMessage[]): ChatMessage | undefined {
  return [...messages].reverse().find((message) => message.role === 'user')
}

function hasNbaKeyword(text: string): boolean {
  const lower = text.trim().toLowerCase()
  if (!lower) {
    return false
  }

  const keywords = [
    'nba',
    'points',
    'assists',
    'rebounds',
    'steals',
    'blocks',
    'turnovers',
    'play-by-play',
    'play by play',
    'season',
    'game log',
    'last ',
    'past ',
    'recent ',
    'yesterday',
    'today',
    'tonight',
    'event ',
    'game id',
  ]
  return keywords.some((keyword) => lower.includes(keyword))
}

function isTeamOnlyPrompt(text: string): boolean {
  const trimmed = text.trim()
  return Boolean(trimmed) && TEAM_HINT_PATTERN.test(trimmed) && trimmed.split(/\s+/).length <= 3
}

function shouldTryNbaTools(messages: ChatMessage[]): boolean {
  const latestMessage = latestUserMessage(messages)
  const text = latestMessage?.content?.trim() ?? ''
  if (!text) {
    return false
  }

  const mode = (process.env.LEVISION_NBA_TOOLS_MATCH_MODE || 'hint')
    .trim()
    .toLowerCase()

  if (mode === 'always') {
    return true
  }

  if (hasNbaKeyword(text)) {
    return true
  }

  if (TEAM_HINT_PATTERN.test(text) && (TEAM_QUERY_PATTERN.test(text) || isTeamOnlyPrompt(text))) {
    return true
  }

  if (!FOLLOW_UP_NBA_PATTERN.test(text)) {
    return false
  }

  const priorContext = messages
    .slice(-8, -1)
    .map((message) => message.content)
    .join(' ')

  return hasNbaKeyword(priorContext) || TEAM_HINT_PATTERN.test(priorContext)
}

function resolveRepoRoot(): string {
  const cwd = process.cwd()
  const candidates = [cwd, path.resolve(cwd, '..')]
  for (const candidate of candidates) {
    const scriptPath = path.join(candidate, 'nba_pipeline', 'chat_tools_cli.py')
    if (fs.existsSync(scriptPath)) {
      return candidate
    }
  }
  return path.resolve(cwd, '..')
}

function parseCliPayload(stdout: string): CliPayload {
  const output = stdout.trim()
  if (!output) {
    return {}
  }

  try {
    return JSON.parse(output) as CliPayload
  } catch {
    const lines = output
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
    const lastLine = lines[lines.length - 1]
    if (!lastLine) {
      return {}
    }
    return JSON.parse(lastLine) as CliPayload
  }
}

export async function runNbaToolQuery(
  messages: ChatMessage[]
): Promise<NbaToolOutcome> {
  if (!toBool(process.env.LEVISION_ENABLE_NBA_TOOLS, true)) {
    return { kind: 'no_match' }
  }

  const latestMessage = latestUserMessage(messages)
  const query = latestMessage?.content?.trim() ?? ''
  if (!query || !shouldTryNbaTools(messages)) {
    return { kind: 'no_match' }
  }

  const repoRoot = resolveRepoRoot()
  const scriptPath = path.join(repoRoot, 'nba_pipeline', 'chat_tools_cli.py')
  if (!fs.existsSync(scriptPath)) {
    return { kind: 'error', error: 'NBA tools are not installed on the backend.' }
  }

  const pythonBin = process.env.LEVISION_PYTHON_BIN || 'python3'
  const timeoutMs = Number(process.env.LEVISION_NBA_TOOL_TIMEOUT_MS || '300000')
  const recentHistory = messages.slice(-12)

  try {
    const { stdout } = await execFileAsync(
      pythonBin,
      [
        '-m',
        'nba_pipeline.chat_tools_cli',
        '--query',
        query,
        '--history-json',
        JSON.stringify(recentHistory),
        '--json',
      ],
      {
        cwd: repoRoot,
        timeout: timeoutMs,
        maxBuffer: 8 * 1024 * 1024,
      }
    )

    const payload = parseCliPayload(stdout)
    if (!payload.matched) {
      return { kind: 'no_match' }
    }
    if (payload.answer) {
      return { kind: 'answer', answer: payload.answer }
    }
    if (payload.error) {
      return { kind: 'error', error: payload.error }
    }

    return {
      kind: 'error',
      error: 'NBA tool matched the query but returned no answer.',
    }
  } catch (error) {
    const details =
      error instanceof Error && error.message
        ? error.message
        : 'unknown tool failure'
    return {
      kind: 'error',
      error: `NBA tool execution failed: ${details}`,
    }
  }
}
