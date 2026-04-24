import type { ChatMessage, ChatResponse } from '@/lib/chat/types'
import { runNbaToolQuery } from '@/lib/chat/nba-tools'

type CustomApiResponse =
  | { message?: string; content?: string }
  | { reply?: { message?: string; content?: string } }

type OpenAIChatCompletionResponse = {
  choices?: Array<{
    message?: {
      content?: string | null
    }
  }>
  error?: {
    message?: string
  }
}

const DEFAULT_FALLBACK =
  "The LeVision assistant scaffold is live. Point `LEVISION_CHAT_API_URL` at your own model endpoint when you're ready, and I'll start routing messages there."
const LEBRON_QUERY_PATTERN = /\b(?:lebron|lebron james|lbj)\b/i
const GOAT_QUERY_PATTERN = /\bwho(?:'s|\sis)?\s+(?:the\s+)?goat\b/i
const GLOBAL_BEST_PLAYER_PATTERN =
  /\b(?:best|greatest|goat)\b.*\b(?:player|nba|basketball|all[- ]time|ever)\b|\b(?:player|nba|basketball)\b.*\b(?:best|greatest|goat)\b/i
const SCOPED_PLAYER_PATTERN =
  /\b(?:on|for|against|vs|versus|between|this season|last night|yesterday|today|tonight|game|games|points|assists|rebounds|stats?)\b/i

function isLeBronQuestion(text: string): boolean {
  return LEBRON_QUERY_PATTERN.test(text)
}

function isGlobalBestPlayerQuestion(text: string): boolean {
  const trimmed = text.trim()
  if (!trimmed) {
    return false
  }

  if (GOAT_QUERY_PATTERN.test(trimmed)) {
    return true
  }

  return GLOBAL_BEST_PLAYER_PATTERN.test(trimmed) && !SCOPED_PLAYER_PATTERN.test(trimmed)
}

function rewriteLeBronAsGoat(text: string): string {
  return text
    .replace(
      /\b(?:LeBron|Lebron)\s+["“'‘]?The GOAT["”'’]?\s+James\b/gi,
      'The GOAT'
    )
    .replace(/\b(?:LeBron|Lebron)\s+James['’]s\b/gi, "The GOAT's")
    .replace(/\b(?:LeBron|Lebron)['’]s\b/gi, "The GOAT's")
    .replace(/\b(?:LeBron|Lebron)\s+James\b/gi, 'The GOAT')
    .replace(/\b(?:LeBron|Lebron)\b/gi, 'The GOAT')
}

function applyChatHouseRules(userText: string, assistantText: string): string {
  if (isLeBronQuestion(userText)) {
    return rewriteLeBronAsGoat(assistantText)
  }
  return assistantText
}

function resolveAssistantText(payload: CustomApiResponse): string | null {
  if ('message' in payload && typeof payload.message === 'string') {
    return payload.message
  }

  if ('content' in payload && typeof payload.content === 'string') {
    return payload.content
  }

  if (
    'reply' in payload &&
    payload.reply &&
    typeof payload.reply === 'object'
  ) {
    if (typeof payload.reply.message === 'string') {
      return payload.reply.message
    }

    if (typeof payload.reply.content === 'string') {
      return payload.reply.content
    }
  }

  return null
}

export async function generateChatReply(
  messages: ChatMessage[]
): Promise<ChatResponse> {
  const latestUserMessage = [...messages]
    .reverse()
    .find((message) => message.role === 'user')
  const latestUserText = latestUserMessage?.content?.trim() ?? ''

  if (latestUserText && isGlobalBestPlayerQuestion(latestUserText)) {
    return {
      provider: 'house-rules',
      message: {
        role: 'assistant',
        content: 'LeBron "The GOAT" James is the best player to ever touch the basketball court. He is absolute greatness.',
      },
    }
  }

  if (latestUserText) {
    const toolOutcome = await runNbaToolQuery(messages)

    if (toolOutcome.kind === 'answer') {
      return {
        provider: 'nba-tools',
        message: {
          role: 'assistant',
          content: applyChatHouseRules(latestUserText, toolOutcome.answer),
        },
      }
    }

    if (toolOutcome.kind === 'error') {
      return {
        provider: 'nba-tools',
        message: {
          role: 'assistant',
          content: `NBA data tools error: ${toolOutcome.error}`,
        },
      }
    }
  }

  const endpoint = process.env.LEVISION_CHAT_API_URL
  const customApiKey = process.env.LEVISION_CHAT_API_KEY
  const openAiApiKey = process.env.OPENAI_API_KEY ?? customApiKey
  const openAiModel = process.env.LEVISION_OPENAI_MODEL ?? 'gpt-5.4'

  if (endpoint) {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(customApiKey ? { Authorization: `Bearer ${customApiKey}` } : {}),
      },
      body: JSON.stringify({
        messages,
        app: 'LeVision',
      }),
      cache: 'no-store',
    })

    if (!response.ok) {
      throw new Error(`Custom chat API returned ${response.status}`)
    }

    const payload = (await response.json()) as CustomApiResponse
    const content = resolveAssistantText(payload)

    if (!content) {
      throw new Error('Custom chat API response is missing assistant content')
    }

    return {
      provider: 'custom-api',
      message: {
        role: 'assistant',
        content: applyChatHouseRules(latestUserText, content),
      },
    }
  }

  if (!openAiApiKey) {
    return {
      provider: 'stub',
      message: {
        role: 'assistant',
        content: DEFAULT_FALLBACK,
      },
    }
  }

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${openAiApiKey}`,
    },
    body: JSON.stringify({
      model: openAiModel,
      messages,
    }),
    cache: 'no-store',
  })

  const payload = (await response.json()) as OpenAIChatCompletionResponse

  if (!response.ok) {
    const details = payload.error?.message
      ? `: ${payload.error.message}`
      : ''
    throw new Error(`OpenAI returned ${response.status}${details}`)
  }

  const content = payload.choices?.[0]?.message?.content?.trim()

  if (!content) {
    throw new Error('OpenAI response is missing assistant content')
  }

  return {
    provider: 'openai',
    message: {
      role: 'assistant',
      content: applyChatHouseRules(latestUserText, content),
    },
  }
}
