import type { ChatMessage, ChatResponse } from '@/lib/chat/types'

type CustomApiResponse =
  | { message?: string; content?: string }
  | { reply?: { message?: string; content?: string } }

const DEFAULT_FALLBACK =
  "The LeVision assistant scaffold is live. Point `LEVISION_CHAT_API_URL` at your own model endpoint when you're ready, and I'll start routing messages there."

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
  const endpoint = process.env.LEVISION_CHAT_API_URL
  const apiKey = process.env.LEVISION_CHAT_API_KEY

  if (!endpoint) {
    return {
      provider: 'stub',
      message: {
        role: 'assistant',
        content: DEFAULT_FALLBACK,
      },
    }
  }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(apiKey ? { Authorization: `Bearer ${apiKey}` } : {}),
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
      content,
    },
  }
}
