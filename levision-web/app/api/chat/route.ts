import { NextResponse } from 'next/server'
import { generateChatReply } from '@/lib/chat/provider'
import type { ChatRequest } from '@/lib/chat/types'

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as Partial<ChatRequest>
    const messages = Array.isArray(body.messages)
      ? body.messages.filter(
          (message) =>
            message &&
            typeof message.content === 'string' &&
            ['system', 'user', 'assistant'].includes(message.role)
        )
      : []

    if (messages.length === 0) {
      return NextResponse.json(
        { error: 'At least one valid chat message is required.' },
        { status: 400 }
      )
    }

    const reply = await generateChatReply(messages)

    return NextResponse.json(reply)
  } catch (error) {
    console.error('Chat route failed', error)

    return NextResponse.json(
      { error: 'Unable to generate a chat response right now.' },
      { status: 500 }
    )
  }
}
