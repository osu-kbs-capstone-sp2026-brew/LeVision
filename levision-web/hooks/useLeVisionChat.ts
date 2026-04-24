'use client'

import { useEffect, useRef, useState } from 'react'
import type { ChatMessage, ChatResponse } from '@/lib/chat/types'

const STORAGE_KEY = 'levision-chat-log-v1'
const MAX_STORED_MESSAGES = 80

const INITIAL_MESSAGE: ChatMessage = {
  role: 'assistant',
  content:
    'Film room is open. Ask about coverages, player trends, or wire this panel to your own model endpoint when you are ready.',
}

function sanitizeStoredMessages(value: unknown): ChatMessage[] {
  if (!Array.isArray(value)) {
    return []
  }

  const parsed = value.filter(
    (message): message is ChatMessage =>
      Boolean(message) &&
      typeof message === 'object' &&
      typeof message.content === 'string' &&
      ['system', 'user', 'assistant'].includes(String(message.role))
  )

  return parsed.length > 0 ? parsed.slice(-MAX_STORED_MESSAGES) : [INITIAL_MESSAGE]
}

export function useLeVisionChat() {
  const [input, setInput] = useState('')
  const [messages, setMessages] = useState<ChatMessage[]>([INITIAL_MESSAGE])
  const [hasLoadedHistory, setHasLoadedHistory] = useState(false)
  const [isSending, setIsSending] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const endRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (typeof window === 'undefined') {
      setHasLoadedHistory(true)
      return
    }

    try {
      const raw = window.localStorage.getItem(STORAGE_KEY)
      if (raw) {
        const stored = JSON.parse(raw)
        setMessages(sanitizeStoredMessages(stored))
      }
    } catch {
      window.localStorage.removeItem(STORAGE_KEY)
      setMessages([INITIAL_MESSAGE])
    } finally {
      setHasLoadedHistory(true)
    }
  }, [])

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  useEffect(() => {
    if (!hasLoadedHistory || typeof window === 'undefined') {
      return
    }

    try {
      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify(messages.slice(-MAX_STORED_MESSAGES))
      )
    } catch {
      return
    }
  }, [hasLoadedHistory, messages])

  async function submitMessage() {
    const trimmed = input.trim()

    if (!trimmed || isSending) {
      return
    }

    const nextMessages: ChatMessage[] = [
      ...messages,
      { role: 'user', content: trimmed },
    ]

    setMessages(nextMessages)
    setInput('')
    setError(null)
    setIsSending(true)

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          messages: nextMessages,
        }),
      })

      if (!response.ok) {
        throw new Error('Request failed')
      }

      const payload = (await response.json()) as ChatResponse
      setMessages((current) => [...current, payload.message])
    } catch {
      setError('The assistant is unavailable right now. Try again in a moment.')
    } finally {
      setIsSending(false)
    }
  }

  return {
    endRef,
    error,
    input,
    isSending,
    messages,
    setInput,
    submitMessage,
  }
}
