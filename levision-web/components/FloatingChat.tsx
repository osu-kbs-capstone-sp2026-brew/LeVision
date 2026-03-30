'use client'

import { useEffect, useRef, useState } from 'react'
import type { ChatMessage, ChatResponse } from '@/lib/chat/types'

const INITIAL_MESSAGE: ChatMessage = {
  role: 'assistant',
  content:
    'Film room is open. Ask about coverages, player trends, or wire this panel to your own model endpoint when you are ready.',
}

export default function FloatingChat() {
  const [isOpen, setIsOpen] = useState(false)
  const [input, setInput] = useState('')
  const [messages, setMessages] = useState<ChatMessage[]>([INITIAL_MESSAGE])
  const [isSending, setIsSending] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const endRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isOpen])

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

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    await submitMessage()
  }

  function handleComposerKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()

      void submitMessage()
    }
  }

  return (
    <div className="pointer-events-none fixed inset-y-0 right-0 z-50 flex items-center">
      <button
        type="button"
        onClick={() => setIsOpen((current) => !current)}
        aria-expanded={isOpen}
        aria-controls="levision-chat-panel"
        className={`pointer-events-auto group absolute right-0 top-1/2 flex -translate-y-1/2 translate-x-[1px] items-center gap-3 rounded-l-[22px] border border-r-0 border-[rgba(200,136,58,0.32)] bg-[linear-gradient(180deg,rgba(19,22,27,0.96),rgba(9,11,14,0.96))] px-4 py-5 text-offwhite shadow-[0_18px_50px_rgba(0,0,0,0.35)] backdrop-blur-md transition-[right,background-color] duration-300 cursor-pointer ${
          isOpen ? 'right-[min(24rem,calc(100vw-2rem))]' : 'right-0'
        }`}
      >
        <span className="absolute inset-y-3 left-0 w-px bg-[linear-gradient(180deg,transparent,rgba(200,136,58,0.95),transparent)]" />
        <span className="absolute inset-x-3 top-0 h-px bg-[linear-gradient(90deg,transparent,rgba(200,136,58,0.5),transparent)]" />
        <span className="flex flex-col items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-brand shadow-[0_0_14px_rgba(200,136,58,0.9)]" />
          <span className="[writing-mode:vertical-rl] rotate-180 font-display text-[0.82rem] tracking-[0.22em]">
            {isOpen ? 'CLOSE CHAT' : 'OPEN CHAT'}
          </span>
        </span>
      </button>

      <section
        id="levision-chat-panel"
        aria-hidden={!isOpen}
        className={`relative h-full w-[min(24rem,calc(100vw-2rem))] overflow-hidden border-l border-[rgba(200,136,58,0.24)] bg-[rgba(9,11,14,0.94)] shadow-[-24px_0_90px_rgba(0,0,0,0.45)] backdrop-blur-xl transition-transform duration-300 ${
          isOpen ? 'translate-x-0 pointer-events-auto' : 'translate-x-full pointer-events-none'
        }`}
      >
        <div className="pointer-events-none absolute inset-0 opacity-90">
          <div className="absolute inset-y-0 left-0 w-px bg-[linear-gradient(180deg,transparent,rgba(200,136,58,0.95),transparent)]" />
          <div className="absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent,rgba(200,136,58,0.95),transparent)]" />
          <div className="absolute -left-10 top-10 h-28 w-28 rounded-full bg-brand/15 blur-3xl" />
          <div className="absolute -right-16 bottom-10 h-32 w-32 rounded-full bg-brand/10 blur-3xl" />
        </div>

        <div className="relative flex h-full flex-col">
          <div className="relative border-b border-white/8 px-5 py-4">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="font-display text-[1.1rem] tracking-[0.12em] text-offwhite">
                  LEVISION AI
                </p>
                <p className="mt-1 text-[0.68rem] uppercase tracking-[0.22em] text-muted">
                  Custom model ready
                </p>
              </div>
              <button
                type="button"
                onClick={() => setIsOpen(false)}
                aria-label="Close chatbot"
                className="text-muted transition-colors duration-200 hover:text-offwhite"
              >
                X
              </button>
            </div>
          </div>

          <div className="relative min-h-0 flex-1 overflow-y-auto px-4 py-4 chat-scroll">
            <div className="flex flex-col gap-3">
              {messages.map((message, index) => {
                const isUser = message.role === 'user'

                return (
                  <div
                    key={`${message.role}-${index}-${message.content.slice(0, 16)}`}
                    className={`max-w-[86%] rounded-2xl px-4 py-3 text-[0.82rem] leading-6 shadow-[0_10px_35px_rgba(0,0,0,0.2)] ${
                      isUser
                        ? 'ml-auto rounded-br-sm bg-brand text-pitch'
                        : 'rounded-bl-sm border border-white/8 bg-white/[0.04] text-offwhite'
                    }`}
                  >
                    {message.content}
                  </div>
                )
              })}

              {isSending && (
                <div className="max-w-[86%] rounded-2xl rounded-bl-sm border border-white/8 bg-white/[0.04] px-4 py-3 text-[0.82rem] text-offwhite">
                  <span className="flex items-center gap-1.5 text-muted">
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-brand" />
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-brand [animation-delay:120ms]" />
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-brand [animation-delay:240ms]" />
                  </span>
                </div>
              )}

              <div ref={endRef} />
            </div>
          </div>

          <form onSubmit={handleSubmit} className="relative border-t border-white/8 px-4 py-4">
            <label htmlFor="chat-input" className="sr-only">
              Message LeVision AI
            </label>
            <textarea
              id="chat-input"
              rows={3}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleComposerKeyDown}
              placeholder="Ask LeVision about your game, scouting, or workflow..."
              className="w-full resize-none rounded-[18px] border border-white/10 bg-white/[0.04] px-4 py-3 pr-14 text-sm text-offwhite outline-none transition-colors duration-200 placeholder:text-white/25 focus:border-brand focus:bg-brand/5"
            />

            <button
              type="submit"
              disabled={isSending || input.trim().length === 0}
              aria-label="Send message"
              className="absolute bottom-7 right-7 flex h-10 w-10 items-center justify-center rounded-full bg-brand text-pitch transition-colors duration-200 hover:bg-brand-light disabled:cursor-not-allowed disabled:opacity-40"
            >
              GO
            </button>

            <div className="mt-3 flex items-center justify-between gap-3">
              <p className="text-[0.65rem] uppercase tracking-[0.18em] text-muted">
                Endpoint: {process.env.NEXT_PUBLIC_LEVISION_CHAT_LABEL ?? 'scaffold'}
              </p>
              {error && <p className="text-[0.72rem] text-accent">{error}</p>}
            </div>
          </form>
        </div>
      </section>
    </div>
  )
}
