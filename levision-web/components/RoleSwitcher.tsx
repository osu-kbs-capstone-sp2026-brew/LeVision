'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase/client'

type Role = 'coach' | 'player' | 'analyst'

type Props = {
  userId: string
  initialRole: Role
}

const ROLE_LABELS: { id: Role; label: string }[] = [
  { id: 'coach',   label: 'Coach' },
  { id: 'player',  label: 'Player' },
  { id: 'analyst', label: 'Analyst' },
]

export default function RoleSwitcher({ userId, initialRole }: Props) {
  const [open, setOpen] = useState(false)
  const [role, setRole] = useState<Role>(initialRole)
  const [saving, setSaving] = useState(false)

  async function handleSelect(nextRole: Role) {
    if (saving || nextRole === role) {
      setOpen(false)
      return
    }

    setSaving(true)
    const supabase = createClient()
    await supabase.from('profiles').update({ role: nextRole }).eq('id', userId)
    setRole(nextRole)
    setSaving(false)
    setOpen(false)
  }

  const currentLabel = ROLE_LABELS.find((r) => r.id === role)?.label ?? role

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="hidden sm:inline-flex items-center gap-1.5 text-[0.68rem] tracking-[0.1em] uppercase text-muted/60 font-body border border-white/[0.06] px-3 py-1.5 rounded-sm bg-transparent hover:text-offwhite hover:border-brand/60 cursor-pointer"
      >
        <span className="w-[4px] h-[4px] bg-brand/60 rounded-full" />
        {saving ? 'Saving…' : currentLabel}
      </button>

      {open && (
        <div className="absolute right-0 mt-2 w-40 border border-white/[0.08] bg-pitch/95 rounded-sm shadow-lg z-20">
          {ROLE_LABELS.map((r) => (
            <button
              key={r.id}
              type="button"
              onClick={() => handleSelect(r.id)}
              className={`w-full text-left px-3 py-2 text-[0.72rem] tracking-[0.08em] uppercase font-body cursor-pointer transition-colors duration-150 ${
                r.id === role
                  ? 'bg-brand/20 text-offwhite'
                  : 'bg-transparent text-muted hover:bg-white/[0.03] hover:text-offwhite'
              }`}
            >
              {r.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

