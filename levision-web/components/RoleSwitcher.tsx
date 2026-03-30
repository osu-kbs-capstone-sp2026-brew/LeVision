'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import type { UserRole } from '@/lib/types'
import { useUserRole } from '@/components/UserRoleProvider'

const ROLE_LABELS: { id: UserRole; label: string }[] = [
  { id: 'coach',   label: 'Coach' },
  { id: 'player',  label: 'Player' },
  { id: 'fan', label: 'Fan' },
]

export default function RoleSwitcher({ disabled = false }: { disabled?: boolean }) {
  const { userId, role, setRole } = useUserRole()
  const [open, setOpen] = useState(false)
  const [saving, setSaving] = useState(false)

  async function handleSelect(nextRole: UserRole) {
    if (disabled) {
      setOpen(false)
      return
    }

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

  const currentLabel = role
    ? (ROLE_LABELS.find((r) => r.id === role)?.label ?? role)
    : 'Set role'

  return (
    <div className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((v) => !v)}
        className="inline-flex items-center gap-1.5 text-[0.68rem] tracking-[0.1em] uppercase text-muted/60 font-body border border-white/[0.06] px-2.5 sm:px-3 py-1.5 rounded-sm bg-transparent hover:text-offwhite hover:border-brand/60 disabled:cursor-not-allowed disabled:opacity-50 cursor-pointer"
      >
        <span className="w-[4px] h-[4px] bg-brand/60 rounded-full" />
        {disabled ? `${currentLabel} (dev)` : saving ? 'Saving…' : currentLabel}
      </button>

      {open && !disabled && (
        <div className="absolute right-0 mt-2 w-40 border border-white/[0.08] bg-pitch/95 rounded-sm shadow-lg z-50">
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
