'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import PageShell from '@/components/PageShell'

type Role = 'coach' | 'player' | 'analyst'

const ROLES: { id: Role; name: string; desc: string; remark: string }[] = [
  {
    id: 'coach',
    name: 'Coach',
    desc: 'Schemes, scouting, and game planning.',
    remark: 'Even the king had Spoelstra.',
  },
  {
    id: 'player',
    name: 'Player',
    desc: 'Your performance, broken down frame by frame.',
    remark: 'LeBron watches more film than you.',
  },
  {
    id: 'analyst',
    name: 'Analyst',
    desc: 'The numbers behind every possession.',
    remark: 'Moneyball was just the beginning.',
  },
]

export default function OnboardingPage() {
  const router = useRouter()
  const [isLoading, setIsLoading]       = useState(true)
  const [transitioning, setTransitioning] = useState(false)
  const [step, setStep]                 = useState(0)
  const [userId, setUserId]             = useState<string | null>(null)
  const [selectedRole, setSelectedRole] = useState<Role | null>(null)
  const [completing, setCompleting]     = useState(false)

  // Load user + restore step and role from DB
  useEffect(() => {
    async function loadUser() {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()

      if (!user) {
        router.push('/login')
        return
      }

      setUserId(user.id)

      const { data: profile } = await supabase
        .from('profiles')
        .select('role, onboarding_step')
        .eq('id', user.id)
        .single()

      if (profile) {
        if (profile.onboarding_step) setStep(profile.onboarding_step)
        if (profile.role) setSelectedRole(profile.role as Role)
      }

      setIsLoading(false)
    }
    loadUser()
  }, [router])

  async function advanceStep(nextStep: number) {
    if (!userId || transitioning) return
    setTransitioning(true)
    const supabase = createClient()
    await supabase.from('profiles').update({ onboarding_step: nextStep }).eq('id', userId)
    // Brief pause so the transition overlay is visible
    await new Promise((r) => setTimeout(r, 350))
    setStep(nextStep)
    setTransitioning(false)
  }

  async function handleRoleSelect(role: Role) {
    if (!userId) return
    setSelectedRole(role)
    const supabase = createClient()
    await supabase.from('profiles').update({ role }).eq('id', userId)
  }

  async function handleComplete() {
    if (!userId || completing) return
    setCompleting(true)
    const supabase = createClient()
    await supabase
      .from('profiles')
      .update({ onboarding_complete: true, onboarding_step: 2 })
      .eq('id', userId)
    router.push('/dashboard')
  }

  async function handleSkipUpload() {
    if (!userId || completing) return
    setCompleting(true)
    const supabase = createClient()
    await supabase
      .from('profiles')
      .update({ onboarding_complete: true, onboarding_step: 2 })
      .eq('id', userId)
    router.push('/dashboard')
  }

  // ── Initial page load ─────────────────────────────────────────────────────
  if (isLoading) {
    return (
      <PageShell>
        <div className="min-h-screen flex items-center justify-center">
          <div className="flex gap-1.5 animate-fade-up">
            {[0, 1, 2].map((i) => (
              <div
                key={i}
                className="w-1.5 h-1.5 rounded-full bg-brand/60"
                style={{ animation: `pulse 1.2s ease-in-out ${i * 0.2}s infinite` }}
              />
            ))}
          </div>
        </div>
      </PageShell>
    )
  }

  return (
    <PageShell>
      <div className="min-h-screen flex items-center justify-center px-5 py-12 relative">

        {/* Step-transition overlay */}
        <div
          className={`absolute inset-0 z-20 flex items-center justify-center bg-pitch/70 backdrop-blur-[2px] transition-opacity duration-300 pointer-events-none ${
            transitioning ? 'opacity-100' : 'opacity-0'
          }`}
        >
          <div className="flex gap-2">
            {[0, 1, 2].map((i) => (
              <div
                key={i}
                className="w-2 h-2 rounded-full bg-brand"
                style={{ animation: `pulse 1s ease-in-out ${i * 0.18}s infinite` }}
              />
            ))}
          </div>
        </div>

        <div className="w-full max-w-[680px]">

          {/* Progress bar */}
          <div className="flex gap-2 mb-14">
            {[0, 1, 2].map((i) => (
              <div
                key={i}
                className={`flex-1 h-0.5 rounded-sm relative overflow-hidden transition-colors duration-400 ${
                  i === step ? 'bg-brand prog-shimmer' : i < step ? 'bg-brand/40' : 'bg-[rgba(200,136,58,0.18)]'
                }`}
              />
            ))}
          </div>

          {/* Steps — key forces re-mount on step change so fadeUp replays */}
          <div key={step} className="animate-fade-up">

            {/* ── Step 0: Welcome ── */}
            {step === 0 && (
              <div className="flex flex-col">
                <span className="text-brand uppercase text-[0.68rem] tracking-[0.22em] font-medium font-body mb-3.5">
                  Welcome to LeVision
                </span>
                <h1 className="font-display text-offwhite leading-[0.92] tracking-[0.04em] text-[clamp(2.8rem,6.5vw,4.8rem)] mb-5">
                  See the game like <em>The King.</em>
                </h1>
                <p className="text-[0.92rem] font-light text-offwhite/60 leading-[1.8] max-w-[460px] mb-10">
                  You now have access to the same obsessive film breakdown LeBron&apos;s been doing since he was 16.
                </p>
                <button
                  onClick={() => advanceStep(1)}
                  disabled={transitioning}
                  className="self-start px-9 py-3.5 bg-brand hover:bg-brand-light disabled:opacity-60 text-pitch font-display tracking-widest rounded-sm transition-colors duration-200 relative overflow-hidden btn-shine cursor-pointer"
                >
                  {transitioning ? (
                    <span className="flex items-center gap-2.5">
                      <LoadingDots /> LOADING
                    </span>
                  ) : "LET'S GET STARTED"}
                </button>
              </div>
            )}

            {/* ── Step 1: Role Selection ── */}
            {step === 1 && (
              <div className="flex flex-col">
                <span className="text-brand uppercase text-[0.68rem] tracking-[0.22em] font-medium font-body mb-3.5">
                  Step 01 — Your Role
                </span>
                <h2 className="font-display text-offwhite leading-[0.92] tracking-[0.04em] text-[clamp(2.4rem,5vw,3.8rem)] mb-4">
                  Who are you in <em>the film room?</em>
                </h2>
                <p className="text-[0.92rem] font-light text-offwhite/60 leading-[1.8] max-w-[460px] mb-8">
                  Your role shapes the experience. Pick the one that fits.
                </p>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-11">
                  {ROLES.map((r, i) => (
                    <button
                      key={r.id}
                      onClick={() => handleRoleSelect(r.id)}
                      className={`text-left p-5 border rounded-sm transition-colors duration-200 cursor-pointer animate-fade-up ${
                        selectedRole === r.id
                          ? 'border-brand bg-[rgba(200,136,58,0.07)]'
                          : 'border-white/[0.07] bg-white/[0.02] hover:border-brand hover:bg-[rgba(200,136,58,0.07)]'
                      }`}
                      style={{ animationDelay: `${0.06 * (i + 1)}s` }}
                    >
                      <div className={`font-display text-[1rem] tracking-[0.07em] mb-1.5 transition-colors duration-200 ${
                        selectedRole === r.id ? 'text-brand' : 'text-offwhite'
                      }`}>
                        {r.name}
                      </div>
                      <div className="text-[0.71rem] text-muted font-light leading-[1.45] mb-2">
                        {r.desc}
                      </div>
                      <div className={`text-[0.65rem] font-light leading-[1.4] italic transition-colors duration-200 ${
                        selectedRole === r.id ? 'text-brand/70' : 'text-white/25'
                      }`}>
                        {r.remark}
                      </div>
                    </button>
                  ))}
                </div>

                <button
                  onClick={() => advanceStep(2)}
                  disabled={!selectedRole || transitioning}
                  className="self-start px-9 py-3.5 bg-brand hover:bg-brand-light disabled:opacity-30 disabled:cursor-not-allowed text-pitch font-display tracking-widest rounded-sm transition-colors duration-200 relative overflow-hidden btn-shine cursor-pointer"
                >
                  {transitioning ? (
                    <span className="flex items-center gap-2.5">
                      <LoadingDots /> SAVING
                    </span>
                  ) : 'CONTINUE'}
                </button>
              </div>
            )}

            {/* ── Step 2: Upload ── */}
            {step === 2 && (
              <div className="flex flex-col">
                <span className="text-brand uppercase text-[0.68rem] tracking-[0.22em] font-medium font-body mb-3.5">
                  Step 02 — First Film
                </span>
                <h2 className="font-display text-offwhite leading-[0.92] tracking-[0.04em] text-[clamp(2.4rem,5vw,3.8rem)] mb-4">
                  Drop in your first <em>game film.</em>
                </h2>
                <p className="text-[0.92rem] font-light text-offwhite/60 leading-[1.8] max-w-[460px] mb-8">
                  Drop your footage. We&apos;ll do what LeBron does at midnight.
                </p>

                <div className="border border-dashed border-[rgba(200,136,58,0.3)] rounded-sm p-10 text-center bg-[rgba(200,136,58,0.025)] hover:border-brand hover:bg-[rgba(200,136,58,0.06)] transition-colors duration-200 mb-8 cursor-default">
                  <div className="font-display text-[1.25rem] tracking-[0.1em] text-offwhite mb-1.5">
                    Drop footage here or click to browse
                  </div>
                  <div className="text-[0.74rem] text-muted font-light mb-3.5">
                    Game film, practice sessions, highlight cuts
                  </div>
                  <div className="flex gap-2 justify-center">
                    {['MP4', 'MOV', 'AVI', 'MKV'].map((fmt) => (
                      <span key={fmt} className="text-[0.62rem] tracking-[0.12em] px-2.5 py-1 border border-[rgba(200,136,58,0.18)] rounded-sm text-muted">
                        {fmt}
                      </span>
                    ))}
                  </div>
                </div>

                <div className="flex items-center gap-4">
                  <button
                    onClick={handleComplete}
                    disabled={completing}
                    className="px-9 py-3.5 bg-brand hover:bg-brand-light disabled:opacity-50 text-pitch font-display tracking-widest rounded-sm transition-colors duration-200 relative overflow-hidden btn-shine cursor-pointer"
                  >
                    {completing ? (
                      <span className="flex items-center gap-2.5">
                        <LoadingDots /> ENTERING
                      </span>
                    ) : 'ENTER THE ARENA'}
                  </button>
                  <button
                    onClick={handleSkipUpload}
                    disabled={completing}
                    className="text-muted hover:text-offwhite font-body text-[0.78rem] tracking-[0.06em] bg-transparent border-none transition-colors duration-200 cursor-pointer disabled:opacity-50"
                  >
                    Skip. Bold choice. LeBron never skips film.
                  </button>
                </div>
              </div>
            )}

          </div>
        </div>
      </div>
    </PageShell>
  )
}

// Inline loading dots for buttons
function LoadingDots() {
  return (
    <span className="flex items-center gap-[3px]">
      {[0, 1, 2].map((i) => (
        <span
          key={i}
          className="w-[3px] h-[3px] rounded-full bg-current"
          style={{ animation: `pulse 1s ease-in-out ${i * 0.15}s infinite` }}
        />
      ))}
    </span>
  )
}
