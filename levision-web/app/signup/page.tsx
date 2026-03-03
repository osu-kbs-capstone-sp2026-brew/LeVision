'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import Image from 'next/image'
import { createClient } from '@/lib/supabase/client'
import PageShell from '@/components/PageShell'

export default function SignupPage() {
  const router = useRouter()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [emailSent, setEmailSent] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)

    if (password !== confirmPassword) {
      setError("Passwords don't match. Focus.")
      return
    }

    setLoading(true)

    const supabase = createClient()

    const { data, error: authError } = await supabase.auth.signUp({ email, password })

    if (authError) {
      setError('Something broke. Not the hairline though.')
      setLoading(false)
      return
    }

    // If session is immediately available, redirect to onboarding
    if (data.session) {
      router.push('/onboarding')
      return
    }

    // Email confirmation required
    setEmailSent(true)
    setLoading(false)
  }

  if (emailSent) {
    return (
      <PageShell>
        <div className="min-h-screen flex items-center justify-center px-5">
          <div className="flex flex-col items-center w-full max-w-[480px] text-center">
            <div className="flex items-center gap-3 mb-8 animate-fade-up">
              <Image src="/bron-face.png" alt="LeVision" width={48} height={48} className="object-contain" />
              <span className="font-display text-[3.8rem] tracking-[0.06em] leading-none text-offwhite">
                Le<span className="text-brand">Vision</span>
              </span>
            </div>
            <h2 className="font-display text-[2rem] tracking-[0.06em] text-offwhite mb-3 animate-fade-up delay-100">
              Check your email
            </h2>
            <p className="text-[0.85rem] text-muted font-light leading-relaxed animate-fade-up delay-200">
              Confirmation sent to{' '}
              <span className="text-brand">{email}</span>. Click the link to activate your account.
            </p>
          </div>
        </div>
      </PageShell>
    )
  }

  return (
    <PageShell>
      <div className="min-h-screen flex items-center justify-center px-5 py-10">
        <div className="flex flex-col items-center w-full max-w-[480px]">

          {/* Logo */}
          <div className="flex items-center gap-3 mb-1.5 animate-fade-up delay-50">
            <Image src="/bron-face.png" alt="LeVision" width={48} height={48} className="object-contain" />
            <span className="font-display text-[3.8rem] tracking-[0.06em] leading-none text-offwhite">
              Le<span className="text-brand">Vision</span>
            </span>
          </div>

          {/* Tagline */}
          <p className="text-[0.72rem] tracking-[0.22em] uppercase text-muted mb-3.5 animate-fade-up delay-100">
            Basketball Intelligence at your Fingertips
          </p>

          {/* Card */}
          <div className="w-full bg-surface border border-[rgba(200,136,58,0.22)] rounded-sm backdrop-blur-md relative overflow-hidden px-[46px] pt-[44px] pb-[38px] card-top animate-fade-up delay-200">

            <h1 className="font-display text-[1.7rem] tracking-[0.07em] text-offwhite mb-1.5">
              The film room is waiting...
            </h1>
            <p className="text-[0.8rem] text-muted font-light leading-[1.55] mb-[34px]">
              Create your account.
            </p>

            <form onSubmit={handleSubmit}>
              <div className="flex flex-col gap-1.5 mb-3.5">
                <label className="text-[0.68rem] tracking-[0.18em] uppercase text-muted font-medium">
                  Email Address
                </label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="goat@levision.ai"
                  required
                  className="bg-white/[0.04] border border-white/10 focus:border-brand focus:bg-brand/5 rounded-sm px-4 py-3 text-offwhite font-body font-light text-sm outline-none transition-colors duration-200 placeholder:text-white/20"
                />
              </div>

              <div className="flex flex-col gap-1.5 mb-3.5">
                <label className="text-[0.68rem] tracking-[0.18em] uppercase text-muted font-medium">
                  Password
                </label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  required
                  minLength={6}
                  className="bg-white/[0.04] border border-white/10 focus:border-brand focus:bg-brand/5 rounded-sm px-4 py-3 text-offwhite font-body font-light text-sm outline-none transition-colors duration-200 placeholder:text-white/20"
                />
              </div>

              <div className="flex flex-col gap-1.5 mb-3.5">
                <label className="text-[0.68rem] tracking-[0.18em] uppercase text-muted font-medium">
                  Confirm Password
                </label>
                <input
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  placeholder="••••••••"
                  required
                  className={`bg-white/[0.04] border rounded-sm px-4 py-3 text-offwhite font-body font-light text-sm outline-none transition-colors duration-200 placeholder:text-white/20 ${
                    confirmPassword && confirmPassword !== password
                      ? 'border-accent/60 focus:border-accent'
                      : 'border-white/10 focus:border-brand focus:bg-brand/5'
                  }`}
                />
                {confirmPassword && confirmPassword !== password && (
                  <p className="text-accent text-[0.68rem] font-light mt-0.5">Passwords don&apos;t match.</p>
                )}
              </div>

              {error && (
                <p className="text-accent text-[0.78rem] font-light mb-3">{error}</p>
              )}

              <button
                type="submit"
                disabled={loading}
                className="w-full mt-5 py-[15px] bg-brand hover:bg-brand-light disabled:opacity-50 text-pitch font-display text-[1.1rem] tracking-[0.16em] rounded-sm transition-colors duration-200 relative overflow-hidden btn-shine cursor-pointer"
              >
                {loading ? (
                  <span className="flex items-center justify-center gap-2.5">
                    <LoadingDots /> CREATING ACCOUNT
                  </span>
                ) : 'CREATE ACCOUNT'}
              </button>

              <div className="flex justify-center mt-[18px]">
                <Link
                  href="/login"
                  className="text-[0.73rem] text-muted font-light hover:text-brand transition-colors duration-200"
                >
                  Already have an account? Sign in
                </Link>
              </div>
            </form>
          </div>

        </div>
      </div>
    </PageShell>
  )
}

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
