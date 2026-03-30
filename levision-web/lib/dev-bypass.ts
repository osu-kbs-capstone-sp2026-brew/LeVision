import { cookies } from 'next/headers'
import type { Profile } from '@/lib/types'

export const DEV_BYPASS_COOKIE = 'levision-dev-bypass'

export function isDevBypassEnabled() {
  return (
    process.env.NODE_ENV !== 'production' &&
    process.env.LEVISION_ENABLE_DEV_BYPASS === 'true'
  )
}

export async function hasDevBypassSession() {
  if (!isDevBypassEnabled()) {
    return false
  }

  const cookieStore = await cookies()
  return cookieStore.get(DEV_BYPASS_COOKIE)?.value === 'admin'
}

export function getDevBypassProfile(): Profile {
  return {
    id: 'dev-admin',
    email: 'admin@local.levision',
    role: 'coach',
    onboarding_complete: true,
    onboarding_step: 2,
    created_at: new Date(0).toISOString(),
  }
}
