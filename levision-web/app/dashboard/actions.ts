'use server'

import { cookies } from 'next/headers'
import { DEV_BYPASS_COOKIE } from '@/lib/dev-bypass'
import { createClient } from '@/lib/supabase/server'
import { redirect } from 'next/navigation'

export async function signOut() {
  const cookieStore = await cookies()
  cookieStore.delete(DEV_BYPASS_COOKIE)

  const supabase = await createClient()
  await supabase.auth.signOut()
  redirect('/login')
}
