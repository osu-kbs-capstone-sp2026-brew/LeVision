import { NextResponse } from 'next/server'
import { DEV_BYPASS_COOKIE, isDevBypassEnabled } from '@/lib/dev-bypass'

export async function GET(request: Request) {
  if (!isDevBypassEnabled()) {
    return NextResponse.json({ error: 'Not found' }, { status: 404 })
  }

  const url = new URL('/dashboard', request.url)
  const response = NextResponse.redirect(url)

  response.cookies.set(DEV_BYPASS_COOKIE, 'admin', {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: 60 * 60 * 8,
  })

  return response
}
