import { NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'

export const runtime = 'nodejs'

export async function PATCH(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params

  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const body = (await request.json()) as { espn_game_id: string; game_label: string }

  if (!body.espn_game_id || !body.game_label) {
    return NextResponse.json({ error: 'espn_game_id and game_label are required' }, { status: 400 })
  }

  const { error } = await supabase
    .from('footage')
    .update({ espn_game_id: body.espn_game_id, game_label: body.game_label })
    .eq('id', id)
    .eq('uploaded_by', user.id)

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }

  return NextResponse.json({ ok: true })
}
