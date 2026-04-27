import { S3Client, DeleteObjectCommand } from '@aws-sdk/client-s3'
import { NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'

export const runtime = 'nodejs'

function createR2Client() {
  return new S3Client({
    region: 'auto',
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID as string,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY as string,
    },
  })
}

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params

  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })

  const { data: footage, error: fetchError } = await supabase
    .from('footage')
    .select('r2_key')
    .eq('id', id)
    .eq('uploaded_by', user.id)
    .single()

  if (fetchError || !footage) {
    return NextResponse.json({ error: 'Footage not found' }, { status: 404 })
  }

  // Delete from R2
  try {
    await createR2Client().send(
      new DeleteObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: footage.r2_key,
      })
    )
  } catch (err) {
    console.error('R2 delete failed', err)
    // Continue to delete DB record even if R2 delete fails
  }

  const { error: deleteError } = await supabase
    .from('footage')
    .delete()
    .eq('id', id)
    .eq('uploaded_by', user.id)

  if (deleteError) {
    return NextResponse.json({ error: deleteError.message }, { status: 500 })
  }

  return NextResponse.json({ ok: true })
}
