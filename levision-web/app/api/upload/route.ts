import { randomUUID } from 'node:crypto'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import { NextResponse } from 'next/server'

export const runtime = 'nodejs'

const MAX_UPLOAD_SIZE_BYTES = 300 * 1024 * 1024
const FILE_TOO_LARGE_MESSAGE = 'File too big (max 300MB).'

const REQUIRED_ENV_VARS = [
  'R2_ACCOUNT_ID',
  'R2_ACCESS_KEY_ID',
  'R2_SECRET_ACCESS_KEY',
  'R2_BUCKET',
] as const

function getMissingEnvVars() {
  return REQUIRED_ENV_VARS.filter((key) => !process.env[key])
}

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

function toUploadKey(filename: string) {
  const extension = filename.split('.').pop()?.toLowerCase() ?? 'mp4'
  const baseName = filename.replace(/\.[^.]+$/, '')
  const safeBaseName = baseName
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-zA-Z0-9-_]/g, '')
    .slice(0, 80) || 'video'

  return `uploads/${Date.now()}-${randomUUID()}__${safeBaseName}.${extension}`
}

function getContentLength(request: Request) {
  const header = request.headers.get('content-length')
  if (!header) return null

  const parsed = Number.parseInt(header, 10)
  return Number.isFinite(parsed) ? parsed : null
}

function isFormDataParseFailure(error: unknown) {
  return error instanceof TypeError && /Failed to parse body as FormData/i.test(error.message)
}

export async function POST(request: Request) {
  try {
    const missingEnv = getMissingEnvVars()
    if (missingEnv.length > 0) {
      return NextResponse.json(
        { error: `Missing required environment variables: ${missingEnv.join(', ')}` },
        { status: 500 }
      )
    }

    let formData: FormData

    try {
      formData = await request.formData()
    } catch (error) {
      const contentLength = getContentLength(request)

      if (
        isFormDataParseFailure(error) ||
        (contentLength !== null && contentLength > MAX_UPLOAD_SIZE_BYTES)
      ) {
        return NextResponse.json({ error: FILE_TOO_LARGE_MESSAGE }, { status: 413 })
      }

      throw error
    }

    const fileEntry = formData.get('file')

    if (!(fileEntry instanceof File)) {
      return NextResponse.json({ error: 'A file field is required.' }, { status: 400 })
    }

    if (fileEntry.size > MAX_UPLOAD_SIZE_BYTES) {
      return NextResponse.json({ error: FILE_TOO_LARGE_MESSAGE }, { status: 413 })
    }

    if (!fileEntry.type.startsWith('video/')) {
      return NextResponse.json({ error: 'Only video files are allowed.' }, { status: 400 })
    }

    const key = toUploadKey(fileEntry.name)
    const body = Buffer.from(await fileEntry.arrayBuffer())

    await createR2Client().send(
      new PutObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: key,
        Body: body,
        ContentType: fileEntry.type || 'application/octet-stream',
      })
    )

    const publicBaseUrl = process.env.R2_PUBLIC_BASE_URL?.replace(/\/$/, '')
    const url = publicBaseUrl ? `${publicBaseUrl}/${key}` : null

    return NextResponse.json({ key, url })
  } catch (error) {
    console.error('Upload route failed', error)
    return NextResponse.json({ error: 'Unable to upload file right now.' }, { status: 500 })
  }
}
