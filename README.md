# LeVision

LeVision is a real-time sports analytics system that detects players from video, tracks their movement, identifies jersey numbers, and overlays live player statistics directly onto game footage.

Our goal is to make sports broadcasts more interactive by automatically linking visual player data with real-time performance insights.

## Current Focus

- Offline video processing (recorded games)
- Player detection and multi-object tracking
- Jersey number recognition (OCR)
- Player stat retrieval and overlay rendering

---

## Local Dev Setup

### Web App (`levision-web/`)

The web app is a Next.js project connected to Supabase for auth and data.

#### 1. Install dependencies

```bash
cd levision-web
npm install
```

#### 2. Set up environment variables

Create `levision-web/.env.local`:

```env
NEXT_PUBLIC_SUPABASE_URL=https://your-project-ref.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
```

Get these from your Supabase project → Settings → API.

#### 3. Run the dev server

```bash
npm run dev
```

App runs at `http://localhost:3000`.

---

### Python / Backend

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
| --- | --- |
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (never expose publicly) |
| `ESPN_SUMMARY_URL` | ESPN game summary API endpoint |
| `SCHEMA_MODE` | `snake` for snake_case column mapping |
| `DRY_RUN` | `true` to skip writes during testing |
