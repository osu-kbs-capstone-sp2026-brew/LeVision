import PageShell from '@/components/PageShell'

export default function DashboardLoading() {
  return (
    <PageShell>
      <div className="min-h-screen flex flex-col">

        {/* Navbar skeleton */}
        <nav className="flex items-center justify-between px-8 py-5 border-b border-[rgba(200,136,58,0.15)] bg-pitch/80 backdrop-blur-sm">
          {/* Logo */}
          <div className="flex items-center gap-2.5">
            <div className="w-[26px] h-[26px] rounded-full bg-white/[0.04] animate-pulse" />
            <div className="w-28 h-7 rounded-sm bg-white/[0.04] animate-pulse" />
          </div>
          {/* Right side */}
          <div className="flex items-center gap-4">
            <div className="w-16 h-5 rounded-sm bg-white/[0.04] animate-pulse hidden sm:block" />
            <div className="w-24 h-5 rounded-sm bg-white/[0.04] animate-pulse hidden sm:block" />
            <div className="w-14 h-5 rounded-sm bg-white/[0.04] animate-pulse" />
          </div>
        </nav>

        {/* Tab bar skeleton */}
        <div className="px-8 pt-10 max-w-[1100px] w-full mx-auto">
          <div className="flex gap-1 border-b border-[rgba(200,136,58,0.15)] mb-10 pb-3.5">
            {[80, 64, 72].map((w, i) => (
              <div key={i} style={{ width: w }} className="h-4 rounded-sm bg-white/[0.04] animate-pulse mr-4" />
            ))}
          </div>

          {/* Content skeleton */}
          <div className="flex flex-col gap-4">
            <div className="w-48 h-8 rounded-sm bg-white/[0.04] animate-pulse" />
            <div className="w-72 h-4 rounded-sm bg-white/[0.03] animate-pulse" />
            <div className="w-full h-48 rounded-sm bg-white/[0.03] animate-pulse mt-4 border border-[rgba(200,136,58,0.07)]" />
          </div>
        </div>

      </div>
    </PageShell>
  )
}
