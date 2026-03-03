import PageShell from '@/components/PageShell'

export default function OnboardingLoading() {
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
