export type Profile = {
  id: string
  email: string | null
  role: 'coach' | 'player' | 'analyst' | null
  onboarding_complete: boolean
  onboarding_step: number
  created_at: string
}
