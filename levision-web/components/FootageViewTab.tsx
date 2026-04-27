'use client'

import CoachFootageView from '@/components/coach/CoachFootageView'
import PlayerFootageView from '@/components/player/PlayerFootageView'
import { useUserRole } from '@/components/UserRoleProvider'
import type { FootageClip } from '@/lib/footage-library'

type Props = {
  reviewClip?: FootageClip | null
}

/**
 * Thin role router. Renders the appropriate footage view based on the
 * current user role:
 *   player → PlayerFootageView  (possession-segment navigation)
 *   coach / fan / null → CoachFootageView  (full team stats panels)
 */
export default function FootageViewTab({ reviewClip = null }: Props) {
  const { role } = useUserRole()

  if (role === 'player') {
    return <PlayerFootageView reviewClip={reviewClip} />
  }

  return <CoachFootageView reviewClip={reviewClip} />
}
