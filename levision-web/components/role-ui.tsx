'use client'

import type { ReactNode } from 'react'
import type { UserRole } from '@/lib/types'
import { useUserRole } from '@/components/UserRoleProvider'

type RoleList = UserRole | readonly UserRole[]

function includesRole(list: RoleList, role: UserRole): boolean {
  return (Array.isArray(list) ? list : [list]).includes(role)
}

/** Renders children only when the active role matches one of `roles`. */
export function RoleGate({ roles, children }: { roles: RoleList; children: ReactNode }) {
  const { role } = useUserRole()
  if (!role || !includesRole(roles, role)) return null
  return <>{children}</>
}

type RoleSwitchProps = {
  coach?: ReactNode
  player?: ReactNode
  fan?: ReactNode
  fallback?: ReactNode
}

/** Picks UI by active role (coach / player / fan). */
export function RoleSwitch({ coach, player, fan, fallback = null }: RoleSwitchProps) {
  const { role } = useUserRole()
  if (role === 'coach') return <>{coach ?? fallback}</>
  if (role === 'player') return <>{player ?? fallback}</>
  if (role === 'fan') return <>{fan ?? fallback}</>
  return <>{fallback}</>
}
