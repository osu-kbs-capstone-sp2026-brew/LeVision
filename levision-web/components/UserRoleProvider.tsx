'use client'

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import type { UserRole } from '@/lib/types'

type UserRoleContextValue = {
  userId: string
  role: UserRole | null
  setRole: (next: UserRole) => void
}

const UserRoleContext = createContext<UserRoleContextValue | null>(null)

export function UserRoleProvider({
  userId,
  initialRole,
  children,
}: {
  userId: string
  initialRole: UserRole | null
  children: ReactNode
}) {
  const [role, setRoleState] = useState<UserRole | null>(initialRole)
  const setRole = useCallback((next: UserRole) => {
    setRoleState(next)
  }, [])

  const value = useMemo(
    () => ({ userId, role, setRole }),
    [userId, role, setRole],
  )

  return (
    <UserRoleContext.Provider value={value}>
      <div className="contents" data-user-role={role ?? undefined}>
        {children}
      </div>
    </UserRoleContext.Provider>
  )
}

export function useUserRole(): UserRoleContextValue {
  const ctx = useContext(UserRoleContext)
  if (!ctx) {
    throw new Error('useUserRole must be used within UserRoleProvider')
  }
  return ctx
}
