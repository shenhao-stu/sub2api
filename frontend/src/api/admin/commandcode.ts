import { apiClient } from '../client'

export interface CommandCodeWindow {
  cap: number | null
  used: number | null
  limit: number | null
  remaining: number | null
  remainingPercent: number | null
  resetAt: string | null
}

export interface CommandCodeQuota {
  credits: { monthlyCredits: number | null } | null
  windowLimits: {
    fiveHour: CommandCodeWindow | null
    weekly: CommandCodeWindow | null
    limited: boolean | null
  } | null
  fetchedAt: string
}

export async function getCommandCodeQuota(id: number, signal: AbortSignal): Promise<CommandCodeQuota> {
  const { data } = await apiClient.get<CommandCodeQuota>(`/admin/accounts/${id}/commandcode/quota`, { signal, timeout: 25000 })
  return data
}
