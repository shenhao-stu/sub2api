import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { checkUpdates } from '@/api/admin/system'
import { useAppStore } from '../app'

vi.mock('@/api/admin/system', () => ({ checkUpdates: vi.fn() }))
vi.mock('@/api/auth', () => ({ getPublicSettings: vi.fn() }))

describe('version update capability', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it.each([
    ['custom', false, false],
    ['source', undefined, false],
    ['release', undefined, true],
    ['release', false, false],
    ['release', true, true]
  ] as const)('preserves %s capability %s when caching', async (buildType, capability, expected) => {
    vi.mocked(checkUpdates).mockResolvedValue({
      current_version: '0.2.8+getoken.r248', latest_version: '0.2.8',
      has_update: false, build_type: buildType, online_update_supported: capability, cached: false
    })
    const store = useAppStore()
    await store.fetchVersion(true)
    expect(store.onlineUpdateSupported).toBe(expected)
    const cached = await store.fetchVersion()
    expect(cached?.online_update_supported).toBe(expected)
    expect(checkUpdates).toHaveBeenCalledOnce()
  })
})
