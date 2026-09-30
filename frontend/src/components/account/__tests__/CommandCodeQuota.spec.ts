import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import CommandCodeQuota from '../CommandCodeQuota.vue'
import { getCommandCodeQuota, type CommandCodeQuota as Quota } from '@/api/admin/commandcode'

vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key, locale: ref('en') }) }))
vi.mock('@/api/admin/commandcode', () => ({ getCommandCodeQuota: vi.fn() }))
const quota: Quota = { credits: { monthlyCredits: 0 }, windowLimits: null, fetchedAt: '2026-09-30T00:00:00Z' }

describe('CommandCodeQuota', () => {
  beforeEach(() => vi.clearAllMocks())

  it('queries only on demand and keeps zero distinct from an unknown window', async () => {
    vi.mocked(getCommandCodeQuota).mockResolvedValue(quota)
    const wrapper = mount(CommandCodeQuota, { props: { accountId: 12 } })
    expect(getCommandCodeQuota).not.toHaveBeenCalled()
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect(getCommandCodeQuota).toHaveBeenCalledWith(12, expect.any(AbortSignal))
    expect(wrapper.findAll('.text-lg').map(node => node.text())).toEqual(['0', 'admin.accounts.commandCode.quotaUnknown', 'admin.accounts.commandCode.quotaUnknown'])
    wrapper.unmount()
  })

  it('aborts and discards an old account response after switching accounts', async () => {
    let resolve!: (value: Quota) => void
    vi.mocked(getCommandCodeQuota).mockImplementation(() => new Promise(done => { resolve = done }))
    const wrapper = mount(CommandCodeQuota, { props: { accountId: 12 } })
    await wrapper.get('button').trigger('click')
    const signal = vi.mocked(getCommandCodeQuota).mock.calls[0][1]
    await wrapper.setProps({ accountId: 13 })
    expect(signal.aborted).toBe(true)
    resolve(quota)
    await flushPromises()
    expect(wrapper.text()).toContain('admin.accounts.commandCode.quotaIdle')
    expect(wrapper.findAll('.text-lg')).toHaveLength(0)
    wrapper.unmount()
  })
})
