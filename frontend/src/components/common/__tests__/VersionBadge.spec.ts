import { beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import VersionBadge from '../VersionBadge.vue'

const { app, api } = vi.hoisted(() => ({
  app: {
    versionLoading: false,
    currentVersion: '0.2.8+getoken.r248',
    latestVersion: '0.2.9',
    hasUpdate: true,
    buildType: 'custom',
    onlineUpdateSupported: false,
    releaseInfo: { html_url: 'https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.9' },
    fetchVersion: vi.fn(),
    clearVersionCache: vi.fn()
  },
  api: {
    performUpdate: vi.fn(),
    restartService: vi.fn(),
    getRollbackVersions: vi.fn(),
    rollback: vi.fn()
  }
}))

vi.mock('@/stores', () => ({ useAuthStore: () => ({ isAdmin: true }), useAppStore: () => app }))
vi.mock('@/api/admin/system', () => api)
vi.mock('@/composables/useClipboard', () => ({ useClipboard: () => ({ copied: false, copyToClipboard: vi.fn() }) }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))

describe('VersionBadge custom build workflow', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    app.buildType = 'custom'
    app.onlineUpdateSupported = false
    app.hasUpdate = true
    api.performUpdate.mockResolvedValue({ need_restart: true })
  })

  it.each([true, false])('shows custom deployment guidance with hasUpdate=%s', async (hasUpdate) => {
    app.hasUpdate = hasUpdate
    const wrapper = mount(VersionBadge)
    await wrapper.find('button').trigger('click')
    expect(wrapper.text()).toContain('v0.2.8+getoken.r248')
    expect(wrapper.text(), wrapper.html()).toContain('version.customModeHint')
    expect(wrapper.text()).not.toContain('version.sourceModeHint')
    expect(wrapper.text()).not.toContain('version.updateNow')
    expect(api.performUpdate).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('shows custom rollback guidance without fetching official binaries', async () => {
    app.hasUpdate = false
    const wrapper = mount(VersionBadge)
    await wrapper.find('button').trigger('click')
    const rollback = wrapper.findAll('button').find((button) => button.text() === 'version.rollback')!
    await rollback.trigger('click')
    expect(wrapper.text()).toContain('version.rollbackCustomHint')
    expect(api.getRollbackVersions).not.toHaveBeenCalled()
    expect(api.rollback).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('preserves the official release update button and action', async () => {
    app.buildType = 'release'
    app.onlineUpdateSupported = true
    const wrapper = mount(VersionBadge)
    await wrapper.find('button').trigger('click')
    const update = wrapper.findAll('button').find((button) => button.text() === 'version.updateNow')!
    expect(update).toBeDefined()
    await update.trigger('click')
    expect(api.performUpdate).toHaveBeenCalledOnce()
    expect(wrapper.text()).not.toContain('version.customModeHint')
    wrapper.unmount()
  })

  it('honors the server capability even if the build type says release', async () => {
    app.buildType = 'release'
    const wrapper = mount(VersionBadge)
    await wrapper.find('button').trigger('click')
    expect(wrapper.text()).not.toContain('version.updateNow')
    wrapper.unmount()
  })
})
