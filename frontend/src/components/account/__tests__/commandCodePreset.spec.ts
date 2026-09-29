import { describe, expect, it } from 'vitest'
import { applyCommandCodePreset, buildCommandCodePreview, resolveCommandCodePreset } from '../commandCodePreset'

describe('Command Code preset invariants', () => {
  it('forces the Go transport while preserving unrelated settings and redacted credentials', () => {
    const credentials = Object.freeze({ base_url: 'https://other.example', pool_mode: true, model_mapping: { public: 'upstream' }, compact_model_mapping: { a: 'b' } })
    const extra = Object.freeze({ provider: 'old', commandcode_zdr: true, openai_passthrough: true, openai_responses_mode: 'force_responses', openai_apikey_responses_websockets_v2_enabled: true, custom: 'preserved' })
    const result = applyCommandCodePreset('go', credentials, extra, true)
    expect(result.credentials).toEqual({ base_url: 'https://api.commandcode.ai', pool_mode: false, model_mapping: credentials.model_mapping, openai_capabilities: ['chat_completions'] })
    expect(result.credentials).not.toHaveProperty('api_key')
    expect(result.extra).toMatchObject({ provider: 'commandcode_go', commandcode_zdr: false, openai_responses_supported: false, openai_responses_mode: 'force_chat_completions', openai_compact_mode: 'force_off', openai_passthrough: false, openai_apikey_responses_websockets_v2_enabled: false, openai_apikey_responses_websockets_v2_mode: 'off', custom: 'preserved' })
    expect(extra.openai_passthrough).toBe(true)
    expect(credentials.pool_mode).toBe(true)
  })

  it('marks official protocols without disabling their native capabilities', () => {
    expect(applyCommandCodePreset('provider_openai', { api_key: 'key' }, undefined, true)).toMatchObject({
      credentials: { api_key: 'key', base_url: 'https://api.commandcode.ai/provider' },
      extra: { provider: 'commandcode', commandcode_zdr: true, openai_responses_supported: true }
    })
    expect(applyCommandCodePreset('provider_anthropic', {}, {}).extra).toMatchObject({ provider: 'commandcode', commandcode_zdr: false, anthropic_apikey_auth_scheme: 'authorization_bearer' })
  })

  it('also disables unsupported pool and WebSocket flags for official Provider accounts', () => {
    for (const preset of ['provider_openai', 'provider_anthropic'] as const) {
      const result = applyCommandCodePreset(preset, { pool_mode: true, pool_mode_retry_count: 2 }, { openai_apikey_responses_websockets_v2_mode: 'ctx_pool', openai_apikey_responses_websockets_v2_enabled: true, openai_passthrough: true, openai_compact_mode: 'auto' })
      expect(result.credentials.pool_mode).toBe(false)
      expect(result.credentials).not.toHaveProperty('pool_mode_retry_count')
      expect(result.extra).toMatchObject({ openai_apikey_responses_websockets_v2_mode: 'off', openai_apikey_responses_websockets_v2_enabled: false, openai_passthrough: true, openai_compact_mode: 'auto' })
    }
  })

  it('previews the public catalog without accepting or forwarding credentials', () => {
    expect(buildCommandCodePreview('go')).toEqual({ platform: 'openai', type: 'apikey', base_url: 'https://api.commandcode.ai', api_key: '', extra: { provider: 'commandcode_go', openai_responses_supported: false } })
    expect(buildCommandCodePreview('provider_anthropic')?.extra.provider).toBe('commandcode')
    expect(buildCommandCodePreview('')).toBeUndefined()
  })

  it('recognizes only the matching platform and removes only owned markers on opting out', () => {
    expect(resolveCommandCodePreset('openai', { provider: 'commandcode_go' })).toBe('go')
    expect(resolveCommandCodePreset('anthropic', { provider: 'commandcode_go' })).toBe('')
    expect(resolveCommandCodePreset('anthropic', { provider: 'commandcode' })).toBe('provider_anthropic')
    expect(applyCommandCodePreset('', {}, { provider: 'commandcode', commandcode_zdr: true, other: 1 }).extra).toEqual({ other: 1 })
    expect(applyCommandCodePreset('', {}, { provider: 'other' }).extra).toEqual({ provider: 'other' })
  })
})
