export type CommandCodePreset = '' | 'provider_openai' | 'provider_anthropic' | 'go'

export const commandCodePresets = [
	{ value: 'go', platform: 'openai', provider: 'commandcode_go', baseUrl: 'https://api.commandcode.ai' },
  { value: 'provider_openai', platform: 'openai', provider: 'commandcode', baseUrl: 'https://api.commandcode.ai/provider' },
  { value: 'provider_anthropic', platform: 'anthropic', provider: 'commandcode', baseUrl: 'https://api.commandcode.ai/provider' }
] as const

export function resolveCommandCodePreset(platform: string, extra?: Record<string, unknown> | null): CommandCodePreset {
  return commandCodePresets.find(item => item.platform === platform && item.provider === extra?.provider)?.value ?? ''
}

export function buildCommandCodePreview(preset: CommandCodePreset) {
  const selected = commandCodePresets.find(item => item.value === preset)
  if (!selected) return undefined
  return {
    platform: selected.platform,
    type: 'apikey',
    base_url: selected.baseUrl,
    api_key: '',
    extra: { provider: selected.provider, openai_responses_supported: preset !== 'go' }
  }
}

export function applyCommandCodePreset(
  preset: CommandCodePreset,
  credentials: Record<string, unknown>,
  extra?: Record<string, unknown> | null,
  zdr = false
): { credentials: Record<string, unknown>; extra: Record<string, unknown> } {
  const nextCredentials = { ...credentials }
  const nextExtra = { ...extra }
  const selected = commandCodePresets.find(item => item.value === preset)
  if (!selected) {
    if (nextExtra.provider === 'commandcode' || nextExtra.provider === 'commandcode_go') {
      delete nextExtra.provider
      delete nextExtra.commandcode_zdr
    }
    return { credentials: nextCredentials, extra: nextExtra }
  }

  nextCredentials.base_url = selected.baseUrl
  nextExtra.provider = selected.provider
  nextExtra.commandcode_zdr = preset !== 'go' && zdr
  if (selected.platform === 'anthropic') {
    nextExtra.anthropic_apikey_auth_scheme = 'authorization_bearer'
  } else {
    nextExtra.openai_responses_supported = preset !== 'go'
  }
  nextCredentials.pool_mode = false
  delete nextCredentials.pool_mode_retry_count
  delete nextCredentials.pool_mode_retry_status_codes
  for (const type of ['apikey', 'oauth']) {
    nextExtra[`openai_${type}_responses_websockets_v2_mode`] = 'off'
    nextExtra[`openai_${type}_responses_websockets_v2_enabled`] = false
  }
  nextExtra.responses_websockets_v2_enabled = false
  nextExtra.openai_ws_enabled = false
  if (preset === 'go') {
    nextCredentials.openai_capabilities = ['chat_completions']
    delete nextCredentials.compact_model_mapping
    nextExtra.openai_responses_mode = 'force_chat_completions'
    nextExtra.openai_compact_mode = 'force_off'
    nextExtra.openai_passthrough = false
    nextExtra.openai_oauth_passthrough = false
    nextExtra.anthropic_passthrough = false
  }
  return { credentials: nextCredentials, extra: nextExtra }
}
