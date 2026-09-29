import { mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'
import CommandCodePreset from '../CommandCodePreset.vue'

vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))

describe('CommandCodePreset', () => {
  it('limits editing to the existing platform and emits the selected protocol', async () => {
    const wrapper = mount(CommandCodePreset, { props: { modelValue: '', platform: 'anthropic' } })
    expect(wrapper.findAll('option').map(option => option.attributes('value'))).toEqual(['', 'provider_anthropic'])
    await wrapper.get('select').setValue('provider_anthropic')
    expect(wrapper.emitted('update:modelValue')).toEqual([['provider_anthropic']])
  })

  it('shows experimental Go status and only offers ZDR for the official API', async () => {
    const wrapper = mount(CommandCodePreset, { props: { modelValue: 'go' } })
    expect(wrapper.get('[data-testid="commandcode-plan-hint"]').text()).toBe('admin.accounts.commandCode.goHint')
    expect(wrapper.find('[data-testid="commandcode-zdr"]').exists()).toBe(false)
    await wrapper.setProps({ modelValue: 'provider_openai' })
    await wrapper.get('[data-testid="commandcode-zdr"]').setValue(true)
    expect(wrapper.emitted('update:zdr')).toEqual([[true]])
  })
})
