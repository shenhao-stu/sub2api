<template>
  <div class="space-y-2" data-testid="commandcode-preset">
    <label class="input-label">
      {{ t('admin.accounts.commandCode.preset') }}
      <select
        :value="modelValue"
        class="input mt-1 font-normal"
        data-testid="commandcode-preset-select"
        @change="emit('update:modelValue', ($event.target as HTMLSelectElement).value as CommandCodePreset)"
      >
        <option value="">{{ t('admin.accounts.commandCode.custom') }}</option>
        <option v-for="preset in availablePresets" :key="preset.value" :value="preset.value">
          {{ t(`admin.accounts.commandCode.${preset.value}`) }}
        </option>
      </select>
    </label>
    <template v-if="modelValue">
      <p class="input-hint" data-testid="commandcode-plan-hint">
        {{ t(modelValue === 'go' ? 'admin.accounts.commandCode.goHint' : 'admin.accounts.commandCode.providerHint') }}
      </p>
      <p class="input-hint">{{ t('admin.accounts.commandCode.keyHint') }}</p>
      <label v-if="modelValue !== 'go'" class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
        <input
          type="checkbox"
          :checked="zdr"
          data-testid="commandcode-zdr"
          @change="emit('update:zdr', ($event.target as HTMLInputElement).checked)"
        />
        {{ t('admin.accounts.commandCode.zdr') }}
      </label>
      <p v-if="modelValue !== 'go' && zdr" class="input-hint">{{ t('admin.accounts.commandCode.zdrHint') }}</p>
    </template>
    <p v-if="requiresNewKey" class="text-sm text-amber-700 dark:text-amber-300">
      {{ t('admin.accounts.commandCode.newKeyRequired') }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { commandCodePresets, type CommandCodePreset } from './commandCodePreset'

const props = defineProps<{
  modelValue: CommandCodePreset
  platform?: string
  zdr?: boolean
  requiresNewKey?: boolean
}>()
const emit = defineEmits<{
  'update:modelValue': [value: CommandCodePreset]
  'update:zdr': [value: boolean]
}>()
const { t } = useI18n()
const availablePresets = computed(() => commandCodePresets.filter(item => !props.platform || item.platform === props.platform))
</script>
