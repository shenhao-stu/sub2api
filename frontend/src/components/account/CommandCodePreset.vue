<template>
  <div class="space-y-3 rounded-xl border border-gray-200 bg-gray-50 p-4 dark:border-dark-600 dark:bg-dark-800" data-testid="commandcode-preset">
    <button
      v-if="availablePresets.some(preset => preset.value === 'go')"
      type="button"
      :aria-pressed="modelValue === 'go'"
      data-testid="commandcode-go-action"
      class="flex w-full items-center justify-between gap-3 rounded-lg border p-3 text-left transition-colors"
      :class="modelValue === 'go' ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20' : 'border-gray-200 bg-white hover:border-primary-400 dark:border-dark-600 dark:bg-dark-700'"
      @click="emit('update:modelValue', 'go')"
    >
      <span>
        <span class="block text-sm font-semibold text-gray-900 dark:text-gray-100">Command Code Go</span>
        <span class="mt-1 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.accounts.commandCode.goSummary') }}</span>
      </span>
      <span class="shrink-0 rounded-full bg-primary-100 px-2 py-1 text-xs text-primary-700 dark:bg-primary-900/40 dark:text-primary-300">{{ t('admin.accounts.commandCode.goPlan') }}</span>
    </button>
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
