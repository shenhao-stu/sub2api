<template>
  <section class="space-y-3 rounded-xl border border-gray-200 p-4 dark:border-dark-600" data-testid="commandcode-quota">
    <div class="flex items-center justify-between gap-3">
      <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">{{ t('admin.accounts.commandCode.quotaTitle') }}</h3>
      <button type="button" class="btn btn-secondary btn-sm" :disabled="loading" @click="refresh">
        {{ t(loading ? 'admin.accounts.commandCode.quotaLoading' : 'admin.accounts.commandCode.quotaRefresh') }}
      </button>
    </div>
    <p class="input-hint">{{ t('admin.accounts.commandCode.quotaSavedAccount') }}</p>
    <p v-if="error" role="alert" class="text-sm text-amber-700 dark:text-amber-300">{{ t('admin.accounts.commandCode.quotaUnavailable') }}</p>
    <template v-if="quota">
      <p v-if="quota.windowLimits?.limited" class="text-sm text-amber-700 dark:text-amber-300">{{ t('admin.accounts.commandCode.quotaLimited') }}</p>
      <div class="grid gap-3 sm:grid-cols-3">
        <div class="rounded-lg bg-gray-50 p-3 dark:bg-dark-700">
          <p class="text-xs text-gray-500">{{ t('admin.accounts.commandCode.monthlyCredits') }}</p>
          <p class="mt-2 text-lg font-semibold">{{ number(quota.credits?.monthlyCredits) }}</p>
        </div>
        <div v-for="period in periods" :key="period.key" class="rounded-lg bg-gray-50 p-3 dark:bg-dark-700">
          <p class="text-xs text-gray-500">{{ t(`admin.accounts.commandCode.${period.key}`) }}</p>
          <p class="mt-2 text-lg font-semibold">{{ remaining(period.value) === null ? unknown : `${number(remaining(period.value))}%` }}</p>
          <div v-if="remaining(period.value) !== null" class="mt-2 h-1.5 overflow-hidden rounded bg-gray-200 dark:bg-dark-600" aria-hidden="true">
            <div class="h-full rounded bg-primary-500" :style="{ width: `${remaining(period.value)}%` }" />
          </div>
          <p v-if="period.value?.resetAt" class="mt-2 text-xs text-gray-500">{{ t('admin.accounts.commandCode.quotaReset', { time: date(period.value.resetAt) }) }}</p>
        </div>
      </div>
      <p class="input-hint">{{ t('admin.accounts.commandCode.quotaFetched', { time: date(quota.fetchedAt) }) }}</p>
    </template>
    <p v-else-if="!error" class="input-hint">{{ t('admin.accounts.commandCode.quotaIdle') }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { getCommandCodeQuota, type CommandCodeQuota, type CommandCodeWindow } from '@/api/admin/commandcode'

const props = defineProps<{ accountId: number }>()
const { t, locale } = useI18n()
const quota = ref<CommandCodeQuota | null>(null)
const loading = ref(false)
const error = ref(false)
let pending: AbortController | undefined
const unknown = computed(() => t('admin.accounts.commandCode.quotaUnknown'))
const periods = computed(() => [
  { key: 'fiveHourRemaining', value: quota.value?.windowLimits?.fiveHour },
  { key: 'weeklyRemaining', value: quota.value?.windowLimits?.weekly }
])
function number(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? unknown.value : value.toLocaleString(locale.value, { maximumFractionDigits: 6 })
}
function date(value: string) {
  return new Date(value).toLocaleString(locale.value)
}
function remaining(window: CommandCodeWindow | null | undefined): number | null {
  if (!window) return null
  let percent = window.remainingPercent
  const limit = window.limit ?? window.cap
  if (percent == null && limit != null && limit > 0 && window.used != null) percent = (1 - window.used / limit) * 100
  return percent == null || !Number.isFinite(percent) ? null : Math.max(0, Math.min(100, percent))
}
function clear() {
  pending?.abort()
  pending = undefined
  quota.value = null
  loading.value = false
  error.value = false
}
watch(() => props.accountId, clear)
onBeforeUnmount(clear)
async function refresh() {
  if (loading.value) return
  const request = new AbortController()
  pending = request
  loading.value = true
  error.value = false
  try {
    const result = await getCommandCodeQuota(props.accountId, request.signal)
    if (pending === request) quota.value = result
  } catch {
    if (pending === request) error.value = true
  } finally {
    if (pending === request) {
      pending = undefined
      loading.value = false
    }
  }
}
</script>
