<template>
  <section class="page" data-module="schema_govern">
    <header class="page-head">
      <div>
        <h2>字段定义版本</h2>
        <p class="page-desc">
          字段定义按不可变版本登记；换格式试写引用某版本期间不能废弃，取消引用后才可清理。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="definitions-refresh" type="button" @click="loadDefinitions">
          刷新字段定义
        </button>
      </div>
    </header>

    <p v-if="message" class="page-desc" data-testid="definitions-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="definition-table">
      <thead>
        <tr>
          <th>字段定义编号</th>
          <th>主题</th>
          <th>版本</th>
          <th>修订</th>
          <th>状态</th>
          <th>字段</th>
          <th>在途试写</th>
          <th>线上引用</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in definitions" :key="item.definition_id" :data-testid="`definition-${item.definition_id}`">
          <td>{{ item.definition_id }}</td>
          <td>{{ item.topic }}</td>
          <td>{{ item.schema_version }}</td>
          <td>r{{ item.revision }}</td>
          <td>{{ item.status }}</td>
          <td class="definition-fields">{{ describeFields(item) }}</td>
          <td>{{ (item.in_flight_trials ?? []).join('、') || '—' }}</td>
          <td>{{ (item.live_tables ?? []).join('、') || '—' }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`refs-${item.definition_id}`"
              @click="showReferences(item)"
            >
              查看引用
            </button>
            <button
              v-if="item.status !== '已废弃'"
              class="link"
              type="button"
              :data-testid="`deprecate-${item.definition_id}`"
              @click="applyDeprecation(item, false)"
            >
              申请废弃
            </button>
            <button
              v-if="(item.in_flight_trials ?? []).length > 0"
              class="link"
              type="button"
              :data-testid="`cancel-deprecate-${item.definition_id}`"
              @click="applyDeprecation(item, true)"
            >
              取消试写后废弃
            </button>
          </td>
        </tr>
        <tr v-if="!definitions.length">
          <td colspan="9" class="empty-state">暂无字段定义，请先在 Schema 版本登记并发布</td>
        </tr>
      </tbody>
    </table>

    <section v-if="referenceDetail" class="modal-backdrop" data-testid="reference-modal">
      <div class="modal">
        <h3>字段定义 {{ referenceDetail.definition_id }} 引用明细</h3>
        <p class="page-desc">{{ referenceDetail.protection_reason || '无在途引用' }}</p>
        <dl class="dag-detail">
          <div>
            <dt>在途试写</dt>
            <dd data-testid="reference-trials">
              <span v-for="trial in referenceDetail.references.in_flight_trials" :key="trial.trial_id">
                {{ trial.trial_id }}（{{ trial.table }}·{{ trial.status }}·第{{ trial.round }}轮）
              </span>
              <span v-if="!referenceDetail.references.in_flight_trials.length">—</span>
            </dd>
          </div>
          <div>
            <dt>待确认发布</dt>
            <dd>
              <span v-for="release in referenceDetail.references.pending_releases" :key="release.release_id">
                {{ release.release_id }}（{{ release.table }}）
              </span>
              <span v-if="!referenceDetail.references.pending_releases.length">—</span>
            </dd>
          </div>
          <div>
            <dt>线上组合</dt>
            <dd>
              <span v-for="live in referenceDetail.references.live_strategies" :key="live.table">
                {{ live.table }}（{{ live.release_id ?? '历史组合' }}）
              </span>
              <span v-if="!referenceDetail.references.live_strategies.length">—</span>
            </dd>
          </div>
        </dl>
        <div class="page-actions">
          <button class="btn" type="button" @click="referenceDetail = null">关闭</button>
        </div>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { definitionReferences, listDefinitions, requestDeprecation } from '@/api/format-governance'
import type { FieldDefinition } from '@/data/types'

type ReferenceDetail = Awaited<ReturnType<typeof definitionReferences>>

const definitions = ref<FieldDefinition[]>([])
const referenceDetail = ref<ReferenceDetail | null>(null)
const message = ref('')
const errorMessage = ref('')

function describeFields(item: FieldDefinition): string {
  return Object.entries(item.specs)
    .map(([name, spec]) => {
      if (spec.type === 'decimal' || spec.type === 'numeric') {
        return `${name}:decimal(${spec.precision ?? '?'},${spec.scale ?? 0})`
      }
      return `${name}:${spec.type ?? 'string'}`
    })
    .join('，')
}

async function loadDefinitions() {
  errorMessage.value = ''
  try {
    const payload = await listDefinitions()
    definitions.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '字段定义读取失败'
  }
}

async function showReferences(item: FieldDefinition) {
  errorMessage.value = ''
  try {
    referenceDetail.value = await definitionReferences(item.definition_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '引用查询失败'
  }
}

async function applyDeprecation(item: FieldDefinition, cancelTrials: boolean) {
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await requestDeprecation(
      item.definition_id,
      cancelTrials,
      cancelTrials ? '取消在途试写后清理候选定义' : '治理入口申请废弃',
    )
    if (!result.ok) {
      message.value = `仍未废弃：${result.reason ?? '存在引用'}`
      await showReferences(item)
      return
    }
    message.value = result.cancelled_trials?.length
      ? `已取消试写 ${result.cancelled_trials.join('、')}，字段定义 ${item.definition_id} 已废弃`
      : `字段定义 ${item.definition_id} 已废弃`
    referenceDetail.value = null
    await loadDefinitions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '废弃申请失败'
  }
}

onMounted(loadDefinitions)
</script>

<style scoped>
.definition-fields { max-width: 320px; word-break: break-all; }
</style>
