<template>
  <section class="page" data-module="table_format">
    <header class="page-head">
      <div>
        <h2>换格式试写</h2>
        <p class="page-desc">
          新格式启用前先试写：草稿、试写中、待确认、已生效逐级留痕。每次写入尝试都记录表名、
          候选格式和字段定义编号，通过与因类型/缺列被拒的行分别保留。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/table_format">返回表格式配置</RouterLink>
        <RouterLink class="btn ghost" to="/field-definitions">字段定义版本</RouterLink>
      </div>
    </header>

    <p v-if="message" class="page-desc" data-testid="trial-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="trial-error">{{ errorMessage }}</p>

    <form class="analysis-form" data-testid="create-trial-form" @submit.prevent="submitCreate">
      <label>
        目标表
        <input v-model="form.table" data-testid="trial-table" placeholder="dwd_orders_merged" required />
      </label>
      <label>
        候选格式
        <select v-model="form.candidate_format" data-testid="trial-format">
          <option value="iceberg">iceberg</option>
          <option value="hudi">hudi</option>
          <option value="delta">delta</option>
        </select>
      </label>
      <label>
        写入模式
        <input v-model="form.write_mode" data-testid="trial-write-mode" placeholder="append / upsert" required />
      </label>
      <label>
        合并引擎
        <select v-model="form.merge_engine" data-testid="trial-merge-engine">
          <option value="copy-on-write">copy-on-write</option>
          <option value="merge-on-read">merge-on-read</option>
        </select>
      </label>
      <label>
        字段定义
        <select v-model="form.definition_id" data-testid="trial-definition">
          <option value="">自动选最新候选</option>
          <option v-for="item in definitionsForTable" :key="item.definition_id" :value="item.definition_id">
            {{ item.definition_id }} · {{ item.schema_version }} · r{{ item.revision }} · {{ item.status }}
          </option>
        </select>
      </label>
      <label class="samples-field">
        试写样本（JSON 数组，可留空由系统生成）
        <textarea v-model="form.samplesText" data-testid="trial-samples" rows="4" />
      </label>
      <button class="btn primary" data-testid="create-trial" type="submit">登记并试写</button>
      <button class="btn" data-testid="resolve-strategy" type="button" @click="resolveStrategy">
        解析线上写入策略
      </button>
    </form>

    <p class="page-desc">
      已登记的试写可以先用同一批样本跑“旧格式 vs 候选格式”对照，预演不写入、不占用试写轮次。
    </p>

    <section v-if="resolution" class="analysis-summary" data-testid="strategy-resolution">
      <article class="analysis-card">
        <span class="stat-label">解析来源</span>
        <strong class="stat-value">{{ resolution.strategy_source ?? (resolution.ok ? '—' : '无已确认组合') }}</strong>
        <p class="page-desc">{{ resolution.reason ?? resolution.note ?? '' }}</p>
      </article>
      <article class="analysis-card">
        <span class="stat-label">格式 / 写入模式</span>
        <strong class="stat-value">{{ resolution.format ?? '—' }} / {{ resolution.write_mode ?? '—' }}</strong>
        <p class="page-desc">合并引擎：{{ resolution.merge_engine ?? '—' }}</p>
      </article>
      <article class="analysis-card">
        <span class="stat-label">字段定义</span>
        <strong class="stat-value">{{ resolution.definition_id ?? '—' }}</strong>
        <p class="page-desc">字段：{{ (resolution.fields ?? []).join('、') || '—' }}</p>
      </article>
      <article class="analysis-card">
        <span class="stat-label">发布清单</span>
        <strong class="stat-value">{{ resolution.release_id ?? '—' }}</strong>
        <p class="page-desc" v-if="resolution.config_requires_attention">配置待处理，但线上仍按上一组合写入</p>
      </article>
    </section>

    <table class="data-table" data-testid="trial-table-list">
      <thead>
        <tr>
          <th>试写编号</th>
          <th>目标表</th>
          <th>候选格式 / 模式</th>
          <th>字段定义</th>
          <th>轮次</th>
          <th>状态</th>
          <th>结论</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="trial in trials" :key="trial.trial_id" :data-testid="`trial-${trial.trial_id}`">
          <td>{{ trial.trial_id }}</td>
          <td>{{ trial.table }}</td>
          <td>{{ trial.candidate_format }} / {{ trial.write_mode }}</td>
          <td>{{ trial.definition_id }} <span class="page-desc">r{{ trial.definition_revision }}</span></td>
          <td>{{ trial.round }}</td>
          <td>{{ trial.status }}</td>
          <td class="conclusion-cell">{{ trial.failure_reason || trial.conclusion || '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actionsFor(trial)"
              :key="action.label"
              class="link"
              type="button"
              :data-testid="`trial-action-${trial.trial_id}-${action.label}`"
              @click="action.run(trial)"
            >
              {{ action.label }}
            </button>
            <button class="link" type="button" :data-testid="`compare-${trial.trial_id}`" @click="runCompare(trial)">
              新旧对照
            </button>
            <button class="link" type="button" :data-testid="`review-${trial.trial_id}`" @click="openReview(trial)">
              复核
            </button>
          </td>
        </tr>
        <tr v-if="!trials.length">
          <td colspan="8" class="empty-state">还没有试写记录，先登记一次换格式试写</td>
        </tr>
      </tbody>
    </table>

    <section v-if="comparison" class="modal-backdrop" data-testid="compare-modal">
      <div class="modal">
        <h3>同一批样本：旧格式 vs 候选格式</h3>
        <p class="page-desc" data-testid="compare-verdict">{{ comparison.verdict }}</p>
        <table class="data-table">
          <thead>
            <tr><th>组合</th><th>格式 / 模式</th><th>字段定义</th><th>通过</th><th>拒绝</th></tr>
          </thead>
          <tbody>
            <tr data-testid="compare-baseline">
              <td>线上已确认</td>
              <td>{{ comparison.baseline.format ?? '—' }} / {{ comparison.baseline.write_mode ?? '—' }}</td>
              <td>{{ comparison.baseline.definition_id ?? '—' }}</td>
              <td>{{ comparison.baseline.passed_count }}</td>
              <td>{{ comparison.baseline.rejected_count }}</td>
            </tr>
            <tr data-testid="compare-candidate">
              <td>候选试写</td>
              <td>{{ comparison.candidate.format }} / {{ comparison.candidate.write_mode }}</td>
              <td>{{ comparison.candidate.definition_id }}</td>
              <td>{{ comparison.candidate.passed_count }}</td>
              <td>{{ comparison.candidate.rejected_count }}</td>
            </tr>
          </tbody>
        </table>
        <p class="page-desc">
          旧格式可写、候选格式会拒绝的行号：{{ comparison.regression_row_indexes.join('、') || '无' }}；
          候选格式新增可写的行号：{{ comparison.improvement_row_indexes.join('、') || '无' }}。
          本次对照 {{ comparison.recorded_as_attempt ? '已记录为写入尝试' : '仅为预演，未写入' }}。
        </p>
        <div class="page-actions">
          <button class="btn" type="button" @click="comparison = null">关闭</button>
        </div>
      </div>
    </section>

    <section v-if="review" class="modal-backdrop" data-testid="review-modal">
      <div class="modal trial-review">
        <h3>复核试写 {{ review.trial.trial_id }}</h3>
        <dl class="dag-detail">
          <div>
            <dt>采用字段定义</dt>
            <dd data-testid="review-definition">
              {{ review.adopted_definition?.definition_id ?? review.trial.definition_id }}
              （{{ review.adopted_definition?.schema_version }}，r{{ review.adopted_definition?.revision }}）
              <span v-if="review.definition_drifted" class="error-text">
                · 试写后已修订到 r{{ review.definition_revision_current }}，依据失效
              </span>
            </dd>
          </div>
          <div>
            <dt>试写结论</dt>
            <dd data-testid="review-conclusion">{{ review.conclusion || '尚未产生结论' }}</dd>
          </div>
          <div>
            <dt>支持生效的样本轮次</dt>
            <dd data-testid="review-supporting-round">
              <span v-if="review.supporting_round">
                第 {{ review.supporting_round.round }} 轮 · 通过 {{ review.supporting_round.passed_count }} 行 ·
                拒绝 {{ review.supporting_round.rejected_count }} 行（{{ review.supporting_round.attempt_id }}）
              </span>
              <span v-else>无</span>
            </dd>
          </div>
          <div>
            <dt>发布清单</dt>
            <dd data-testid="review-release">
              {{ review.release?.release_id ?? '—' }} · {{ review.release?.status ?? '—' }}
            </dd>
          </div>
          <div>
            <dt>下一次策略解析</dt>
            <dd data-testid="review-next-resolution">
              {{ review.next_resolution.ok
                ? `${review.next_resolution.format} / ${review.next_resolution.write_mode} · ${review.next_resolution.definition_id ?? '无定义'}`
                : review.next_resolution.reason }}
            </dd>
          </div>
        </dl>

        <h4>被拒样本</h4>
        <table class="data-table" data-testid="review-rejected">
          <thead>
            <tr><th>轮次</th><th>行号</th><th>拒绝类型</th><th>明细</th><th>原始行</th></tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in review.rejected_samples" :key="`${row.attempt_id}-${index}`">
              <td>{{ row.round }}</td>
              <td>{{ row.row_index }}</td>
              <td>{{ row.kinds.join('、') }}</td>
              <td>{{ row.issues.map((issue) => `${issue.field}: ${issue.detail}`).join('；') }}</td>
              <td class="raw-row">{{ JSON.stringify(row.row) }}</td>
            </tr>
            <tr v-if="!review.rejected_samples.length">
              <td colspan="5" class="empty-state">本次试写没有被拒样本</td>
            </tr>
          </tbody>
        </table>

        <div class="page-actions">
          <button class="btn" type="button" @click="review = null">关闭</button>
        </div>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  abandonTrial,
  compareTrialSamples,
  confirmTrial,
  createTrial,
  getTableStrategy,
  listDefinitions,
  listTrials,
  retryTrial,
  reviewTrial,
  runTrialAttempt,
  submitTrial,
} from '@/api/format-governance'
import type { FieldDefinition, StrategyResolution, Trial, TrialReview } from '@/data/types'

const trials = ref<Trial[]>([])
const definitions = ref<FieldDefinition[]>([])
const review = ref<TrialReview | null>(null)
const comparison = ref<Awaited<ReturnType<typeof compareTrialSamples>> | null>(null)
const resolution = ref<StrategyResolution | null>(null)
const message = ref('')
const errorMessage = ref('')

const form = ref({
  table: 'dwd_orders_merged',
  candidate_format: 'iceberg',
  write_mode: 'append',
  merge_engine: 'copy-on-write',
  definition_id: '',
  samplesText: JSON.stringify(
    [
      { order_id: 'ORD-1001', amount: '128.50', order_date: '2026-11-01', ts: '2026-11-01T10:00:00' },
      { order_id: 'ORD-1002', amount: '12.345', order_date: '2026-11-01', ts: '2026-11-01T10:05:00' },
      { order_id: 'ORD-1003', order_date: '2026-11-01', ts: '2026-11-01T10:10:00' },
    ],
    null,
    2,
  ),
})

const definitionsForTable = computed(() =>
  definitions.value.filter((item) => item.topic === form.value.table),
)

function parseSamples(): Record<string, unknown>[] | undefined {
  if (!form.value.samplesText.trim()) return undefined
  const parsed = JSON.parse(form.value.samplesText)
  if (!Array.isArray(parsed)) throw new Error('样本必须是 JSON 数组')
  return parsed as Record<string, unknown>[]
}

async function loadTrials() {
  errorMessage.value = ''
  try {
    const payload = await listTrials()
    trials.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '试写列表读取失败'
  }
}

async function loadDefinitions() {
  try {
    definitions.value = (await listDefinitions()).items
  } catch {
    definitions.value = []
  }
}

async function submitCreate() {
  message.value = ''
  errorMessage.value = ''
  try {
    const samples = parseSamples()
    const result = await createTrial({
      ...form.value,
      config_entry_id: undefined,
      samples,
    })
    if (!result.ok) {
      errorMessage.value = result.reason ?? '试写登记失败'
      return
    }
    message.value = `试写 ${result.trial.trial_id} 已登记${samples ? '并执行第一轮写入尝试' : ''}`
    await loadTrials()
    await loadDefinitions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '试写登记失败'
  }
}

async function resolveStrategy() {
  errorMessage.value = ''
  try {
    resolution.value = await getTableStrategy(form.value.table)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '策略解析失败'
  }
}

async function runInTrial(trial: Trial, action: () => Promise<{ ok: boolean; reason?: string; trial?: Trial }>) {
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await action()
    if (!result.ok) {
      errorMessage.value = result.reason ?? '操作未完成'
    } else {
      message.value = `试写 ${trial.trial_id} 已更新为 ${result.trial?.status ?? '新状态'}`
    }
    await loadTrials()
    await loadDefinitions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function attempt(trial: Trial) {
  let samples: Record<string, unknown>[] | undefined
  try {
    samples = parseSamples()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '样本解析失败'
    return
  }
  await runInTrial(trial, () => runTrialAttempt(trial.trial_id, samples))
}

function actionsFor(trial: Trial) {
  const actions: { label: string; run: (row: Trial) => void }[] = []
  if (['草稿', '试写中', '待确认'].includes(trial.status)) {
    actions.push({ label: '执行试写', run: (row) => void attempt(row) })
  }
  if (['草稿', '试写中'].includes(trial.status)) {
    actions.push({ label: '提交确认', run: (row) => void runInTrial(row, () => submitTrial(row.trial_id)) })
  }
  if (trial.status === '待确认') {
    actions.push({ label: '确认生效', run: (row) => void runInTrial(row, () => confirmTrial(row.trial_id)) })
  }
  if (trial.status === '已失败') {
    actions.push({ label: '退回重试', run: (row) => void runInTrial(row, () => retryTrial(row.trial_id)) })
  }
  if (['草稿', '试写中', '待确认'].includes(trial.status)) {
    actions.push({ label: '放弃试写', run: (row) => void runInTrial(row, () => abandonTrial(row.trial_id, '页面放弃')) })
  }
  return actions
}

async function runCompare(trial: Trial) {
  errorMessage.value = ''
  try {
    const samples = parseSamples()
    if (!samples?.length) throw new Error('对照需要至少一行样本')
    comparison.value = await compareTrialSamples(trial.trial_id, samples)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '对照执行失败'
  }
}

async function openReview(trial: Trial) {
  errorMessage.value = ''
  try {
    review.value = await reviewTrial(trial.trial_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核读取失败'
  }
}

onMounted(async () => {
  await Promise.all([loadTrials(), loadDefinitions()])
})
</script>

<style scoped>
.samples-field { flex: 1 1 320px; }
.samples-field textarea {
  width: 100%;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 10px;
  font-family: 'SFMono-Regular', Consolas, monospace;
  font-size: 12px;
}
.conclusion-cell { max-width: 280px; }
.trial-review { width: min(880px, 100%); }
.raw-row { max-width: 260px; word-break: break-all; font-family: monospace; font-size: 11px; }
</style>
