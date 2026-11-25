/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

// ---- 格式试写 / 字段定义治理 ----

export type TrialRejectedRow = {
  row_index: number
  row: Record<string, unknown>
  kinds: string[]
  issues: { field: string; kind: string; detail: string }[]
}

export type TrialAttempt = {
  attempt_id: string
  trial_id: string
  round: number
  table: string
  candidate_format: string
  write_mode: string
  merge_engine: string
  definition_id: string
  definition_fingerprint: string
  definition_revision: number
  schema_version: string
  status: string
  origin: string | null
  sample_count: number
  passed_count: number
  rejected_count: number
  rejection_breakdown: Record<string, number>
  key_column: string | null
  passed_rows: Record<string, unknown>[]
  rejected_rows: TrialRejectedRow[]
  created_at: number
}

export type Trial = {
  trial_id: string
  table: string
  candidate_format: string
  write_mode: string
  merge_engine: string
  definition_id: string
  definition_fingerprint: string
  definition_revision: number
  schema_version: string
  definition_fields: string[]
  config_entry_id: number | null
  status: string
  stage: string
  round: number
  release_id: string | null
  supersedes: string | null
  retry_of: string | null
  conclusion: string | null
  failure_reason: string | null
  baseline_release_id: string | null
  attempts: TrialAttempt[]
  created_at: number
  updated_at: number
}

export type FormatRelease = {
  release_id: string
  table: string
  format: string
  write_mode: string
  merge_engine: string
  definition_id: string
  definition_fingerprint: string
  definition_revision: number
  schema_version: string
  trial_id: string
  config_entry_id: number | null
  status: string
  conclusion: string | null
  evidence: Record<string, unknown> | null
  reason: string | null
  effective: boolean
  created_at: number
  updated_at: number
  decided_at: number | null
}

export type FieldDefinition = {
  definition_id: string
  topic: string
  schema_version: string
  revision: number
  fingerprint: string
  status: string
  fields: string[]
  field_count: number
  specs: Record<string, { name?: string; type?: string; precision?: number; scale?: number; nullable?: boolean; required?: boolean }>
  definition: Record<string, unknown>
  source_entry_id: number | null
  replaced_definition_id: string | null
  replaced_by: string | null
  change_reason: string | null
  created_at: number
  deprecated_at: number | null
  deprecate_reason: string | null
  reference_count?: number
  in_flight_trials?: string[]
  live_tables?: string[]
}

export type StrategyResolution = {
  ok: boolean
  reason?: string
  table?: string
  format?: string
  write_mode?: string
  merge_engine?: string
  recommended_write_mode?: string
  strategy_source?: string
  definition_id?: string
  definition_revision?: number
  schema_version?: string
  fields?: string[]
  release_id?: string | null
  confirmed_at?: number
  config_requires_attention?: boolean
  note?: string
  previous_release_id?: string | null
}

export type TrialReview = {
  ok: boolean
  trial: Trial
  adopted_definition: FieldDefinition | null
  definition_revision_at_trial: number
  definition_revision_current: number | null
  definition_drifted: boolean
  release: FormatRelease | null
  supporting_round: TrialAttempt | null
  conclusion: string | null
  failure_reason: string | null
  rejected_samples: (TrialRejectedRow & {
    attempt_id: string
    round: number
    definition_id: string
    candidate_format: string
    write_mode: string
  })[]
  next_resolution: StrategyResolution
  conflicts: Record<string, unknown>[]
  retry_of: string | null
}
