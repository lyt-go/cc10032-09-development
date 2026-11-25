import { request } from './client'
import type {
  FieldDefinition,
  FormatRelease,
  PageResult,
  StrategyResolution,
  Trial,
  TrialReview,
} from '@/data/types'

async function postJson<T>(path: string, body: unknown = {}): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = (await response.json()) as T & { ok?: boolean; reason?: string }
  if (!response.ok) throw new Error(payload.reason || `接口返回 ${response.status}`)
  return payload
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

export function listTrials(params: { table?: string; status?: string } = {}) {
  const query = new URLSearchParams(
    Object.entries(params).filter(([, value]) => value) as [string, string][],
  ).toString()
  return getJson<{ ok: boolean; items: Trial[]; total: number }>(`/api/format-trials?${query}`)
}

export function getTrial(trialId: string) {
  return getJson<{ ok: boolean; trial: Trial }>(`/api/format-trials/${trialId}`)
}

export function createTrial(payload: {
  table: string
  candidate_format: string
  write_mode: string
  merge_engine?: string
  definition_id?: string
  config_entry_id?: number
  note?: string
  samples?: Record<string, unknown>[]
}) {
  return postJson<{ ok: boolean; reason?: string; trial: Trial; release: FormatRelease }>(
    '/api/format-trials',
    payload,
  )
}

export function runTrialAttempt(trialId: string, samples?: Record<string, unknown>[]) {
  return postJson<{ ok: boolean; reason?: string; trial: Trial; release: FormatRelease }>(
    `/api/format-trials/${trialId}/attempts`,
    { samples },
  )
}

export function compareTrialSamples(trialId: string, samples: Record<string, unknown>[]) {
  return postJson<{
    ok: boolean
    reason?: string
    sample_count: number
    candidate: { format: string; write_mode: string; definition_id: string; passed_count: number; rejected_count: number; rejection_breakdown: Record<string, number> }
    baseline: { release_id: string | null; format: string | null; write_mode: string | null; definition_id: string | null; passed_count: number; rejected_count: number; note?: string }
    regression_row_indexes: number[]
    improvement_row_indexes: number[]
    verdict: string
    recorded_as_attempt: boolean
  }>(`/api/format-trials/${trialId}/compare`, { samples })
}

export function submitTrial(trialId: string) {
  return postJson<{ ok: boolean; reason?: string; trial: Trial; release: FormatRelease }>(
    `/api/format-trials/${trialId}/submit`,
  )
}

export function confirmTrial(trialId: string) {
  return postJson<{
    ok: boolean
    reason?: string
    failure_kind?: string
    required_action?: string
    trial: Trial
    release: FormatRelease
    live_write_restored?: boolean
    next_step?: string
  }>(`/api/format-trials/${trialId}/confirm`)
}

export function retryTrial(trialId: string, definitionId?: string) {
  return postJson<{ ok: boolean; reason?: string; trial: Trial; release: FormatRelease }>(
    `/api/format-trials/${trialId}/retry`,
    { definition_id: definitionId },
  )
}

export function abandonTrial(trialId: string, reason: string) {
  return postJson<{ ok: boolean; reason?: string; trial: Trial; release: FormatRelease }>(
    `/api/format-trials/${trialId}/abandon`,
    { reason },
  )
}

export function reviewTrial(trialId: string) {
  return getJson<TrialReview>(`/api/format-trials/${trialId}/review`)
}

export function listReleases(params: { table?: string; status?: string } = {}) {
  const query = new URLSearchParams(
    Object.entries(params).filter(([, value]) => value) as [string, string][],
  ).toString()
  return getJson<{ ok: boolean; items: FormatRelease[]; total: number }>(
    `/api/format-releases?${query}`,
  )
}

export function getTableStrategy(table: string) {
  return getJson<StrategyResolution>(`/api/format-strategies/${encodeURIComponent(table)}`)
}

export function listDefinitions(params: { topic?: string; status?: string } = {}) {
  const query = new URLSearchParams(
    Object.entries(params).filter(([, value]) => value) as [string, string][],
  ).toString()
  return getJson<{ ok: boolean; items: FieldDefinition[]; total: number }>(
    `/api/field-definitions?${query}`,
  )
}

export function amendDefinition(
  definitionId: string,
  payload: { changes?: Record<string, unknown>[]; version?: string; reason?: string },
) {
  return postJson<{
    ok: boolean
    reason?: string
    previous_definition_id?: string
    definition?: FieldDefinition
    revision_bumped?: boolean
    stale_trials?: string[]
  }>(`/api/field-definitions/${definitionId}/amend`, payload)
}

export function requestDeprecation(definitionId: string, cancelTrials: boolean, reason: string) {
  return postJson<{
    ok: boolean
    reason?: string
    protected?: boolean
    live_protected?: boolean
    references?: Record<string, unknown>
    cancelled_trials?: string[]
    note?: string
  }>(`/api/field-definitions/${definitionId}/deprecate`, {
    cancel_trials: cancelTrials,
    reason,
  })
}

export function definitionReferences(definitionId: string) {
  return getJson<{
    ok: boolean
    definition_id: string
    definition_status: string
    protected: boolean
    protection_reason: string
    references: {
      in_flight_trials: { trial_id: string; table: string; status: string; round: number }[]
      pending_releases: { release_id: string; table: string; status: string }[]
      live_strategies: { table: string; release_id: string | null }[]
    }
  }>(`/api/field-definitions/${definitionId}/references`)
}

export type { FieldDefinition, FormatRelease, Trial, TrialReview, StrategyResolution }
export type { PageResult }
