"""table_format 领域服务：表格式治理、换格式试写与写入策略解析。

业务逻辑：
1. 表格式配置：表格式类型（Iceberg/Hudi/Delta）、主键列、分区列、写入模式
2. 换格式试写：候选格式 + 字段定义先跑样本，草稿→试写中→待确认→已生效 逐级推进
3. 试写留痕：每次写入尝试关联表名、候选格式、字段定义编号，保留通过行与被拒行
4. 格式发布清单：试写结论回写清单，确认时校验字段定义是否在试写后被并发改动
5. 写入策略解析：优先读已确认的“格式 + 字段定义”组合，试写失败时仍按上一组合恢复写入
6. Schema 治理联动：记录字段定义被哪些在途试写引用，引用未清前拒绝废弃

跨模块数据流：
  lineage → table_format（血缘追踪结果决定表格式写入策略）
  schema_govern → table_format（字段定义版本作为试写候选与生效依据）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import (
    evaluate_row,
    schema_field_names,
    schema_field_specs,
    slugify,
    store,
)

TRIAL_DRAFT = "草稿"
TRIAL_RUNNING = "试写中"
TRIAL_PENDING = "待确认"
TRIAL_EFFECTIVE = "已生效"
TRIAL_FAILED = "已失败"
TRIAL_ABANDONED = "已放弃"
TRIAL_ACTIVE = (TRIAL_DRAFT, TRIAL_RUNNING, TRIAL_PENDING)

RELEASE_DRAFT = "草稿"
RELEASE_RUNNING = "试写中"
RELEASE_PENDING = "待确认"
RELEASE_PUBLISHED = "已发布"
RELEASE_REJECTED = "已驳回"
RELEASE_CANCELLED = "已取消"


class TableFormatService(BaseModuleService):
    MODULE = "table_format"
    ENTITY = "表格式配置"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["配置编号", "目标表", "表格式类型", "主键列", "分区列", "排序键列", "写入模式", "合并引擎"]
    REQUIRED_FIELDS = ["配置编号", "目标表", "表格式类型"]
    STATUS_ORDER = ["草稿", "待验证", "试写中", "待确认", "已生效", "已停用"]
    ACTION_RULES = {
        "提交验证": "待验证",
        "提交试写": "试写中",
        "提交确认": "待确认",
        "确认生效": "已生效",
        "退回重试": "草稿",
        "放弃试写": "草稿",
        "标记生效": "已生效",
        "停用配置": "已停用",
    }
    NEGATIVE_ACTIONS = ["停用配置"]

    PIPELINE = "table_format_governance"
    UPSTREAM_MODULES = ["lineage", "schema_govern"]
    PRODUCES_ARTIFACT = "表格式写入配置记录"

    SUPPORTED_FORMATS = ("iceberg", "hudi", "delta")
    VALID_WRITE_MODES = {
        "iceberg": ("append", "overwrite", "upsert"),
        "hudi": ("insert", "upsert", "insert_overwrite", "bulk_insert"),
        "delta": ("append", "overwrite", "merge"),
    }
    VALID_MERGE_ENGINES = ("copy-on-write", "merge-on-read")
    TRIAL_ACTIONS = ("提交试写", "提交确认", "确认生效", "退回重试", "放弃试写")

    # ------------------------------------------------------------------
    # 原有格式校验
    # ------------------------------------------------------------------
    def validate_format(self, entry_id: int) -> dict[str, Any]:
        """验证表格式配置的有效性（类型、写入模式、合并引擎三者自洽）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        return self._validate_combo(
            str(entry.get("表格式类型", "")).lower(),
            str(entry.get("写入模式", "append")),
            str(entry.get("合并引擎", "copy-on-write")),
        )

    def _validate_combo(self, fmt: str, write_mode: str, merge_engine: str) -> dict[str, Any]:
        fmt = fmt.lower()
        if fmt not in self.SUPPORTED_FORMATS:
            return {"ok": False, "reason": f"表格式 {fmt} 不在支持列表: {self.SUPPORTED_FORMATS}"}
        if write_mode not in self.VALID_WRITE_MODES.get(fmt, ()):
            return {"ok": False, "reason": f"写入模式 {write_mode} 与格式 {fmt} 不兼容"}
        if merge_engine not in self.VALID_MERGE_ENGINES:
            return {"ok": False, "reason": f"合并引擎 {merge_engine} 不受支持"}
        return {"ok": True, "format": fmt, "write_mode": write_mode, "merge_engine": merge_engine}

    # ------------------------------------------------------------------
    # 试写创建 / 执行
    # ------------------------------------------------------------------
    def create_trial(
        self,
        *,
        table: str,
        candidate_format: str,
        write_mode: str,
        merge_engine: str = "copy-on-write",
        definition_id: str | None = None,
        config_entry_id: int | None = None,
        operator: str = "数据工程组",
        note: str = "",
    ) -> dict[str, Any]:
        """登记一次换格式试写（草稿状态），此时还没有触碰线上写入组合。"""
        table = str(table or "").strip()
        if not table:
            return {"ok": False, "reason": "目标表为空，无法登记试写"}

        combo = self._validate_combo(candidate_format, write_mode, merge_engine)
        if not combo["ok"]:
            return combo
        candidate_format = combo["format"]

        definition = self._resolve_trial_definition(table, definition_id)
        if isinstance(definition, dict) and definition.get("ok") is False:
            return definition
        assert definition is not None

        config_entry = self._resolve_config(table, config_entry_id)
        if isinstance(config_entry, dict) and config_entry.get("ok") is False:
            return config_entry
        if config_entry is not None and config_entry.get("status") == "已停用":
            return {
                "ok": False,
                "reason": f"表格式配置 {config_entry.get('id')} 已停用，不能发起换格式试写",
            }
        config_entry_id = int(config_entry["id"]) if config_entry else None

        active = [
            t for t in store.trials(table)
            if t["status"] in TRIAL_ACTIVE
            and t["candidate_format"] == candidate_format
            and t["write_mode"] == write_mode
            and t["merge_engine"] == merge_engine
        ]
        if active:
            running = active[-1]
            return {
                "ok": False,
                "reason": (
                    f"表 {table} 的 {candidate_format}/{write_mode} 已有在途试写 {running['trial_id']}"
                    f"（{running['status']}），请先完成、退回或放弃后再登记"
                ),
                "trial_id": running["trial_id"],
                "trial": self.trial_view(running),
            }

        trial = store.create_trial(
            table=table,
            candidate_format=candidate_format,
            write_mode=write_mode,
            merge_engine=merge_engine,
            definition=definition,
            config_entry_id=config_entry_id,
            operator=operator,
            note=note,
        )
        release = store.add_release({
            "table": table,
            "format": candidate_format,
            "write_mode": write_mode,
            "merge_engine": merge_engine,
            "definition_id": definition["definition_id"],
            "definition_fingerprint": definition["fingerprint"],
            "definition_revision": definition["revision"],
            "schema_version": definition["schema_version"],
            "trial_id": trial["trial_id"],
            "config_entry_id": config_entry_id,
            "status": RELEASE_DRAFT,
            "conclusion": "试写已登记，尚未执行样本",
            "evidence": {"rounds": 0, "supporting_round": None},
            "reason": note,
            "effective": False,
            "decided_at": None,
        })
        trial["release_id"] = release["release_id"]

        if config_entry is not None:
            config_entry["status"] = "试写中"
            config_entry["pending"] = True
            config_entry["abnormal"] = False

        return {"ok": True, "trial": self.trial_view(trial), "release": self.release_view(release)}

    def execute_trial_attempt(
        self,
        trial_id: str,
        samples: list[dict[str, Any]] | None = None,
        *,
        origin: str = "手工提交样本",
    ) -> dict[str, Any]:
        """执行一轮实际写入尝试：通过行写入，类型/缺列行拒绝并留档。"""
        trial = store.trial(trial_id)
        if trial is None:
            return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
        if trial["status"] in (TRIAL_EFFECTIVE, TRIAL_ABANDONED):
            return {"ok": False, "reason": f"试写 {trial_id} 已{trial['status']}，不能再执行写入尝试"}

        definition = store.definition(trial["definition_id"])
        if definition is None or definition["status"] == "已废弃":
            return {
                "ok": False,
                "reason": f"试写依据的字段定义 {trial['definition_id']} 已不存在或已废弃，请重新登记试写",
                "requires_new_trial": True,
            }

        specs = schema_field_specs(trial["definition_snapshot"])
        if not specs:
            return {"ok": False, "reason": "字段定义没有可用字段，无法试写"}

        rows = samples if samples is not None else self._default_samples(specs, trial["table"])
        if not rows:
            return {"ok": False, "reason": "样本为空，试写没有可写入的行"}

        key_col = self._key_column(trial)
        if trial["write_mode"] in ("upsert", "merge", "insert_overwrite") and not key_col:
            return {
                "ok": False,
                "reason": f"写入模式 {trial['write_mode']} 需要主键列，但字段定义和表配置都没有主键",
            }
        if trial["write_mode"] in ("upsert", "merge") and key_col and key_col not in specs:
            return {
                "ok": False,
                "reason": f"主键列 {key_col} 不在字段定义 {trial['definition_id']} 中，写入模式无法落地",
            }

        passed_rows: list[dict[str, Any]] = []
        rejected_rows: list[dict[str, Any]] = []
        rejection_breakdown: dict[str, int] = {}
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                rejected_rows.append({
                    "row_index": index,
                    "row": row,
                    "kinds": ["类型"],
                    "issues": [{"field": "-", "kind": "类型", "detail": "样本不是行对象"}],
                })
                rejection_breakdown["类型"] = rejection_breakdown.get("类型", 0) + 1
                continue
            issues = evaluate_row(row, specs)
            if trial["write_mode"] in ("upsert", "merge") and key_col:
                key_value = row.get(key_col)
                if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
                    issues.append({
                        "field": key_col,
                        "kind": "缺列",
                        "detail": f"写入模式 {trial['write_mode']} 要求主键列有值",
                    })
            if not issues:
                passed_rows.append(dict(row))
                continue
            kinds = sorted({item["kind"] for item in issues})
            for kind in kinds:
                rejection_breakdown[kind] = rejection_breakdown.get(kind, 0) + 1
            rejected_rows.append({
                "row_index": index,
                "row": dict(row),
                "kinds": kinds,
                "issues": issues,
            })

        if passed_rows and not rejected_rows:
            status = "通过"
        elif passed_rows:
            status = "部分通过"
        else:
            status = "失败"

        attempt = store.record_trial_attempt(trial, {
            "table": trial["table"],
            "candidate_format": trial["candidate_format"],
            "write_mode": trial["write_mode"],
            "merge_engine": trial["merge_engine"],
            "definition_id": trial["definition_id"],
            "definition_fingerprint": trial["definition_fingerprint"],
            "definition_revision": trial["definition_revision"],
            "schema_version": trial["schema_version"],
            "status": status,
            "origin": origin,
            "sample_count": len(rows),
            "passed_count": len(passed_rows),
            "rejected_count": len(rejected_rows),
            "rejection_breakdown": rejection_breakdown,
            "passed_rows": passed_rows,
            "rejected_rows": rejected_rows,
            "key_column": key_col,
        })

        # 新一轮样本会推翻“待确认”结论，必须重新提交确认
        trial["status"] = TRIAL_RUNNING
        trial["updated_at"] = time.time()
        trial["conclusion"] = self._attempt_conclusion(attempt)
        config_entry = self._config_row(trial)
        if config_entry is not None:
            config_entry["status"] = "试写中"
            config_entry["pending"] = True
        self._sync_release(trial, status=RELEASE_RUNNING, conclusion=trial["conclusion"])

        return {
            "ok": True,
            "trial": self.trial_view(trial),
            "attempt": self.attempt_view(attempt),
            "release": self.release_view(store.release(trial["release_id"])),
            "next_step": "提交确认" if passed_rows else "调整样本或字段定义后重新试写",
        }

    def compare_samples(
        self, trial_id: str, samples: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """用同一批样本分别跑线上已确认组合与候选组合，作为换格式前的对照。

        这是只读预演：不写入任何一行，也不产生正式写入尝试记录，
        确认前想先看“旧格式能写、新格式写不进”的行就靠这里。
        """
        trial = store.trial(trial_id)
        if trial is None:
            return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
        if not samples:
            return {"ok": False, "reason": "对照样本为空"}

        candidate = self._dry_run(
            samples,
            specs=schema_field_specs(trial["definition_snapshot"]),
            write_mode=trial["write_mode"],
            key_col=self._key_column(trial),
        )
        live = store.strategy(trial["table"])
        baseline = None
        if live is not None:
            live_definition = store.definition(live["definition_id"])
            baseline = {
                "release_id": live.get("release_id"),
                "format": live["format"],
                "write_mode": live["write_mode"],
                "definition_id": live["definition_id"],
                **self._dry_run(
                    samples,
                    specs=schema_field_specs(
                        live_definition["definition"] if live_definition else None),
                    write_mode=live["write_mode"],
                    key_col=self._key_column({"table": trial["table"], "config_entry_id": live.get("config_entry_id")}),
                ),
            }
        else:
            baseline = {
                "release_id": None,
                "format": None,
                "write_mode": None,
                "definition_id": None,
                "passed_count": 0,
                "rejected_count": len(samples),
                "passed_indexes": [],
                "rejected_indexes": list(range(len(samples))),
                "rejection_breakdown": {},
                "note": "表上还没有已确认组合，没有可对照的旧格式",
            }

        regressions = [
            index for index in baseline["passed_indexes"]
            if index not in candidate["passed_indexes"]
        ]
        improvements = [
            index for index in candidate["passed_indexes"]
            if index not in baseline["passed_indexes"]
        ]
        if not regressions:
            verdict = "候选格式可承接同一批样本"
        else:
            verdict = f"有 {len(regressions)} 行旧格式可写、候选格式会拒绝，确认前需处理"

        preview = {
            "trial_id": trial_id,
            "table": trial["table"],
            "sample_count": len(samples),
            "candidate": {
                "format": trial["candidate_format"],
                "write_mode": trial["write_mode"],
                "definition_id": trial["definition_id"],
                **candidate,
            },
            "baseline": baseline,
            "regression_row_indexes": regressions,
            "improvement_row_indexes": improvements,
            "verdict": verdict,
            "recorded_as_attempt": False,
        }
        trial["last_comparison"] = preview
        return {"ok": True, **preview}

    def _dry_run(
        self,
        samples: list[dict[str, Any]],
        *,
        specs: dict[str, dict[str, Any]],
        write_mode: str,
        key_col: str,
    ) -> dict[str, Any]:
        passed_indexes: list[int] = []
        rejected_indexes: list[int] = []
        breakdown: dict[str, int] = {}
        rejected_rows: list[dict[str, Any]] = []
        if not specs:
            return {
                "passed_count": 0,
                "rejected_count": len(samples),
                "passed_indexes": [],
                "rejected_indexes": list(range(len(samples))),
                "rejection_breakdown": {"缺列": len(samples)},
                "rejected_rows": [],
                "note": "没有可用字段定义",
            }
        for index, row in enumerate(samples):
            if not isinstance(row, dict):
                rejected_indexes.append(index)
                breakdown["类型"] = breakdown.get("类型", 0) + 1
                rejected_rows.append({
                    "row_index": index, "row": row, "kinds": ["类型"],
                    "issues": [{"field": "-", "kind": "类型", "detail": "样本不是行对象"}],
                })
                continue
            issues = evaluate_row(row, specs)
            if write_mode in ("upsert", "merge") and key_col:
                key_value = row.get(key_col)
                if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
                    issues.append({
                        "field": key_col, "kind": "缺列",
                        "detail": f"写入模式 {write_mode} 要求主键列有值",
                    })
            if issues:
                rejected_indexes.append(index)
                kinds = sorted({item["kind"] for item in issues})
                for kind in kinds:
                    breakdown[kind] = breakdown.get(kind, 0) + 1
                rejected_rows.append({
                    "row_index": index, "row": dict(row), "kinds": kinds, "issues": issues,
                })
            else:
                passed_indexes.append(index)
        return {
            "passed_count": len(passed_indexes),
            "rejected_count": len(rejected_indexes),
            "passed_indexes": passed_indexes,
            "rejected_indexes": rejected_indexes,
            "rejection_breakdown": breakdown,
            "rejected_rows": rejected_rows,
        }

    def submit_trial(self, trial_id: str) -> dict[str, Any]:
        """试写中 → 待确认：把本试写的结论回写到格式发布清单。"""
        trial = store.trial(trial_id)
        if trial is None:
            return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
        if trial["status"] not in (TRIAL_RUNNING, TRIAL_DRAFT):
            return {"ok": False, "reason": f"试写当前状态为 {trial['status']}，不能提交确认"}

        attempts = store.trial_attempts(trial_id)
        if not attempts:
            return {"ok": False, "reason": "还没有任何写入尝试，不能提交确认", "requires_attempt": True}
        latest = attempts[-1]
        if latest["passed_count"] <= 0:
            return {
                "ok": False,
                "reason": f"第 {latest['round']} 轮样本没有任何通过行，不能提交确认，需要重新试写",
                "requires_retry": True,
                "attempt": self.attempt_view(latest),
            }

        trial["status"] = TRIAL_PENDING
        trial["updated_at"] = time.time()
        config_entry = self._config_row(trial)
        if config_entry is not None:
            config_entry["status"] = "待确认"
            config_entry["pending"] = True

        conclusion = self._attempt_conclusion(latest)
        release = self._sync_release(
            trial,
            status=RELEASE_PENDING,
            conclusion=f"{conclusion}；待确认生效",
            evidence_extra={"supporting_round": latest["attempt_id"]},
        )
        return {
            "ok": True,
            "trial": self.trial_view(trial),
            "release": self.release_view(release),
            "supporting_round": self._attempt_summary(latest),
        }

    # ------------------------------------------------------------------
    # 确认生效 / 冲突 / 重试 / 放弃
    # ------------------------------------------------------------------
    def confirm_trial(self, trial_id: str) -> dict[str, Any]:
        """待确认 → 已生效：只有带完整试写依据且字段定义未漂移的组合才能生效。"""
        with store._lock:
            trial = store.trial(trial_id)
            if trial is None:
                return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
            if trial["status"] == TRIAL_EFFECTIVE:
                return {"ok": False, "reason": f"试写 {trial_id} 已生效，无需重复确认"}
            if trial["status"] != TRIAL_PENDING:
                return {
                    "ok": False,
                    "reason": f"试写当前状态为 {trial['status']}，只有待确认的试写可以确认生效",
                }

            # 登记时与当前线上组合必须一致，否则属于不同版本试写互相踩踏
            live = store.strategy(trial["table"])
            baseline = trial.get("baseline_strategy")
            baseline_release = (baseline or {}).get("release_id")
            live_release = (live or {}).get("release_id")
            if live_release != baseline_release:
                conflict = store.add_conflict({
                    "table": trial["table"],
                    "kind": "试写版本冲突",
                    "trial_id": trial["trial_id"],
                    "trial_release_id": trial["release_id"],
                    "baseline_release_id": baseline_release,
                    "current_release_id": live_release,
                    "trial_definition_id": trial["definition_id"],
                    "current_definition_id": (live or {}).get("definition_id"),
                    "reason": "确认时发现线上组合与试写登记时的基线不一致，本试写的基线已过期",
                    "resolution": "按当前线上组合重新试写，或显式复核后再确认",
                })
                return self._fail_trial(
                    trial,
                    reason=(
                        f"表 {trial['table']} 的写入组合已变化"
                        f"（基线 {baseline_release or '无'}，当前 {live_release or '无'}）"
                    ),
                    kind="版本冲突",
                    required_action="基于最新线上组合重新试写",
                    conflict=conflict,
                )

            attempts = store.trial_attempts(trial_id)
            if not attempts:
                return self._fail_trial(
                    trial,
                    reason="试写没有任何写入尝试，缺少生效依据",
                    kind="依据缺失",
                    required_action="先执行至少一轮试写",
                )
            latest = attempts[-1]
            if latest["passed_count"] <= 0:
                return self._fail_trial(
                    trial,
                    reason="最近一轮样本没有通过行，缺少生效依据",
                    kind="依据缺失",
                    required_action="补充可用样本重新试写",
                )
            if latest["definition_fingerprint"] != trial["definition_fingerprint"]:
                return self._fail_trial(
                    trial,
                    reason="最近一轮试写依据与登记时的字段定义不一致",
                    kind="版本冲突",
                    required_action="按当前字段定义重新试写",
                )

            definition = store.definition(trial["definition_id"])
            if definition is None or definition["status"] == "已废弃":
                return self._fail_trial(
                    trial,
                    reason=f"字段定义 {trial['definition_id']} 已被废弃，试写依据失效",
                    kind="定义废弃",
                    required_action="选择新的字段定义重新试写",
                )
            if definition["fingerprint"] != trial["definition_fingerprint"]:
                return self._fail_trial(
                    trial,
                    reason="字段定义内容在试写后被修改，试写依据不再匹配",
                    kind="版本冲突",
                    required_action="按最新字段定义重新试写",
                )

            # 字段发布与候选确认并发：主题上有更新的 revision 时，旧依据必须作废
            current_revision = store.topic_revision(definition["topic"])
            if current_revision != trial["definition_revision"]:
                latest_definition = store.latest_definition(definition["topic"])
                conflict = store.add_conflict({
                    "table": trial["table"],
                    "kind": "字段定义版本冲突",
                    "trial_id": trial["trial_id"],
                    "trial_release_id": trial["release_id"],
                    "trial_definition_id": definition["definition_id"],
                    "trial_definition_revision": trial["definition_revision"],
                    "current_definition_id": (
                        latest_definition["definition_id"] if latest_definition else None
                    ),
                    "current_definition_revision": current_revision,
                    "reason": "字段定义在试写提交后发布了新版本，试写依据已过期",
                    "resolution": "按最新字段定义重新试写后再确认",
                })
                return self._fail_trial(
                    trial,
                    reason=(
                        f"字段定义已在试写后发布新版本：试写基于 revision {trial['definition_revision']}"
                        f"（{trial['definition_id']}），当前已到 revision {current_revision}"
                        f"（{latest_definition['definition_id'] if latest_definition else '未知'}）"
                    ),
                    kind="版本冲突",
                    required_action="按最新字段定义重新试写后再确认",
                    current_definition=latest_definition,
                    conflict=conflict,
                )

            # ---- 通过全部校验，正式换组合 ----
            now = time.time()
            previous_strategy = dict(live) if live else None
            strategy = {
                "table": trial["table"],
                "format": trial["candidate_format"],
                "write_mode": trial["write_mode"],
                "merge_engine": trial["merge_engine"],
                "definition_id": definition["definition_id"],
                "definition_fingerprint": definition["fingerprint"],
                "definition_revision": definition["revision"],
                "schema_version": definition["schema_version"],
                "config_entry_id": trial["config_entry_id"],
                "release_id": trial["release_id"],
                "confirmed_at": now,
                "previous": previous_strategy,
                "source": "trial_confirmed",
                "trial_id": trial["trial_id"],
            }
            store.set_strategy(trial["table"], strategy)

            # 同主题只能有一个“已确认”的字段定义，旧的退为“已退役”
            for other in store.definitions(topic=definition["topic"], status="已确认"):
                if other["definition_id"] != definition["definition_id"]:
                    other["status"] = "已退役"
            definition["status"] = "已确认"

            trial["status"] = TRIAL_EFFECTIVE
            trial["updated_at"] = now
            trial["conclusion"] = self._attempt_conclusion(latest) + "；已确认生效"
            config_entry = self._config_row(trial)
            if config_entry is not None:
                config_entry["目标表"] = trial["table"]
                config_entry["表格式类型"] = trial["candidate_format"]
                config_entry["写入模式"] = trial["write_mode"]
                config_entry["合并引擎"] = trial["merge_engine"]
                config_entry["status"] = "已生效"
                config_entry["pending"] = False
                config_entry["abnormal"] = False

            release = self._sync_release(
                trial,
                status=RELEASE_PUBLISHED,
                conclusion=(
                    f"{self._attempt_conclusion(latest)}；已确认生效，"
                    f"采用字段定义 {definition['definition_id']}（revision {definition['revision']}）"
                ),
                evidence_extra={"supporting_round": latest["attempt_id"]},
                decided=True,
            )

            # 关闭同表同格式其它在途试写，避免旧依据继续挂着
            for other in store.trials(trial["table"]):
                if other is trial or other["status"] not in TRIAL_ACTIVE:
                    continue
                if other["candidate_format"] != trial["candidate_format"]:
                    continue
                other["status"] = TRIAL_ABANDONED
                other["updated_at"] = now
                other["conclusion"] = f"同格式试写 {trial['trial_id']} 已生效，本试写自动收束"
                self._sync_release(
                    other,
                    status=RELEASE_CANCELLED,
                    conclusion=other["conclusion"],
                    decided=True,
                )

            store.append_changelog({
                "目标表": trial["table"],
                "变更类型": "ALTER_FORMAT",
                "变更内容": (
                    f"表格式切换为 {trial['candidate_format']}/{trial['write_mode']}，"
                    f"字段定义 {definition['definition_id']}"
                ),
                "操作时间": now,
                "操作来源": "table_format.trial_confirm",
                "影响行数": latest["passed_count"],
                "回滚版本": (previous_strategy or {}).get("release_id") or "",
            })

            return {
                "ok": True,
                "trial": self.trial_view(trial),
                "release": self.release_view(release),
                "strategy": strategy,
                "supporting_round": self._attempt_summary(latest),
                "adopted_definition": self._definition_summary(definition),
                "previous_release_id": (previous_strategy or {}).get("release_id"),
            }

    def _fail_trial(
        self,
        trial: dict[str, Any],
        *,
        reason: str,
        kind: str,
        required_action: str,
        current_definition: dict[str, Any] | None = None,
        conflict: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """确认失败：试写判失败、配置退回可再试、发布清单记录驳回原因。"""
        now = time.time()
        trial["status"] = TRIAL_FAILED
        trial["updated_at"] = now
        trial["failure_reason"] = reason
        trial["conclusion"] = f"确认失败：{reason}"

        config_entry = self._config_row(trial)
        previous_status = config_entry.get("status") if config_entry else None
        if config_entry is not None:
            config_entry["status"] = "草稿"
            config_entry["pending"] = True
            config_entry["abnormal"] = True

        live = store.strategy(trial["table"])
        release = self._sync_release(
            trial,
            status=RELEASE_REJECTED,
            conclusion=f"确认失败：{reason}；需{required_action}",
            evidence_extra={
                "failure_kind": kind,
                "required_action": required_action,
                "current_definition_id": (current_definition or {}).get("definition_id"),
            },
            decided=True,
        )
        return {
            "ok": False,
            "reason": reason,
            "failure_kind": kind,
            "required_action": required_action,
            "trial": self.trial_view(trial),
            "release": self.release_view(release),
            "config_status": config_entry.get("status") if config_entry else None,
            "config_previous_status": previous_status,
            "retry_available": True,
            "current_definition": (
                self._definition_summary(current_definition) if current_definition else None
            ),
            "conflict": conflict,
            "live_strategy": self._strategy_summary(live) if live else None,
            "live_write_restored": bool(live),
            "next_step": f"退回重试：{required_action}",
        }

    def retry_trial(self, trial_id: str, definition_id: str | None = None) -> dict[str, Any]:
        """失败后回到可再试阶段：基于当前字段定义登记新一轮试写。"""
        with store._lock:
            old = store.trial(trial_id)
            if old is None:
                return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
            if old["status"] not in (TRIAL_FAILED, TRIAL_PENDING, TRIAL_RUNNING, TRIAL_DRAFT):
                return {"ok": False, "reason": f"试写当前状态为 {old['status']}，不能重试"}

            target_definition = self._resolve_trial_definition(old["table"], definition_id)
            if isinstance(target_definition, dict) and target_definition.get("ok") is False:
                return target_definition
            assert target_definition is not None

            # 同格式的旧在途试写收束，避免两条依据并行
            for other in store.trials(old["table"]):
                if other["status"] in TRIAL_ACTIVE and other["candidate_format"] == old["candidate_format"]:
                    other["status"] = TRIAL_ABANDONED
                    other["updated_at"] = time.time()
                    other["conclusion"] = f"由新一轮试写 {old['trial_id']} 的重试收束"
                    self._sync_release(other, status=RELEASE_CANCELLED, conclusion=other["conclusion"], decided=True)

            created = self.create_trial(
                table=old["table"],
                candidate_format=old["candidate_format"],
                write_mode=old["write_mode"],
                merge_engine=old["merge_engine"],
                definition_id=target_definition["definition_id"],
                config_entry_id=old["config_entry_id"],
                operator=old.get("operator", "数据工程组"),
                note=f"由试写 {old['trial_id']} 退回重试",
            )
            if not created.get("ok"):
                return created
            record = store.trial(created["trial"]["trial_id"])
            assert record is not None
            record["supersedes"] = old["trial_id"]
            record["retry_of"] = old["trial_id"]
            created["trial"] = self.trial_view(record)
            return {
                "ok": True,
                "previous_trial": self.trial_view(old),
                "trial": created["trial"],
                "release": created["release"],
                "definition": self._definition_summary(target_definition),
                "next_step": "执行试写样本后提交确认",
            }

    def abandon_trial(self, trial_id: str, reason: str = "人工放弃") -> dict[str, Any]:
        with store._lock:
            trial = store.trial(trial_id)
            if trial is None:
                return {"ok": False, "reason": f"试写 {trial_id} 不存在"}
            if trial["status"] == TRIAL_EFFECTIVE:
                return {"ok": False, "reason": "已生效的试写不能放弃"}
            trial["status"] = TRIAL_ABANDONED
            trial["updated_at"] = time.time()
            trial["conclusion"] = f"已放弃：{reason}"
            config_entry = self._config_row(trial)
            if config_entry is not None and config_entry.get("status") != "已生效":
                config_entry["status"] = "草稿"
                config_entry["pending"] = True
            release = self._sync_release(
                trial,
                status=RELEASE_CANCELLED,
                conclusion=trial["conclusion"],
                reason=reason,
                decided=True,
            )
            return {"ok": True, "trial": self.trial_view(trial), "release": self.release_view(release)}

    # ------------------------------------------------------------------
    # 写入策略解析
    # ------------------------------------------------------------------
    def resolve_write_strategy(self, entry_id: int) -> dict[str, Any]:
        """解析表的写入策略：已确认组合优先，失败/试写中不影响线上恢复写入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}

        table_name = str(entry.get("目标表", ""))
        live = store.strategy(table_name)
        if live is not None:
            return self._resolve_from_live(entry, live)

        # 兼容旧行为：没有已确认组合时，按已生效配置 + 最新 Schema 给出建议
        if entry.get("status") != "已生效":
            return {"ok": False, "reason": f"配置未生效，当前状态: {entry.get('status')}"}

        fmt = str(entry.get("表格式类型", "iceberg")).lower()
        lineage_counts = self._lineage_counts(table_name)
        schema_versions = store.schema_versions(table_name)
        latest_schema = schema_versions[-1] if schema_versions else None

        strategy = {
            "format": fmt,
            "table": table_name,
            "strategy_source": "配置建议（尚无已确认组合）",
            **lineage_counts,
            "schema_version": latest_schema.get("version") if latest_schema else None,
        }
        if lineage_counts["lineage_upstream_count"] > 2 or lineage_counts["lineage_downstream_count"] > 2:
            strategy["recommended_write_mode"] = "upsert" if fmt == "iceberg" else "merge"
            strategy["reason"] = "血缘关系复杂，推荐使用upsert/merge模式"
        else:
            strategy["recommended_write_mode"] = "append"
            strategy["reason"] = "血缘关系简单，使用append模式"

        if latest_schema:
            fields = latest_schema.get("definition", {}).get("fields", [])
            strategy["partition_recommendation"] = "按时间分区" if len(fields) > 10 else "按主键分区"

        batch_id = f"TABLE_FMT_{entry_id}_{int(time.time())}"
        self.produce_artifact([strategy], batch_id)
        return {"ok": True, "entry_id": entry_id, **strategy}

    def resolve_strategy_for_table(self, table: str) -> dict[str, Any]:
        """按表名解析线上写入策略（供同步任务直接消费）。"""
        live = store.strategy(table)
        if live is None:
            return {
                "ok": False,
                "reason": f"表 {table} 尚无已确认的格式+字段定义组合",
                "table": table,
                "confirmed": False,
            }
        entry = store.find(self.MODULE, int(live.get("config_entry_id") or 0))
        return self._resolve_from_live(entry or {"目标表": table}, live, table_override=table)

    def _resolve_from_live(
        self,
        entry: dict[str, Any],
        live: dict[str, Any],
        *,
        table_override: str | None = None,
    ) -> dict[str, Any]:
        table_name = table_override or str(entry.get("目标表", ""))
        definition = store.definition(live["definition_id"])
        definition_summary = self._definition_summary(definition) if definition else None
        lineage_counts = self._lineage_counts(table_name)
        config_status = entry.get("status")
        resolution = {
            "ok": True,
            "entry_id": entry.get("id"),
            "table": table_name,
            "format": live["format"],
            "write_mode": live["write_mode"],
            "merge_engine": live["merge_engine"],
            "recommended_write_mode": live["write_mode"],
            "strategy_source": "已确认组合",
            "definition_id": live["definition_id"],
            "definition_revision": live.get("definition_revision"),
            "definition_fingerprint": live.get("definition_fingerprint"),
            "schema_version": live.get("schema_version"),
            "definition": definition_summary,
            "fields": definition_summary["fields"] if definition_summary else [],
            "release_id": live.get("release_id"),
            "confirmed_at": live.get("confirmed_at"),
            "lineage_upstream_count": lineage_counts["lineage_upstream_count"],
            "lineage_downstream_count": lineage_counts["lineage_downstream_count"],
            "config_status": config_status,
            "config_requires_attention": config_status not in (None, "已生效"),
            "previous_release_id": (live.get("previous") or {}).get("release_id"),
            "reason": (
                f"按格式发布清单 {live.get('release_id')} 确认的 {live['format']}/{live['write_mode']}"
                f" 组合写入，字段定义 {live['definition_id']}"
            ),
        }
        if definition_summary:
            resolution["partition_recommendation"] = (
                "按时间分区" if definition_summary["field_count"] > 10 else "按主键分区"
            )
        if resolution["config_requires_attention"]:
            resolution["note"] = (
                f"配置 {entry.get('id')} 当前状态为 {config_status}，但线上仍按上一已确认组合写入"
            )
        return resolution

    # ------------------------------------------------------------------
    # 通用动作入口（工作台按钮）
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "标记生效":
            # 兼容旧按钮：不再允许直接改状态，必须落到已确认的试写结论上
            trial = self._latest_trial_for_entry_entry(entry_id)
            if trial is None or trial["status"] != TRIAL_PENDING:
                return None, "标记生效必须基于待确认的试写结论，请先提交试写并确认"
            result = self.confirm_trial(trial["trial_id"])
            if not result.get("ok"):
                return None, result["reason"]
            return store.find(self.MODULE, entry_id), f"试写 {trial['trial_id']} 已确认生效"

        if action not in self.TRIAL_ACTIONS:
            return super().run_action(entry_id, action)

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "提交试写":
            trial = self._latest_trial_for_entry(entry)
            if trial is not None and trial["status"] in (TRIAL_DRAFT, TRIAL_RUNNING):
                result = self.execute_trial_attempt(trial["trial_id"], origin="工作台一键试写")
            else:
                created = self.create_trial(
                    table=str(entry.get("目标表", "")),
                    candidate_format=str(entry.get("表格式类型", "iceberg")),
                    write_mode=str(entry.get("写入模式", "append")),
                    merge_engine=str(entry.get("合并引擎", "copy-on-write")),
                    definition_id=self._auto_definition_id(entry),
                    config_entry_id=entry_id,
                    operator="工作台",
                    note="工作台一键试写",
                )
                if not created.get("ok"):
                    return None, created["reason"]
                result = self.execute_trial_attempt(
                    created["trial"]["trial_id"], origin="工作台一键试写")
            if not result.get("ok"):
                return None, result["reason"]
            attempt = result["attempt"]
            return entry, (
                f"第 {attempt['round']} 轮试写完成：通过 {attempt['passed_count']} 行、"
                f"拒绝 {attempt['rejected_count']} 行（{self._breakdown_text(attempt)}）"
            )

        trial = self._latest_trial_for_entry(entry)
        if trial is None:
            return None, f"表 {entry.get('目标表')} 还没有试写记录，请先提交试写"

        if action == "提交确认":
            result = self.submit_trial(trial["trial_id"])
        elif action == "确认生效":
            result = self.confirm_trial(trial["trial_id"])
        elif action == "退回重试":
            result = self.retry_trial(trial["trial_id"])
        else:
            result = self.abandon_trial(trial["trial_id"], reason="工作台放弃")
        if not result.get("ok"):
            return None, result["reason"]
        messages = {
            "提交确认": f"试写 {trial['trial_id']} 已提交确认，等待生效",
            "确认生效": f"试写 {trial['trial_id']} 已确认生效",
            "退回重试": f"已退回并登记新一轮试写 {result.get('trial', {}).get('trial_id')}",
            "放弃试写": f"试写 {trial['trial_id']} 已放弃",
        }
        return entry, messages[action]

    # ------------------------------------------------------------------
    # 视图与辅助
    # ------------------------------------------------------------------
    def trial_view(self, trial: dict[str, Any]) -> dict[str, Any]:
        attempts = [self.attempt_view(a) for a in store.trial_attempts(trial["trial_id"])]
        return {
            "trial_id": trial["trial_id"],
            "table": trial["table"],
            "candidate_format": trial["candidate_format"],
            "write_mode": trial["write_mode"],
            "merge_engine": trial["merge_engine"],
            "definition_id": trial["definition_id"],
            "definition_fingerprint": trial["definition_fingerprint"],
            "definition_revision": trial["definition_revision"],
            "schema_version": trial["schema_version"],
            "definition_fields": schema_field_names(trial.get("definition_snapshot")),
            "config_entry_id": trial.get("config_entry_id"),
            "status": trial["status"],
            "stage": trial["status"],
            "round": trial["round"],
            "release_id": trial.get("release_id"),
            "supersedes": trial.get("supersedes"),
            "retry_of": trial.get("retry_of") or trial.get("supersedes"),
            "conclusion": trial.get("conclusion"),
            "failure_reason": trial.get("failure_reason"),
            "baseline_release_id": (trial.get("baseline_strategy") or {}).get("release_id"),
            "attempts": attempts,
            "created_at": trial["created_at"],
            "updated_at": trial["updated_at"],
        }

    def attempt_view(self, attempt: dict[str, Any]) -> dict[str, Any]:
        return {
            "attempt_id": attempt["attempt_id"],
            "trial_id": attempt["trial_id"],
            "round": attempt.get("round"),
            "table": attempt["table"],
            "candidate_format": attempt["candidate_format"],
            "write_mode": attempt["write_mode"],
            "merge_engine": attempt["merge_engine"],
            "definition_id": attempt["definition_id"],
            "definition_fingerprint": attempt["definition_fingerprint"],
            "definition_revision": attempt["definition_revision"],
            "schema_version": attempt["schema_version"],
            "status": attempt["status"],
            "origin": attempt.get("origin"),
            "sample_count": attempt["sample_count"],
            "passed_count": attempt["passed_count"],
            "rejected_count": attempt["rejected_count"],
            "rejection_breakdown": attempt["rejection_breakdown"],
            "key_column": attempt.get("key_column"),
            "passed_rows": attempt["passed_rows"],
            "rejected_rows": attempt["rejected_rows"],
            "created_at": attempt["created_at"],
        }

    def release_view(self, release: dict[str, Any] | None) -> dict[str, Any] | None:
        if release is None:
            return None
        return {
            "release_id": release["release_id"],
            "table": release["table"],
            "format": release["format"],
            "write_mode": release["write_mode"],
            "merge_engine": release["merge_engine"],
            "definition_id": release["definition_id"],
            "definition_fingerprint": release["definition_fingerprint"],
            "definition_revision": release["definition_revision"],
            "schema_version": release["schema_version"],
            "trial_id": release["trial_id"],
            "config_entry_id": release.get("config_entry_id"),
            "status": release["status"],
            "conclusion": release.get("conclusion"),
            "evidence": release.get("evidence"),
            "reason": release.get("reason"),
            "effective": release.get("effective", False),
            "created_at": release["created_at"],
            "updated_at": release["updated_at"],
            "decided_at": release.get("decided_at"),
        }

    def _sync_release(
        self,
        trial: dict[str, Any],
        *,
        status: str,
        conclusion: str,
        evidence_extra: dict[str, Any] | None = None,
        reason: str | None = None,
        decided: bool = False,
    ) -> dict[str, Any] | None:
        release = store.release(trial.get("release_id")) if trial.get("release_id") else None
        if release is None:
            release = store.add_release({
                "table": trial["table"],
                "format": trial["candidate_format"],
                "write_mode": trial["write_mode"],
                "merge_engine": trial["merge_engine"],
                "definition_id": trial["definition_id"],
                "definition_fingerprint": trial["definition_fingerprint"],
                "definition_revision": trial["definition_revision"],
                "schema_version": trial["schema_version"],
                "trial_id": trial["trial_id"],
                "config_entry_id": trial.get("config_entry_id"),
                "status": status,
                "conclusion": conclusion,
                "evidence": {},
                "reason": reason,
                "effective": False,
                "decided_at": None,
            })
            trial["release_id"] = release["release_id"]
        attempts = store.trial_attempts(trial["trial_id"])
        latest = attempts[-1] if attempts else None
        release.update({
            "status": status,
            "conclusion": conclusion,
            "effective": status == RELEASE_PUBLISHED,
            "updated_at": time.time(),
        })
        if reason is not None:
            release["reason"] = reason
        if decided:
            release["decided_at"] = time.time()
        release["evidence"] = {
            "rounds": len(attempts),
            "latest_round": latest["attempt_id"] if latest else None,
            "latest_status": latest["status"] if latest else None,
            "passed_rows": latest["passed_count"] if latest else 0,
            "rejected_rows": latest["rejected_count"] if latest else 0,
            "rejection_breakdown": latest["rejection_breakdown"] if latest else {},
            "attempt_ids": [a["attempt_id"] for a in attempts],
            **(evidence_extra or {}),
        }
        return release

    def _resolve_trial_definition(
        self, table: str, definition_id: str | None
    ) -> dict[str, Any] | None:
        if definition_id:
            record = store.definition(definition_id)
            if record is None:
                return {"ok": False, "reason": f"字段定义 {definition_id} 不存在"}
            if record["status"] == "已废弃":
                return {"ok": False, "reason": f"字段定义 {definition_id} 已废弃，不能作为试写候选"}
            return record
        record = store.latest_definition(table) or store.current_definition_for_topic(table)
        if record is None:
            return {
                "ok": False,
                "reason": f"表 {table} 没有可用的字段定义，请先在 Schema 治理发布字段定义",
            }
        return record

    def _resolve_config(
        self, table: str, config_entry_id: int | None
    ) -> dict[str, Any] | None:
        if config_entry_id is not None:
            entry = store.find(self.MODULE, config_entry_id)
            if entry is None:
                return {"ok": False, "reason": f"表格式配置 {config_entry_id} 不存在"}
            if str(entry.get("目标表", "")) != table:
                return {
                    "ok": False,
                    "reason": f"配置 {config_entry_id} 的目标表是 {entry.get('目标表')}，与 {table} 不一致",
                }
            return entry
        for entry in store.rows(self.MODULE):
            if str(entry.get("目标表", "")) == table:
                return entry
        return None

    def _config_row(self, trial: dict[str, Any]) -> dict[str, Any] | None:
        entry_id = trial.get("config_entry_id")
        if entry_id is None:
            entry = self._resolve_config(trial["table"], None)
            return entry if isinstance(entry, dict) and "id" in entry else None
        return store.find(self.MODULE, int(entry_id))

    def _latest_trial_for_entry(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        table = str(entry.get("目标表", ""))
        trials = [t for t in store.trials(table)]
        if not trials:
            return None
        entry_id = int(entry.get("id", 0))
        matching = [t for t in trials if t.get("config_entry_id") == entry_id]
        pool = matching or trials
        return max(pool, key=lambda t: (t["created_at"], t["trial_id"]))

    def _latest_trial_for_entry_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        return self._latest_trial_for_entry(entry)

    def _auto_definition_id(self, entry: dict[str, Any]) -> str | None:
        table = str(entry.get("目标表", ""))
        record = store.latest_definition(table) or store.current_definition_for_topic(table)
        if record is not None:
            return record["definition_id"]
        # 没有任何字段定义时，用表配置里的键列生成一份最小定义，保证试写链路可走通
        fields = []
        for column, label in (("主键列", "id"), ("分区列", "partition"), ("排序键列", "sort_key")):
            name = str(entry.get(column) or "").strip()
            if name:
                fields.append({"name": name, "type": "string", "nullable": False})
        if not fields:
            fields = [{"name": f"{slugify(table).lower()}_id", "type": "string", "nullable": False}]
        record = store.register_definition(
            topic=table,
            definition={"fields": fields, "version": "v1-auto"},
            version="v1-auto",
            source_entry_id=int(entry.get("id", 0)),
            status="候选",
            reason="工作台按表配置自动生成的候选定义",
        )
        return record["definition_id"]

    def _key_column(self, trial: dict[str, Any]) -> str:
        entry = self._config_row(trial)
        key = str(entry.get("主键列", "") if entry else "").strip()
        if key:
            return key
        for name in schema_field_names(trial.get("definition_snapshot")):
            if name.lower() in ("id", "order_id", "trade_id") or name.lower().endswith("_id"):
                return name
        return ""

    def _default_samples(
        self, specs: dict[str, dict[str, Any]], table: str
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index in range(3):
            row: dict[str, Any] = {}
            for name, spec in specs.items():
                row[name] = self._sample_value(name, spec, index, table)
            rows.append(row)
        return rows

    def _sample_value(
        self, name: str, spec: dict[str, Any], index: int, table: str
    ) -> Any:
        field_type = str(spec.get("type", "string")).lower()
        scale = int(spec.get("scale") or 0)
        if field_type in ("decimal", "numeric"):
            return f"{100 + index}.{'0' * scale}" if scale else str(100 + index)
        if field_type in ("int", "integer", "bigint", "long", "number"):
            return 100 + index
        if field_type in ("float", "double"):
            return 100.5 + index
        if field_type in ("boolean", "bool"):
            return index % 2 == 0
        if field_type in ("timestamp", "datetime"):
            return f"2026-11-0{index + 1}T00:00:00"
        if field_type == "date":
            return f"2026-11-0{index + 1}"
        lowered = name.lower()
        if lowered.endswith("_id") or lowered == "id":
            return f"{slugify(table)}-{index + 1:04d}"
        return f"{name}_{index + 1}"

    def _attempt_conclusion(self, attempt: dict[str, Any]) -> str:
        return (
            f"第 {attempt['round']} 轮试写（{attempt['candidate_format']}/{attempt['write_mode']}，"
            f"字段定义 {attempt['definition_id']}）：通过 {attempt['passed_count']} 行、"
            f"拒绝 {attempt['rejected_count']} 行（{self._breakdown_text(attempt)}）"
        )

    def _breakdown_text(self, attempt: dict[str, Any]) -> str:
        breakdown = attempt.get("rejection_breakdown") or {}
        if not breakdown:
            return "无拒绝"
        return "、".join(f"{kind} {count} 行" for kind, count in sorted(breakdown.items()))

    def _attempt_summary(self, attempt: dict[str, Any]) -> dict[str, Any]:
        return {
            "attempt_id": attempt["attempt_id"],
            "round": attempt["round"],
            "status": attempt["status"],
            "passed_count": attempt["passed_count"],
            "rejected_count": attempt["rejected_count"],
            "definition_id": attempt["definition_id"],
            "definition_revision": attempt["definition_revision"],
            "sample_count": attempt["sample_count"],
        }

    def _definition_summary(self, record: dict[str, Any] | None) -> dict[str, Any] | None:
        if record is None:
            return None
        names = schema_field_names(record["definition"])
        return {
            "definition_id": record["definition_id"],
            "topic": record["topic"],
            "schema_version": record["schema_version"],
            "revision": record["revision"],
            "fingerprint": record["fingerprint"],
            "status": record["status"],
            "fields": names,
            "field_count": len(names),
            "specs": schema_field_specs(record["definition"]),
        }

    def _strategy_summary(self, live: dict[str, Any] | None) -> dict[str, Any] | None:
        if live is None:
            return None
        return {
            "table": live["table"],
            "format": live["format"],
            "write_mode": live["write_mode"],
            "merge_engine": live["merge_engine"],
            "definition_id": live["definition_id"],
            "release_id": live.get("release_id"),
            "confirmed_at": live.get("confirmed_at"),
        }

    def _lineage_counts(self, table_name: str) -> dict[str, int]:
        upstream_count = sum(
            1 for edges in store._lineage_graph.values()
            for e in edges if e.get("downstream") == table_name
        )
        downstream_count = len(store.lineage_downstream(table_name))
        return {
            "lineage_upstream_count": upstream_count,
            "lineage_downstream_count": downstream_count,
        }

    def definition_references(self, definition_id: str) -> dict[str, Any]:
        return store.definition_references(definition_id)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_configs"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["trials_in_flight"] = sum(1 for t in store.trials() if t["status"] in TRIAL_ACTIVE)
        base["trials_failed"] = sum(1 for t in store.trials() if t["status"] == TRIAL_FAILED)
        base["releases_published"] = sum(
            1 for r in store.releases() if r["status"] == RELEASE_PUBLISHED)
        base["confirmed_strategies"] = len(store._confirmed_strategies)
        base["conflicts"] = len(store.conflicts())
        return base
