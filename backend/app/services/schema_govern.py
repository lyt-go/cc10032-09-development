"""schema_govern 领域服务：Schema治理、字段定义版本与引用保护。

业务逻辑：
1. Schema 注册、版本号递增、兼容性校验（类型/精度变化按字段规格判断）
2. 字段定义版本：每次字段变更登记一个不可变版本（DEF-xxxxxx），并推进主题 revision
3. 引用保护：在途试写、待确认发布、线上已确认组合都会保护字段定义不被废弃
4. 废弃申请：被引用时拒绝并说明原因；取消在途试写后才允许废弃
5. 与上下游流水线：消费 change_capture 变更；为 table_format 提供候选字段定义

跨模块数据流：
  change_capture → schema_govern（CDC变更驱动Schema演进）
  schema_govern → table_format（字段定义版本作为试写依据与生效依据）
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import (
    evaluate_row,
    schema_field_names,
    schema_field_specs,
    store,
)


class SchemaGovernService(BaseModuleService):
    MODULE = "schema_govern"
    ENTITY = "Schema版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["主题名称", "Schema定义", "版本号", "兼容策略", "注册时间", "序列化格式",
                   "字段列表", "演进历史", "字段定义编号"]
    REQUIRED_FIELDS = ["主题名称", "Schema定义", "版本号"]
    STATUS_ORDER = ["草稿", "待校验", "已注册", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "确认注册": "已注册", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "schema_evolution"
    UPSTREAM_MODULES = ["change_capture"]
    DOWNSTREAM_MODULES = ["table_format"]
    PRODUCES_ARTIFACT = "Schema演进版本链"

    # ------------------------------------------------------------------
    # 定义登记 / 发布 / 修订
    # ------------------------------------------------------------------
    def publish_definition(
        self,
        *,
        topic: str,
        definition: dict[str, Any],
        version: str,
        source_entry_id: int | None = None,
        status: str = "候选",
        replace_definition_id: str | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """登记字段定义版本。相同内容复用已有版本，内容变化才推进 revision。"""
        if not topic:
            return {"ok": False, "reason": "主题名称为空，无法登记字段定义"}
        specs = schema_field_specs(definition)
        if not specs:
            return {"ok": False, "reason": "字段定义为空，无法登记"}
        record = store.register_definition(
            topic=topic,
            definition=definition,
            version=version,
            source_entry_id=source_entry_id,
            status=status,
            replace_definition_id=replace_definition_id,
            reason=reason,
        )
        return {"ok": True, "definition": self.definition_view(record)}

    def publish_from_entry(self, entry_id: int) -> dict[str, Any]:
        """把某条 Schema 版本落成字段定义，并回填字段定义编号。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}
        if entry.get("status") != "已注册":
            return {"ok": False, "reason": f"Schema 未注册，当前状态: {entry.get('status')}"}
        definition = self._definition_from_entry(entry)
        if definition is None:
            return {"ok": False, "reason": "Schema定义无法解析为字段列表"}

        previous_id = entry.get("字段定义编号")
        result = self.publish_definition(
            topic=str(entry.get("主题名称", "")),
            definition=definition,
            version=str(entry.get("版本号", "v1")),
            source_entry_id=entry_id,
            status="已确认",
            replace_definition_id=previous_id if previous_id else None,
            reason="Schema注册发布",
        )
        if not result["ok"]:
            return result
        self._bind_entry(entry, result["definition"])
        return {"ok": True, "entry_id": entry_id, "definition": result["definition"]}

    def amend_definition(
        self,
        definition_id: str,
        changes: list[dict[str, Any]] | dict[str, Any] | None = None,
        *,
        definition: dict[str, Any] | None = None,
        version: str | None = None,
        reason: str = "",
        source_entry_id: int | None = None,
    ) -> dict[str, Any]:
        """在已有字段定义上改字段（例如调整金额列精度），生成新 revision。

        这一步会推进主题 revision，让基于旧 revision 的待确认试写在确认时被拒绝，
        从而保证“字段发布与候选确认并发”时只有依据完整的组合生效。
        """
        base = store.definition(definition_id)
        if base is None:
            return {"ok": False, "reason": f"字段定义 {definition_id} 不存在"}
        if base["status"] == "已废弃":
            return {"ok": False, "reason": f"字段定义 {definition_id} 已废弃，不能继续修订"}

        base_specs = schema_field_specs(base["definition"])
        new_specs = {name: dict(spec) for name, spec in base_specs.items()}
        new_definition = definition
        if definition is None and changes:
            change_list = [changes] if isinstance(changes, dict) else changes
            for change in change_list:
                field_name = str(change.get("field") or change.get("name") or "").strip()
                if not field_name:
                    return {"ok": False, "reason": "字段变更缺少 field 名称"}
                if field_name not in new_specs:
                    return {"ok": False, "reason": f"字段 {field_name} 不在定义 {definition_id} 中"}
                spec = new_specs[field_name]
                for key in ("type", "precision", "scale", "nullable", "required"):
                    if key in change:
                        spec[key] = change[key]
                if "precision" in change or "scale" in change:
                    spec["type"] = "decimal"
                new_specs[field_name] = spec
            new_definition = {"fields": list(new_specs.values())}
        elif definition is None:
            new_definition = {"fields": list(new_specs.values())}

        # 兼容性：不允许删除字段，不允许把必填字段改为可选
        candidate_specs = schema_field_specs(new_definition)
        for name, old_spec in base_specs.items():
            if name not in candidate_specs:
                return {"ok": False, "reason": f"修订删除字段 {name}，不向后兼容"}
            if old_spec.get("required") and not candidate_specs[name].get("required"):
                return {"ok": False, "reason": f"字段 {name} 从必填改为可选，需走回退流程"}

        new_version = version or self._next_version(base.get("schema_version", "v1"))
        result = self.publish_definition(
            topic=base["topic"],
            definition=new_definition,
            version=new_version,
            source_entry_id=source_entry_id or base.get("source_entry_id"),
            status="候选",
            replace_definition_id=definition_id,
            reason=reason or "字段定义修订",
        )
        if not result["ok"]:
            return result
        new_record = store.definition(result["definition"]["definition_id"])
        assert new_record is not None
        if new_record["definition_id"] == definition_id:
            return {
                "ok": False,
                "reason": "修订内容与原定义完全一致，未产生新版本",
                "definition": self.definition_view(base),
            }
        return {
            "ok": True,
            "previous_definition_id": definition_id,
            "definition": self.definition_view(new_record),
            "revision_bumped": new_record["revision"] > base["revision"],
            "stale_trials": self._stale_trial_ids(base["topic"], new_record["revision"]),
        }

    # ------------------------------------------------------------------
    # 兼容性校验（原链路保留，补上字段规格）
    # ------------------------------------------------------------------
    def validate_compatibility(self, entry_id: int, new_field_spec: dict[str, Any]) -> dict[str, Any]:
        """校验新Schema版本与已注册版本的兼容性。

        失败场景：
        - 字段类型变更 → 不向后兼容
        - 精度/小数位收缩 → 可能截断
        - 删除已有字段 → 破坏性变更
        - 重复版本号 → 拒绝注册
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}

        topic = str(entry.get("主题名称", ""))
        versions = store.schema_versions(topic)
        version_number = new_field_spec.get("version", "v1")

        for v in versions:
            if v.get("version") == version_number:
                return {"ok": False, "reason": f"版本号 {version_number} 已存在，请使用新版本号"}

        fields = new_field_spec.get("fields", [])
        names = schema_field_names({"fields": fields})
        result = store.check_schema_compatibility(topic, names)
        if not result["compatible"]:
            entry["abnormal"] = True
            entry["status"] = "待校验"
            return {"ok": False, "reason": f"兼容性校验失败: {result['reason']}", "old_fields": result}

        previous = self._previous_definition(entry)
        if previous is not None:
            conflict = self._spec_compatibility(previous["definition"], new_field_spec)
            if conflict:
                entry["abnormal"] = True
                entry["status"] = "待校验"
                return {"ok": False, "reason": f"兼容性校验失败: {conflict}", "definition_id": previous["definition_id"]}

        return {
            "ok": True,
            "topic": topic,
            "version": version_number,
            "fields": names,
            "compatibility": "BACKWARD",
            "definition_id": previous["definition_id"] if previous else None,
        }

    def _spec_compatibility(
        self, old_definition: dict[str, Any], new_spec: dict[str, Any]
    ) -> str | None:
        old_specs = schema_field_specs(old_definition)
        new_specs = schema_field_specs({"fields": new_spec.get("fields", [])})
        for name, old in old_specs.items():
            new = new_specs.get(name)
            if new is None:
                return f"缺少已有字段 {name}"
            old_type = str(old.get("type", "string")).lower()
            new_type = str(new.get("type", "string")).lower()
            if old_type != new_type:
                return f"字段 {name} 类型由 {old_type} 变为 {new_type}，不向后兼容"
            if old_type in ("decimal", "numeric"):
                old_precision = int(old.get("precision") or 38)
                old_scale = int(old.get("scale") or 0)
                new_precision = int(new.get("precision") or 38)
                new_scale = int(new.get("scale") or 0)
                if new_precision < old_precision or new_scale < old_scale:
                    return (
                        f"字段 {name} 精度由 decimal({old_precision},{old_scale}) "
                        f"收缩为 decimal({new_precision},{new_scale})，可能截断历史数据"
                    )
        return None

    def evolve_schema(self, entry_id: int, change_events: list[dict[str, Any]]) -> dict[str, Any]:
        """基于CDC变更事件驱动Schema演进，并登记新的字段定义版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"Schema {entry_id} 不存在"}
        if entry.get("status") != "已注册":
            return {"ok": False, "reason": f"Schema未注册，当前状态: {entry.get('status')}"}

        topic = str(entry.get("主题名称", ""))
        current_version = str(entry.get("版本号", "v1"))
        next_version = self._next_version(current_version)

        old_definition = self._definition_from_entry(entry) or {"fields": []}
        new_specs = schema_field_specs(old_definition)
        added: list[str] = []
        for event in change_events:
            if isinstance(event, dict) and event.get("type") == "INSERT":
                field_name = "new_field_from_cdc"
                if field_name not in new_specs:
                    new_specs[field_name] = {
                        "name": field_name, "type": "string", "nullable": True, "required": False,
                    }
                    added.append(field_name)

        schema_def = {"fields": list(new_specs.values()), "version": next_version}
        compat = self.validate_compatibility(entry_id, schema_def)
        if not compat["ok"]:
            return compat

        store.register_schema(topic, schema_def, next_version)

        new_version = dict(entry)
        new_version["id"] = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        new_version["版本号"] = next_version
        new_version["Schema定义"] = str(schema_def)
        new_version["字段列表"] = ",".join(schema_field_names(schema_def))
        new_version["演进历史"] = f"{current_version}→{next_version}: " + (
            f"新增字段 {added}" if added else "无字段变化"
        )
        new_version["status"] = "草稿"
        new_version["pending"] = True
        new_version["abnormal"] = False
        store.rows(self.MODULE).append(new_version)

        published = self.publish_definition(
            topic=topic,
            definition=schema_def,
            version=next_version,
            source_entry_id=new_version["id"],
            status="候选",
            replace_definition_id=entry.get("字段定义编号"),
            reason="CDC演进发布",
        )
        if published["ok"]:
            self._bind_entry(new_version, published["definition"])

        batch_id = f"SCHEMA_EVOLVE_{entry_id}_{int(time.time())}"
        self.produce_artifact([schema_def], batch_id)

        return {
            "ok": True,
            "topic": topic,
            "old_version": current_version,
            "new_version": next_version,
            "new_fields": sorted(new_specs),
            "added_fields": added,
            "definition": published.get("definition"),
            "source": "change_capture_events",
        }

    # ------------------------------------------------------------------
    # 废弃申请与引用保护
    # ------------------------------------------------------------------
    def request_deprecation(
        self,
        definition_id: str,
        *,
        reason: str = "",
        cancel_trials: bool = False,
    ) -> dict[str, Any]:
        """治理入口：申请废弃字段定义。

        被未完成试写、待确认发布或线上组合引用时拒绝，并逐条说明引用来源；
        cancel_trials=True 会先取消在途试写，成功后才允许废弃。
        """
        record = store.definition(definition_id)
        if record is None:
            return {"ok": False, "reason": f"字段定义 {definition_id} 不存在", "definition_id": definition_id}

        references = store.definition_references(definition_id)
        blocking = (
            sum(len(v) for v in references.values())
        )
        if blocking and not cancel_trials:
            return {
                "ok": False,
                "protected": True,
                "definition_id": definition_id,
                "definition_status": record["status"],
                "reason": self._protection_reason(references),
                "references": references,
                "suggested_action": "先取消引用该定义的在途试写，再重新申请废弃",
                "can_force": bool(references["in_flight_trials"] or references["pending_releases"]),
                "live_protected": bool(references["live_strategies"]),
            }

        if references["live_strategies"]:
            tables = "、".join(s["table"] for s in references["live_strategies"])
            return {
                "ok": False,
                "protected": True,
                "definition_id": definition_id,
                "reason": (
                    f"字段定义仍被线上已确认写入组合引用（{tables}），不能直接废弃；"
                    "请先在表格式确认新的组合"
                ),
                "references": references,
                "live_protected": True,
            }

        result = store.deprecate_definition(
            definition_id, reason=reason or "治理入口申请废弃", cancel_trials=cancel_trials)
        if not result["ok"]:
            return result

        deprecated = result["definition"]
        for entry in store.rows(self.MODULE):
            if entry.get("字段定义编号") == definition_id:
                entry["status"] = "已废弃"
                entry["pending"] = False
                entry["abnormal"] = False
                entry["演进历史"] = (
                    f"{entry.get('演进历史') or ''}；字段定义 {definition_id} 废弃"
                ).strip("；")
        return {
            "ok": True,
            "definition_id": definition_id,
            "definition": self.definition_view(deprecated),
            "cancelled_trials": result.get("cancelled_trials", []),
            "references_before": result.get("references_before", references),
            "note": (
                "在途试写已取消，字段定义已废弃"
                if cancel_trials and result.get("cancelled_trials")
                else "字段定义已废弃"
            ),
        }

    def definition_references(self, definition_id: str) -> dict[str, Any]:
        record = store.definition(definition_id)
        if record is None:
            return {"ok": False, "reason": f"字段定义 {definition_id} 不存在"}
        references = store.definition_references(definition_id)
        return {
            "ok": True,
            "definition_id": definition_id,
            "definition_status": record["status"],
            "protected": any(references.values()),
            "protection_reason": self._protection_reason(references) if any(references.values()) else "",
            "references": references,
        }

    def _protection_reason(self, references: dict[str, list[dict[str, Any]]]) -> str:
        parts = []
        if references["in_flight_trials"]:
            trials = "、".join(
                f"{t['trial_id']}({t['table']}·{t['status']})" for t in references["in_flight_trials"]
            )
            parts.append(f"被 {len(references['in_flight_trials'])} 条在途试写引用：{trials}")
        if references["pending_releases"]:
            releases = "、".join(
                f"{r['release_id']}({r['table']}·{r['status']})" for r in references["pending_releases"]
            )
            parts.append(f"被 {len(references['pending_releases'])} 条格式发布清单引用：{releases}")
        if references["live_strategies"]:
            tables = "、".join(s["table"] for s in references["live_strategies"])
            parts.append(f"仍被线上写入组合使用：{tables}")
        return "；".join(parts) if parts else "无在途引用"

    # ------------------------------------------------------------------
    # 查询视图
    # ------------------------------------------------------------------
    def list_definitions(self, topic: str | None = None, status: str | None = None) -> dict[str, Any]:
        rows = [self.definition_view(d) for d in store.definitions(topic=topic, status=status)]
        for row in rows:
            refs = store.definition_references(row["definition_id"])
            row["reference_count"] = sum(len(v) for v in refs.values())
            row["in_flight_trials"] = [t["trial_id"] for t in refs["in_flight_trials"]]
            row["live_tables"] = [s["table"] for s in refs["live_strategies"]]
        return {"ok": True, "items": rows, "total": len(rows), "topic_revisions": dict(store._topic_revisions)}

    def definition_view(self, record: dict[str, Any]) -> dict[str, Any]:
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
            "definition": record["definition"],
            "source_entry_id": record.get("source_entry_id"),
            "replaced_definition_id": record.get("replaced_definition_id"),
            "replaced_by": record.get("replaced_by"),
            "change_reason": record.get("change_reason"),
            "created_at": record["created_at"],
            "deprecated_at": record.get("deprecated_at"),
            "deprecate_reason": record.get("deprecate_reason"),
        }

    def emit_schema_for_table_format(self, topic: str) -> dict[str, Any]:
        """向 table_format 暴露当前选定字段定义（没有则回退到最新候选）。"""
        record = store.current_definition_for_topic(topic) or store.latest_definition(topic)
        if record is None:
            versions = store.schema_versions(topic)
            if not versions:
                return {"ok": False, "reason": f"主题 {topic} 无已注册Schema"}
            latest = versions[-1]
            return {
                "ok": True,
                "topic": topic,
                "latest_version": latest.get("version"),
                "definition": latest.get("definition"),
                "fields": schema_field_names(latest.get("definition")),
                "definition_id": None,
                "downstream_consumer": "table_format",
            }
        return {
            "ok": True,
            "topic": topic,
            "latest_version": record["schema_version"],
            "definition": record["definition"],
            "fields": schema_field_names(record["definition"]),
            "definition_id": record["definition_id"],
            "revision": record["revision"],
            "status": record["status"],
            "downstream_consumer": "table_format",
        }

    def sample_definition_check(
        self, definition_id: str, samples: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """用一份字段定义直接检查样本，便于治理入口先看类型/缺列问题。"""
        record = store.definition(definition_id)
        if record is None:
            return {"ok": False, "reason": f"字段定义 {definition_id} 不存在"}
        specs = schema_field_specs(record["definition"])
        passed = 0
        rejected = []
        for index, row in enumerate(samples):
            issues = evaluate_row(row, specs) if isinstance(row, dict) else [
                {"field": "-", "kind": "类型", "detail": "样本不是行对象"}
            ]
            if issues:
                rejected.append({"row_index": index, "row": row, "issues": issues})
            else:
                passed += 1
        return {
            "ok": True,
            "definition_id": definition_id,
            "sample_count": len(samples),
            "passed_count": passed,
            "rejected_count": len(rejected),
            "rejected_rows": rejected,
        }

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------
    def _definition_from_entry(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        for key in ("字段定义", "Schema定义"):
            raw = entry.get(key)
            if not raw:
                continue
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(parsed, dict) and parsed.get("fields"):
                return parsed
        return None

    def _bind_entry(self, entry: dict[str, Any], definition: dict[str, Any]) -> None:
        entry["字段定义编号"] = definition["definition_id"]
        entry["字段定义"] = json.dumps(definition["definition"], ensure_ascii=False)
        entry["字段列表"] = ",".join(definition["fields"])

    def _previous_definition(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        definition_id = entry.get("字段定义编号")
        if definition_id:
            record = store.definition(str(definition_id))
            if record is not None:
                return record
        topic = str(entry.get("主题名称", ""))
        return store.current_definition_for_topic(topic) or store.latest_definition(topic)

    def _next_version(self, current: str) -> str:
        digits = "".join(ch for ch in str(current) if ch.isdigit())
        return f"v{int(digits or 1) + 1}"

    def _stale_trial_ids(self, topic: str, new_revision: int) -> list[str]:
        return [
            t["trial_id"]
            for t in store.trials()
            if t["definition_topic"] == topic
            and t["status"] in ("草稿", "试写中", "待确认")
            and t["definition_revision"] < new_revision
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["registry_topics"] = len(store._schema_registry)
        base["definitions"] = len(store.definitions())
        base["deprecated_definitions"] = len(store.definitions(status="已废弃"))
        base["protected_definitions"] = sum(
            1 for d in store.definitions()
            if any(store.definition_references(d["definition_id"]).values())
        )
        return base
