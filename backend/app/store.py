"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from app.seed import SEED_ROWS


def slugify(text: str) -> str:
    """把表名/主题名压成可读的编号片段，例如 dwd.orders → DWD-ORDERS。"""
    cleaned = re.sub(r"[^0-9A-Za-z一-龥]+", "-", str(text)).strip("-")
    return (cleaned or "DEF").upper()


def schema_field_names(definition: dict[str, Any] | None) -> list[str]:
    """兼容两种 Schema 定义：["id", ...] 与 [{"name": "id", ...}]。"""
    if not definition:
        return []
    names: list[str] = []
    for field in definition.get("fields", []) or []:
        if isinstance(field, dict):
            name = field.get("name")
        else:
            name = field
        if name is not None:
            names.append(str(name))
    return names


def schema_field_specs(definition: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """把 Schema 定义归一化成 {字段名: 规格}。

    旧数据只给了字段名，按可选字符串处理；新定义的字段带 type/precision/scale/nullable，
    试写校验依据这里的规格判断“类型不符”和“缺列”。
    """
    specs: dict[str, dict[str, Any]] = {}
    if not definition:
        return specs
    for field in definition.get("fields", []) or []:
        if isinstance(field, dict):
            spec = dict(field)
            name = spec.get("name")
            spec.setdefault("type", "string")
            spec.setdefault("nullable", True)
            spec.setdefault("required", not bool(spec.get("nullable", True)))
        else:
            name = field
            spec = {"name": str(field), "type": "string", "nullable": True, "required": False}
        if name is not None:
            specs[str(name)] = spec
    return specs


def schema_fingerprint(definition: dict[str, Any] | None) -> str:
    """字段定义指纹：字段名、类型、精度、可空性任一变化都会变。"""
    specs = schema_field_specs(definition)
    canonical = {
        name: {
            "type": str(spec.get("type", "string")).lower(),
            "precision": spec.get("precision"),
            "scale": spec.get("scale"),
            "nullable": bool(spec.get("nullable", True)),
            "required": bool(spec.get("required", False)),
        }
        for name, spec in sorted(specs.items())
    }
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _decimal_scale(value: Any) -> tuple[Decimal | None, str | None]:
    """把金额类取值转成 Decimal，同时返回失败原因。"""
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None, f"无法解析为数值: {value!r}"
    if not number.is_finite():
        return None, f"非有限数值: {value!r}"
    return number, None


def _fits_decimal(value: Any, precision: int, scale: int) -> tuple[bool, str]:
    number, error = _decimal_scale(value)
    if error:
        return False, error
    assert number is not None
    exponent = number.as_tuple().exponent
    fraction_digits = max(-exponent, 0)
    if fraction_digits > scale:
        return False, f"小数位 {fraction_digits} 超过允许 {scale}"
    integer_digits = max(len(number.as_tuple().digits) - fraction_digits, 0)
    if integer_digits > precision - scale:
        return False, f"整数位 {integer_digits} 超过允许 {precision - scale}"
    return True, ""


def check_value_against_spec(value: Any, spec: dict[str, Any]) -> str | None:
    """按字段规格校验单个值，返回拒绝原因或 None。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        if spec.get("required") or not bool(spec.get("nullable", True)):
            return "空值但字段非空"
        return None
    field_type = str(spec.get("type", "string")).lower()
    if field_type in ("decimal", "numeric", "number", "bigint", "int", "integer", "long"):
        if field_type in ("decimal", "numeric"):
            precision = int(spec.get("precision") or 38)
            scale = int(spec.get("scale") or 0)
            ok, reason = _fits_decimal(value, precision, scale)
            return None if ok else f"数值越界: {reason}"
        number, error = _decimal_scale(value)
        if error:
            return error
        assert number is not None
        if number != number.to_integral_value():
            return f"类型不符: {value!r} 不是整数"
        return None
    if field_type in ("float", "double"):
        _, error = _decimal_scale(value)
        return error
    if field_type in ("boolean", "bool"):
        if isinstance(value, bool) or str(value).lower() in ("true", "false", "0", "1"):
            return None
        return f"类型不符: {value!r} 不是布尔值"
    if field_type == "string":
        return None
    # 未识别的类型按字符串放行，避免误伤旧定义
    return None


def evaluate_row(
    row: dict[str, Any], specs: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """对照字段定义检查一行样本。

    检查顺序：缺列（必填字段缺失）→ 类型/精度拒绝。
    返回问题清单，空清单代表这一行可以通过试写；未定义的额外列不阻塞写入。
    """
    issues: list[dict[str, Any]] = []
    for name, spec in specs.items():
        if name not in row:
            if spec.get("required") or not bool(spec.get("nullable", True)):
                issues.append({"field": name, "kind": "缺列", "detail": "样本缺少必填字段"})
            continue
        reason = check_value_against_spec(row.get(name), spec)
        if reason:
            issues.append({
                "field": name,
                "kind": "缺列" if "缺少" in reason else "类型",
                "detail": reason,
            })
    return issues


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

        # ---- 格式试写 / 格式发布清单 / 已确认写入组合 ----
        # 一次“换格式”不再直接改线上策略：先对候选格式+字段定义做试写，
        # 试写结论回写发布清单，确认后才替换已确认组合。
        self._lock = threading.RLock()
        self._definitions: list[dict[str, Any]] = []
        self._definition_index: dict[str, dict[str, Any]] = {}
        self._topic_revisions: dict[str, int] = {}
        self._trials: list[dict[str, Any]] = []
        self._trial_writes: list[dict[str, Any]] = []       # 实际写入尝试
        self._format_releases: list[dict[str, Any]] = []    # 格式发布清单
        self._confirmed_strategies: dict[str, dict[str, Any]] = {}
        self._conflicts: list[dict[str, Any]] = []          # 版本冲突回查
        self._definition_seq = 0
        self._trial_seq = 0
        self._attempt_seq = 0
        self._release_seq = 0
        self._bootstrap_format_governance()

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(schema_field_names(latest.get("definition", {})))
        missing_required = [f for f in old_fields if f not in set(new_fields)]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]

    # ---------- 字段定义版本（schema_govern → table_format 的候选来源） ----------
    def _bootstrap_format_governance(self) -> None:
        """把示例 Schema 版本登记成字段定义，并为已生效表格式建立初始已确认组合。"""
        for row in self.rows("schema_govern"):
            definition = self._definition_from_schema_row(row)
            if definition is None:
                continue
            status = "已确认" if row.get("status") == "已注册" else "候选"
            definition_id = self.register_definition(
                topic=str(row.get("主题名称", "")),
                definition=definition,
                version=str(row.get("版本号", "v1")),
                source_entry_id=int(row.get("id", 0)),
                status=status,
            )["definition_id"]
            row["字段定义编号"] = definition_id
            row["字段定义"] = json.dumps(definition, ensure_ascii=False)

        for row in self.rows("table_format"):
            if row.get("status") != "已生效":
                continue
            table = str(row.get("目标表", ""))
            definition = self.current_definition(table) or self.current_definition_for_topic(table)
            if not definition:
                continue
            self._confirmed_strategies[table] = {
                "table": table,
                "format": str(row.get("表格式类型", "iceberg")).lower(),
                "write_mode": str(row.get("写入模式", "append")),
                "merge_engine": str(row.get("合并引擎", "copy-on-write")),
                "definition_id": definition["definition_id"],
                "definition_fingerprint": definition["fingerprint"],
                "definition_revision": definition["revision"],
                "schema_version": definition["schema_version"],
                "config_entry_id": int(row.get("id", 0)),
                "release_id": None,
                "confirmed_at": time.time(),
                "previous": None,
                "source": "seed",
            }

    def _definition_from_schema_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        raw = row.get("字段定义")
        if raw:
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(parsed, dict) and parsed.get("fields"):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
        raw_schema = row.get("Schema定义")
        if not raw_schema:
            return None
        try:
            parsed = json.loads(raw_schema) if isinstance(raw_schema, str) else raw_schema
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(parsed, dict) or not parsed.get("fields"):
            return None
        return parsed

    def register_definition(
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
        """登记一份不可变的字段定义版本。

        重复登记同一指纹（同一主题、同一字段组合）时复用已有版本，避免试写依据被无意义替换；
        每次真正的内容变化都会推进该主题的 revision，供并发确认做乐观校验。
        """
        with self._lock:
            fingerprint = schema_fingerprint(definition)
            for existing in self._definitions:
                if (
                    existing["topic"] == topic
                    and existing["fingerprint"] == fingerprint
                    and existing["status"] != "已废弃"
                ):
                    if source_entry_id and not existing.get("source_entry_id"):
                        existing["source_entry_id"] = source_entry_id
                    return existing

            self._definition_seq += 1
            revision = self._topic_revisions.get(topic, 0) + 1
            self._topic_revisions[topic] = revision
            record = {
                "definition_id": f"DEF-{self._definition_seq:06d}",
                "topic": topic,
                "schema_version": version,
                "source_entry_id": source_entry_id,
                "definition": json.loads(json.dumps(definition, ensure_ascii=False)),
                "fingerprint": fingerprint,
                "revision": revision,
                "status": status,
                "created_at": time.time(),
                "deprecated_at": None,
                "deprecate_reason": None,
                "replaced_definition_id": replace_definition_id,
                "change_reason": reason,
            }
            self._definitions.append(record)
            self._definition_index[record["definition_id"]] = record
            if replace_definition_id and replace_definition_id in self._definition_index:
                old = self._definition_index[replace_definition_id]
                old["replaced_by"] = record["definition_id"]
            return record

    def definitions(self, topic: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._definitions)
        if topic:
            rows = [r for r in rows if r["topic"] == topic]
        if status:
            rows = [r for r in rows if r["status"] == status]
        return rows

    def definition(self, definition_id: str) -> dict[str, Any] | None:
        return self._definition_index.get(definition_id)

    def current_definition(self, table: str) -> dict[str, Any] | None:
        """已确认组合里选定的字段定义。"""
        strategy = self._confirmed_strategies.get(table)
        if strategy and strategy.get("definition_id"):
            record = self._definition_index.get(strategy["definition_id"])
            if record and record["status"] == "已确认":
                return record
        return None

    def current_definition_for_topic(self, topic: str) -> dict[str, Any] | None:
        candidates = [d for d in self._definitions if d["topic"] == topic and d["status"] == "已确认"]
        if candidates:
            return max(candidates, key=lambda d: d["revision"])
        candidates = [d for d in self._definitions if d["topic"] == topic and d["status"] != "已废弃"]
        return max(candidates, key=lambda d: d["revision"]) if candidates else None

    def latest_definition(self, topic: str) -> dict[str, Any] | None:
        candidates = [d for d in self._definitions if d["topic"] == topic and d["status"] != "已废弃"]
        return max(candidates, key=lambda d: d["revision"]) if candidates else None

    def topic_revision(self, topic: str) -> int:
        return self._topic_revisions.get(topic, 0)

    def definition_references(self, definition_id: str) -> dict[str, list[dict[str, Any]]]:
        """统计一份字段定义仍被哪些在途流程引用。

        在途口径：未结束的试写（草稿/试写中/待确认）、待确认的格式发布、以及线上已确认组合。
        历史上已发布但已被替换的发布清单不再构成阻塞，否则旧定义永远无法清理。
        """
        active_trial_status = {"草稿", "试写中", "待确认"}
        trials = [
            {"trial_id": t["trial_id"], "table": t["table"], "status": t["status"], "round": t["round"]}
            for t in self._trials
            if t["definition_id"] == definition_id and t["status"] in active_trial_status
        ]
        live_release_ids = {
            s.get("release_id") for s in self._confirmed_strategies.values() if s.get("release_id")
        }
        releases = [
            {"release_id": r["release_id"], "table": r["table"], "status": r["status"]}
            for r in self._format_releases
            if r["definition_id"] == definition_id
            and (
                r["status"] == "待确认"
                or (r["status"] == "已发布" and r["release_id"] in live_release_ids)
            )
        ]
        strategies = [
            {"table": table, "release_id": s.get("release_id")}
            for table, s in self._confirmed_strategies.items()
            if s.get("definition_id") == definition_id
        ]
        return {
            "in_flight_trials": trials,
            "pending_releases": releases,
            "live_strategies": strategies,
        }

    def deprecate_definition(
        self, definition_id: str, *, reason: str = "", cancel_trials: bool = False
    ) -> dict[str, Any]:
        """作废候选定义；被在途试写/线上组合引用时必须先取消或拒绝。"""
        with self._lock:
            record = self._definition_index.get(definition_id)
            if record is None:
                return {"ok": False, "reason": f"字段定义 {definition_id} 不存在"}
            if record["status"] == "已废弃":
                return {"ok": True, "definition": record, "already": True}
            references = self.definition_references(definition_id)
            blocking = (
                len(references["in_flight_trials"])
                + len(references["pending_releases"])
                + len(references["live_strategies"])
            )
            if blocking and not cancel_trials:
                return {
                    "ok": False,
                    "reason": "字段定义仍被在途试写或线上组合引用，不能废弃",
                    "references": references,
                    "definition": record,
                }
            cancelled: list[str] = []
            if cancel_trials:
                for trial in self._trials:
                    if trial["definition_id"] == definition_id and trial["status"] in {"草稿", "试写中", "待确认"}:
                        trial["status"] = "已放弃"
                        trial["updated_at"] = time.time()
                        trial["conclusion"] = "字段定义被治理入口废弃，试写取消"
                        trial["cancel_reason"] = reason or "定义废弃"
                        cancelled.append(trial["trial_id"])
                for release in self._format_releases:
                    if release["definition_id"] == definition_id and release["status"] == "待确认":
                        release["status"] = "已取消"
                        release["updated_at"] = time.time()
            record["status"] = "已废弃"
            record["deprecated_at"] = time.time()
            record["deprecate_reason"] = reason or "候选定义清理"
            record["cancelled_trials"] = cancelled
            return {
                "ok": True,
                "definition": record,
                "cancelled_trials": cancelled,
                "references_before": references,
            }

    # ---------- 试写 ----------
    def create_trial(
        self,
        *,
        table: str,
        candidate_format: str,
        write_mode: str,
        merge_engine: str,
        definition: dict[str, Any],
        config_entry_id: int | None,
        operator: str = "数据工程组",
        note: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            self._trial_seq += 1
            previous = self._confirmed_strategies.get(table)
            trial = {
                "trial_id": f"TRIAL-{self._trial_seq:06d}",
                "table": table,
                "candidate_format": candidate_format,
                "write_mode": write_mode,
                "merge_engine": merge_engine,
                "definition_id": definition["definition_id"],
                "definition_fingerprint": definition["fingerprint"],
                "definition_revision": definition["revision"],
                "definition_topic": definition["topic"],
                "definition_snapshot": json.loads(json.dumps(definition["definition"], ensure_ascii=False)),
                "schema_version": definition["schema_version"],
                "config_entry_id": config_entry_id,
                "status": "草稿",
                "round": 0,
                "attempt_ids": [],
                "baseline_strategy": json.loads(json.dumps(previous, ensure_ascii=False)) if previous else None,
                "operator": operator,
                "note": note,
                "created_at": time.time(),
                "updated_at": time.time(),
                "conclusion": "",
                "failure_reason": "",
                "supersedes": None,
            }
            # 同表同格式的历史试写保留，作为轮次回查依据
            same = [
                t for t in self._trials
                if t["table"] == table and t["candidate_format"] == candidate_format
                and t["status"] in {"草稿", "试写中", "待确认"}
            ]
            if same:
                trial["supersedes"] = same[-1]["trial_id"]
            self._trials.append(trial)
            return trial

    def trials(self, table: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._trials)
        if table:
            rows = [r for r in rows if r["table"] == table]
        if status:
            rows = [r for r in rows if r["status"] == status]
        return rows

    def trial(self, trial_id: str) -> dict[str, Any] | None:
        for row in self._trials:
            if row["trial_id"] == trial_id:
                return row
        return None

    def record_trial_attempt(self, trial: dict[str, Any], attempt: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._attempt_seq += 1
            attempt["attempt_id"] = f"TRY-{self._attempt_seq:06d}"
            attempt["trial_id"] = trial["trial_id"]
            attempt["created_at"] = time.time()
            trial["round"] += 1
            attempt["round"] = trial["round"]
            self._trial_writes.append(attempt)
            trial["attempt_ids"].append(attempt["attempt_id"])
            trial["updated_at"] = time.time()
            return attempt

    def attempt(self, attempt_id: str) -> dict[str, Any] | None:
        for row in self._trial_writes:
            if row["attempt_id"] == attempt_id:
                return row
        return None

    def trial_attempts(self, trial_id: str) -> list[dict[str, Any]]:
        return [a for a in self._trial_writes if a["trial_id"] == trial_id]

    # ---------- 格式发布清单 / 已确认组合 ----------
    def add_release(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._release_seq += 1
            release = {
                "release_id": f"REL-{self._release_seq:06d}",
                "created_at": time.time(),
                "updated_at": time.time(),
                **payload,
            }
            self._format_releases.append(release)
            return release

    def releases(self, table: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._format_releases)
        if table:
            rows = [r for r in rows if r["table"] == table]
        if status:
            rows = [r for r in rows if r["status"] == status]
        return rows

    def release(self, release_id: str) -> dict[str, Any] | None:
        for row in self._format_releases:
            if row["release_id"] == release_id:
                return row
        return None

    def strategy(self, table: str) -> dict[str, Any] | None:
        return self._confirmed_strategies.get(table)

    def set_strategy(self, table: str, strategy: dict[str, Any] | None) -> None:
        with self._lock:
            if strategy is None:
                self._confirmed_strategies.pop(table, None)
            else:
                self._confirmed_strategies[table] = strategy

    def add_conflict(self, conflict: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            conflict["conflict_id"] = f"CFT-{len(self._conflicts) + 1:06d}"
            conflict["ts"] = time.time()
            self._conflicts.append(conflict)
            return conflict

    def conflicts(self, table: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._conflicts)
        if table:
            rows = [r for r in rows if r.get("table") == table]
        return rows


store = Store()