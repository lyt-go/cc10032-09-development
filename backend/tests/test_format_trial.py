"""换格式试写与字段定义治理测试。

围绕订单表金额列，覆盖：
1. 试写状态机：草稿 → 试写中 → 待确认 → 已生效 / 已失败 / 已放弃
2. 写入尝试留痕：表名、候选格式、字段定义编号，通过与拒绝行分列
3. 格式发布清单：试写结论回写，确认后成为线上组合
4. 写入策略解析：读已确认组合；失败后线上仍按上一组合恢复写入
5. Schema 引用保护：在途试写引用旧定义时拒绝废弃，取消后才可废弃
6. 并发：字段定义在试写后发布新 revision，旧依据确认必须被拒绝
7. 冲突回查与重试
"""
from __future__ import annotations

import pytest

from app.services import service_for
from app.services.schema_govern import SchemaGovernService
from app.services.table_format import TableFormatService
from app.store import store


ORDERS = "dwd_orders_merged"

MONEY_V1 = {
    "fields": [
        {"name": "order_id", "type": "string", "nullable": False},
        {"name": "amount", "type": "decimal", "precision": 18, "scale": 2, "nullable": False},
        {"name": "order_date", "type": "date", "nullable": True},
        {"name": "ts", "type": "timestamp", "nullable": True},
    ]
}


@pytest.fixture(autouse=True)
def reset_governance_state():
    """每个用例从干净的试写/发布/定义状态开始，避免共享内存 store 互相污染。"""
    store._trials.clear()
    store._trial_writes.clear()
    store._format_releases.clear()
    store._conflicts.clear()
    store._confirmed_strategies.clear()
    store._definitions = [d for d in store._definitions if d["topic"] != ORDERS]
    store._definition_index = {d["definition_id"]: d for d in store._definitions}
    store._topic_revisions.pop(ORDERS, None)
    yield


@pytest.fixture()
def tf() -> TableFormatService:
    service = service_for("table_format")
    assert isinstance(service, TableFormatService)
    return service


@pytest.fixture()
def sg() -> SchemaGovernService:
    service = service_for("schema_govern")
    assert isinstance(service, SchemaGovernService)
    return service


@pytest.fixture()
def orders_config() -> dict:
    """订单表格式配置：初始为 append/copy-on-write 的 iceberg 配置（草稿）。"""
    rows = store.rows("table_format")
    for row in rows:
        if row.get("目标表") == ORDERS:
            row["表格式类型"] = "iceberg"
            row["写入模式"] = "append"
            row["合并引擎"] = "copy-on-write"
            row["主键列"] = "order_id"
            row["status"] = "草稿"
            return row
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "status": "草稿", "pending": True, "abnormal": False,
        "配置编号": "TABL-ORDERS", "目标表": ORDERS, "表格式类型": "iceberg",
        "主键列": "order_id", "分区列": "order_date", "排序键列": "order_id",
        "写入模式": "append", "合并引擎": "copy-on-write",
    }
    rows.append(entry)
    return entry


def clean_samples() -> list[dict]:
    return [
        {"order_id": "ORD-1", "amount": "12.34", "order_date": "2026-11-01", "ts": "2026-11-01T00:00:00"},
        {"order_id": "ORD-2", "amount": "99.00", "order_date": "2026-11-02", "ts": "2026-11-02T00:00:00"},
    ]


@pytest.fixture()
def v1_definition(sg: SchemaGovernService) -> dict:
    result = sg.publish_definition(
        topic=ORDERS,
        definition=MONEY_V1,
        version="v1",
        status="候选",
        reason="订单金额列初始定义",
    )
    assert result["ok"], result
    return result["definition"]


def create_iceberg_trial(tf: TableFormatService, config: dict, definition_id: str,
                          write_mode: str = "append") -> dict:
    result = tf.create_trial(
        table=ORDERS,
        candidate_format="iceberg",
        write_mode=write_mode,
        merge_engine="copy-on-write",
        definition_id=definition_id,
        config_entry_id=int(config["id"]),
        note="orders 金额列换格式试写",
    )
    assert result["ok"], result
    return result


class TestTrialRegistration:
    def test_trial_starts_as_draft_and_release_is_written(
        self, tf, orders_config, v1_definition
    ):
        result = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])
        trial = result["trial"]
        assert trial["status"] == "草稿"
        assert trial["round"] == 0
        assert trial["table"] == ORDERS
        assert trial["candidate_format"] == "iceberg"
        assert trial["definition_id"] == v1_definition["definition_id"]

        release = result["release"]
        assert release["status"] == "草稿"
        assert release["trial_id"] == trial["trial_id"]
        assert release["effective"] is False
        # 配置进入试写中，但线上组合还没有被替换
        assert store.find("table_format", int(orders_config["id"]))["status"] == "试写中"
        assert store.strategy(ORDERS) is None

    def test_trial_rejects_unsupported_format(self, tf, orders_config, v1_definition):
        result = tf.create_trial(
            table=ORDERS, candidate_format="parquet", write_mode="append",
            definition_id=v1_definition["definition_id"],
            config_entry_id=int(orders_config["id"]),
        )
        assert result["ok"] is False
        assert "不在支持列表" in result["reason"]

    def test_trial_rejects_incompatible_write_mode(self, tf, orders_config, v1_definition):
        result = tf.create_trial(
            table=ORDERS, candidate_format="iceberg", write_mode="insert_overwrite",
            definition_id=v1_definition["definition_id"],
            config_entry_id=int(orders_config["id"]),
        )
        assert result["ok"] is False
        assert "不兼容" in result["reason"]

    def test_duplicate_in_flight_trial_is_rejected(self, tf, orders_config, v1_definition):
        first = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])
        second = tf.create_trial(
            table=ORDERS,
            candidate_format="iceberg",
            write_mode="append",
            definition_id=v1_definition["definition_id"],
            config_entry_id=int(orders_config["id"]),
        )
        assert second["ok"] is False
        assert first["trial"]["trial_id"] in second["reason"]


class TestWriteAttemptEvidence:
    def test_attempt_splits_passed_and_rejected_rows_by_kind(
        self, tf, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        samples = clean_samples() + [
            {"order_id": "ORD-3", "amount": "12.345", "order_date": "2026-11-03", "ts": "2026-11-03T00:00:00"},
            {"order_id": "ORD-4", "order_date": "2026-11-04", "ts": "2026-11-04T00:00:00"},
        ]
        result = tf.execute_trial_attempt(trial_id, samples)
        assert result["ok"] is True
        attempt = result["attempt"]

        # 实际写入尝试关联表名、候选格式、字段定义编号
        assert attempt["table"] == ORDERS
        assert attempt["candidate_format"] == "iceberg"
        assert attempt["definition_id"] == v1_definition["definition_id"]
        assert attempt["round"] == 1

        assert attempt["passed_count"] == 2
        assert len(attempt["passed_rows"]) == 2
        assert attempt["rejected_count"] == 2
        assert attempt["rejection_breakdown"]["类型"] == 1   # 金额小数位超限
        assert attempt["rejection_breakdown"]["缺列"] == 1   # 缺 amount
        rejected = {row["row"]["order_id"]: row for row in attempt["rejected_rows"]}
        assert rejected["ORD-3"]["kinds"] == ["类型"]
        assert rejected["ORD-4"]["kinds"] == ["缺列"]
        assert result["trial"]["status"] == "试写中"
        assert result["release"]["status"] == "试写中"
        assert result["release"]["evidence"]["passed_rows"] == 2

    def test_upsert_requires_primary_key(self, tf, orders_config, v1_definition):
        result = tf.create_trial(
            table=ORDERS, candidate_format="iceberg", write_mode="upsert",
            definition_id=v1_definition["definition_id"],
            config_entry_id=int(orders_config["id"]),
        )
        attempt = tf.execute_trial_attempt(result["trial"]["trial_id"], clean_samples())
        assert attempt["ok"] is True
        assert attempt["attempt"]["key_column"] == "order_id"

    def test_upsert_rejects_rows_without_key(self, tf, orders_config, v1_definition):
        result = tf.create_trial(
            table=ORDERS, candidate_format="iceberg", write_mode="upsert",
            definition_id=v1_definition["definition_id"],
            config_entry_id=int(orders_config["id"]),
        )
        attempt = tf.execute_trial_attempt(
            result["trial"]["trial_id"],
            [{"amount": "1.00", "order_date": "2026-11-01", "ts": "2026-11-01T00:00:00"}],
        )
        assert attempt["ok"] is True
        assert attempt["attempt"]["passed_count"] == 0
        assert attempt["attempt"]["rejection_breakdown"]["缺列"] == 1
        assert attempt["attempt"]["status"] == "失败"

    def test_trial_without_attempt_cannot_be_submitted(self, tf, orders_config, v1_definition):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        result = tf.submit_trial(trial_id)
        assert result["ok"] is False
        assert result["requires_attempt"] is True

    def test_trial_with_no_passed_rows_cannot_be_submitted(
        self, tf, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, [{"order_id": "ORD-X", "amount": "1.234"}])
        result = tf.submit_trial(trial_id)
        assert result["ok"] is False
        assert result["requires_retry"] is True
        assert store.trial(trial_id)["status"] == "试写中"


class TestConfirmAndResolve:
    def test_confirm_activates_release_and_resolution_reads_it(
        self, tf, orders_config, v1_definition
    ):
        created = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])
        trial_id = created["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        assert tf.submit_trial(trial_id)["ok"] is True
        assert store.trial(trial_id)["status"] == "待确认"

        confirmed = tf.confirm_trial(trial_id)
        assert confirmed["ok"] is True, confirmed
        assert confirmed["release"]["status"] == "已发布"
        assert confirmed["release"]["effective"] is True
        assert confirmed["adopted_definition"]["definition_id"] == v1_definition["definition_id"]
        assert store.trial(trial_id)["status"] == "已生效"
        assert store.find("table_format", int(orders_config["id"]))["status"] == "已生效"

        resolution = tf.resolve_write_strategy(int(orders_config["id"]))
        assert resolution["ok"] is True
        assert resolution["format"] == "iceberg"
        assert resolution["strategy_source"] == "已确认组合"
        assert resolution["definition_id"] == v1_definition["definition_id"]
        assert resolution["release_id"] == confirmed["release"]["release_id"]
        assert resolution["fields"] == ["order_id", "amount", "order_date", "ts"]

    def test_resolution_by_table_matches_release(self, tf, orders_config, v1_definition):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        tf.submit_trial(trial_id)
        tf.confirm_trial(trial_id)

        resolution = tf.resolve_strategy_for_table(ORDERS)
        assert resolution["ok"] is True
        assert resolution["table"] == ORDERS
        assert resolution["definition_id"] == v1_definition["definition_id"]

    def test_superseded_same_format_trial_is_rolled_up(
        self, tf, orders_config, v1_definition
    ):
        first = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(first, clean_samples())
        tf.submit_trial(first)
        tf.confirm_trial(first)

        # 第二份同格式试写：登记时第一份已生效，允许；确认时应收束并覆盖组合
        second = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(second, clean_samples())
        tf.submit_trial(second)
        result = tf.confirm_trial(second)
        assert result["ok"] is True
        assert store.trial(first)["status"] == "已生效"
        resolution = tf.resolve_strategy_for_table(ORDERS)
        assert resolution["release_id"] == result["release"]["release_id"]


class TestConcurrentFieldRelease:
    def test_amend_after_trial_blocks_confirm_and_bumps_revision(
        self, tf, sg, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        tf.submit_trial(trial_id)

        # 字段发布与候选确认并发：确认前把金额列精度从 18,2 改为 18,4
        amended = sg.amend_definition(
            v1_definition["definition_id"],
            changes=[{"field": "amount", "precision": 18, "scale": 4}],
            version="v2",
            reason="确认前调整金额列精度",
        )
        assert amended["ok"] is True, amended
        new_definition = amended["definition"]
        assert new_definition["definition_id"] != v1_definition["definition_id"]
        assert new_definition["revision"] == v1_definition["revision"] + 1
        assert new_definition["specs"]["amount"]["scale"] == 4
        assert trial_id in amended["stale_trials"]

        result = tf.confirm_trial(trial_id)
        assert result["ok"] is False
        assert result["failure_kind"] == "版本冲突"
        assert result["current_definition"]["definition_id"] == new_definition["definition_id"]
        assert store.trial(trial_id)["status"] == "已失败"
        # 没有生效时，解析仍应回到旧组合（此处此前没有已确认组合）
        assert store.strategy(ORDERS) is None

    def test_confirmation_without_full_evidence_is_rejected(
        self, tf, orders_config, v1_definition
    ):
        """直接把试写置成待确认但抹掉试写记录，确认必须拒绝。"""
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        tf.submit_trial(trial_id)
        # 模拟依据丢失
        store._trial_writes = [a for a in store._trial_writes if a["trial_id"] != trial_id]
        store.trial(trial_id)["attempt_ids"] = []
        result = tf.confirm_trial(trial_id)
        assert result["ok"] is False
        assert result["failure_kind"] == "依据缺失"


class TestFailureAndRecovery:
    def test_failed_confirm_restores_live_strategy_and_allows_retry(
        self, tf, sg, orders_config, v1_definition
    ):
        # 先确认一版线上组合
        first = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(first, clean_samples())
        tf.submit_trial(first)
        live = tf.confirm_trial(first)
        previous_release = live["release"]["release_id"]

        # 再发起一版试写，提交确认后又被字段发布打断
        second = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(second, clean_samples())
        tf.submit_trial(second)
        amended = sg.amend_definition(
            v1_definition["definition_id"],
            changes=[{"field": "amount", "precision": 18, "scale": 4}],
            version="v2",
        )
        assert amended["ok"]

        failed = tf.confirm_trial(second)
        assert failed["ok"] is False
        assert failed["config_status"] == "草稿"           # 回到可再试阶段
        assert failed["live_write_restored"] is True

        resolution = tf.resolve_strategy_for_table(ORDERS)
        assert resolution["release_id"] == previous_release  # 线上仍按上一组合写入
        assert resolution["config_requires_attention"] is True

        # 按最新字段定义重试，重新走完链路后生效
        retried = tf.retry_trial(second, definition_id=amended["definition"]["definition_id"])
        assert retried["ok"] is True
        new_trial = retried["trial"]["trial_id"]
        assert store.trial(new_trial)["definition_id"] == amended["definition"]["definition_id"]
        assert retried["release"]["status"] == "草稿"

        attempt = tf.execute_trial_attempt(
            new_trial,
            clean_samples() + [{"order_id": "ORD-9", "amount": "1.2345"}],
        )
        # 新定义允许 4 位小数，1.2345 现在通过
        assert attempt["attempt"]["passed_count"] == 3
        assert tf.submit_trial(new_trial)["ok"] is True
        confirmed = tf.confirm_trial(new_trial)
        assert confirmed["ok"] is True
        assert tf.resolve_strategy_for_table(ORDERS)["definition_id"] == amended["definition"]["definition_id"]

    def test_abandon_returns_config_to_draft(self, tf, orders_config, v1_definition):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        result = tf.abandon_trial(trial_id, reason="格式评估暂停")
        assert result["ok"] is True
        assert result["trial"]["status"] == "已放弃"
        assert result["release"]["status"] == "已取消"
        assert store.find("table_format", int(orders_config["id"]))["status"] == "草稿"


class TestSchemaReferenceProtection:
    def test_deprecation_blocked_while_trial_in_flight_then_allowed(
        self, tf, sg, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]

        refs = sg.definition_references(v1_definition["definition_id"])
        assert refs["protected"] is True
        assert [t["trial_id"] for t in refs["references"]["in_flight_trials"]] == [trial_id]

        blocked = sg.request_deprecation(v1_definition["definition_id"])
        assert blocked["ok"] is False
        assert blocked["protected"] is True
        assert trial_id in blocked["reason"]
        assert trial_id in blocked["references"]["in_flight_trials"][0]["trial_id"]
        # 在途试写存在，定义保持候选状态
        assert store.definition(v1_definition["definition_id"])["status"] == "候选"

        allowed = sg.request_deprecation(
            v1_definition["definition_id"], reason="清理候选定义", cancel_trials=True)
        assert allowed["ok"] is True
        assert allowed["cancelled_trials"] == [trial_id]
        assert store.trial(trial_id)["status"] == "已放弃"
        assert store.definition(v1_definition["definition_id"])["status"] == "已废弃"

    def test_live_definition_is_protected_even_with_cancel(
        self, tf, sg, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        tf.submit_trial(trial_id)
        tf.confirm_trial(trial_id)

        blocked = sg.request_deprecation(v1_definition["definition_id"], cancel_trials=True)
        assert blocked["ok"] is False
        assert blocked["live_protected"] is True
        assert ORDERS in blocked["reason"]
        assert store.definition(v1_definition["definition_id"])["status"] == "已确认"

    def test_abandoned_trial_releases_protection(
        self, tf, sg, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.abandon_trial(trial_id)
        result = sg.request_deprecation(v1_definition["definition_id"], reason="不再使用")
        assert result["ok"] is True
        assert store.definition(v1_definition["definition_id"])["status"] == "已废弃"


class TestDefinitionVersioning:
    def test_spec_compatibility_rejects_type_and_precision_shrink(self, sg, v1_definition):
        rows = store.rows("schema_govern")
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "已注册", "pending": False, "abnormal": False,
            "主题名称": ORDERS, "Schema定义": "{}", "版本号": "v1",
            "兼容策略": "BACKWARD", "注册时间": "2026-11-01", "序列化格式": "avro",
            "字段列表": "order_id,amount,order_date,ts", "演进历史": "",
        }
        rows.append(entry)
        store.register_schema(ORDERS, MONEY_V1, "v1")

        type_change = sg.validate_compatibility(entry["id"], {
            "version": "v2",
            "fields": [
                {"name": "order_id", "type": "integer"},
                {"name": "amount", "type": "decimal", "precision": 18, "scale": 2},
                {"name": "order_date", "type": "date"},
                {"name": "ts", "type": "timestamp"},
            ],
        })
        assert type_change["ok"] is False
        assert "类型" in type_change["reason"]

        precision_shrink = sg.validate_compatibility(entry["id"], {
            "version": "v3",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "decimal", "precision": 18, "scale": 1},
                {"name": "order_date", "type": "date"},
                {"name": "ts", "type": "timestamp"},
            ],
        })
        assert precision_shrink["ok"] is False
        assert "精度" in precision_shrink["reason"]

    def test_amend_cannot_delete_fields(self, sg, v1_definition):
        result = sg.amend_definition(
            v1_definition["definition_id"],
            definition={"fields": [
                {"name": "order_id", "type": "string", "nullable": False},
            ]},
            version="v3",
        )
        assert result["ok"] is False
        assert "删除字段" in result["reason"]

    def test_amended_definition_has_new_fingerprint_and_revision(self, sg, v1_definition):
        original = store.definition(v1_definition["definition_id"])
        amended = sg.amend_definition(
            v1_definition["definition_id"],
            changes=[{"field": "amount", "precision": 20, "scale": 4}],
            version="v2",
        )
        assert amended["ok"] is True
        new = amended["definition"]
        assert new["fingerprint"] != original["fingerprint"]
        assert new["revision"] == original["revision"] + 1
        assert new["replaced_definition_id"] == original["definition_id"]
        assert original["replaced_by"] == new["definition_id"]

    def test_sample_check_reports_type_and_missing_columns(self, sg, v1_definition):
        result = sg.sample_definition_check(
            v1_definition["definition_id"],
            clean_samples() + [{"order_id": "ORD-9", "amount": "1.234"}],
        )
        assert result["ok"] is True
        assert result["passed_count"] == 2
        assert result["rejected_count"] == 1
        kinds = {i["kind"] for i in result["rejected_rows"][0]["issues"]}
        # amount 精度超限算类型问题；缺失的 order_date/ts 是可选列，不算缺列
        assert "类型" in kinds
        missing = sg.sample_definition_check(
            v1_definition["definition_id"],
            clean_samples() + [{"order_id": "ORD-10", "order_date": "2026-11-04", "ts": "x"}],
        )
        assert missing["rejected_count"] == 1
        assert "缺列" in {i["kind"] for i in missing["rejected_rows"][0]["issues"]}


class TestConflictTraceability:
    def test_different_version_trials_conflict_is_recorded(
        self, tf, orders_config, v1_definition
    ):
        first = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(first, clean_samples())
        tf.submit_trial(first)
        tf.confirm_trial(first)

        second = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(second, clean_samples())
        tf.submit_trial(second)
        tf.confirm_trial(second)

        conflicts = store.conflicts(ORDERS)
        # 两轮都基于同一线上基线时不会冲突；人为把基线改旧后确认应被识别
        assert isinstance(conflicts, list)

        third = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(third, clean_samples())
        tf.submit_trial(third)
        # 模拟另一轮试写在待确认期间先一步生效
        store.trial(third)["baseline_strategy"] = {"release_id": "REL-STALE"}
        result = tf.confirm_trial(third)
        assert result["ok"] is False
        assert result["failure_kind"] == "版本冲突"
        assert result["conflict"]["table"] == ORDERS
        assert len(store.conflicts(ORDERS)) >= 1


class TestOldDefinitionCleanup:
    def test_superseded_definition_is_free_after_new_combination_confirmed(
        self, tf, sg, orders_config, v1_definition
    ):
        first = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(first, clean_samples())
        tf.submit_trial(first)
        tf.confirm_trial(first)

        amended = sg.amend_definition(
            v1_definition["definition_id"],
            changes=[{"field": "amount", "precision": 18, "scale": 4}],
            version="v2",
        )
        new_definition = amended["definition"]
        # 已生效的试写不能“重试”，直接按新定义登记下一版试写
        retried = create_iceberg_trial(tf, orders_config, new_definition["definition_id"])
        new_trial = retried["trial"]["trial_id"]
        tf.execute_trial_attempt(new_trial, clean_samples())
        tf.submit_trial(new_trial)
        assert tf.confirm_trial(new_trial)["ok"] is True

        # 旧定义已退役，发布清单也被替换，不再阻塞废弃
        refs = sg.definition_references(v1_definition["definition_id"])
        assert refs["protected"] is False
        assert refs["references"]["live_strategies"] == []
        assert refs["references"]["pending_releases"] == []
        deprecated = sg.request_deprecation(v1_definition["definition_id"], reason="旧版本清理")
        assert deprecated["ok"] is True
        assert store.definition(v1_definition["definition_id"])["status"] == "已废弃"

        # 线上组合仍指向新定义
        assert tf.resolve_strategy_for_table(ORDERS)["definition_id"] == new_definition["definition_id"]

    def test_pending_release_protects_definition_even_after_trial_failed(
        self, tf, sg, orders_config, v1_definition
    ):
        trial_id = create_iceberg_trial(tf, orders_config, v1_definition["definition_id"])["trial"]["trial_id"]
        tf.execute_trial_attempt(trial_id, clean_samples())
        tf.submit_trial(trial_id)
        # 发布清单进入待确认，此时定义受保护
        refs = sg.definition_references(v1_definition["definition_id"])
        assert refs["protected"] is True
        assert len(refs["references"]["pending_releases"]) == 1
        blocked = sg.request_deprecation(v1_definition["definition_id"])
        assert blocked["ok"] is False
