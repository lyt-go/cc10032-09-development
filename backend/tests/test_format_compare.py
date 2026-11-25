"""同批样本新旧格式对照测试。"""
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
    ]
}
MONEY_TIGHT = {
    "fields": [
        {"name": "order_id", "type": "string", "nullable": False},
        {"name": "amount", "type": "decimal", "precision": 10, "scale": 2, "nullable": False},
    ]
}


@pytest.fixture(autouse=True)
def clean_state():
    store._trials.clear()
    store._trial_writes.clear()
    store._format_releases.clear()
    store._conflicts.clear()
    store._confirmed_strategies.clear()
    store._definitions = [d for d in store._definitions if d["topic"] != ORDERS]
    store._definition_index = {d["definition_id"]: d for d in store._definitions}
    store._topic_revisions.pop(ORDERS, None)
    yield


def services():
    tf = service_for("table_format")
    sg = service_for("schema_govern")
    assert isinstance(tf, TableFormatService) and isinstance(sg, SchemaGovernService)
    return tf, sg


def orders_config() -> dict:
    for row in store.rows("table_format"):
        if row.get("目标表") == ORDERS:
            row.update({
                "表格式类型": "iceberg", "写入模式": "append",
                "合并引擎": "copy-on-write", "主键列": "order_id", "status": "草稿",
            })
            return row
    raise AssertionError("缺少 orders 配置")


def samples() -> list[dict]:
    return [
        {"order_id": "ORD-1", "amount": "10.00"},
        {"order_id": "ORD-2", "amount": "123456789.00"},  # 超出窄精度
        {"amount": "5.00"},
    ]


def test_compare_reports_regressions_against_live_strategy():
    tf, sg = services()
    config = orders_config()
    old = sg.publish_definition(topic=ORDERS, definition=MONEY_V1, version="v1", status="候选")["definition"]

    live_trial = tf.create_trial(
        table=ORDERS, candidate_format="iceberg", write_mode="append",
        definition_id=old["definition_id"], config_entry_id=int(config["id"]),
    )["trial"]["trial_id"]
    tf.execute_trial_attempt(live_trial, [{"order_id": "ORD-0", "amount": "1.00"}])
    tf.submit_trial(live_trial)
    assert tf.confirm_trial(live_trial)["ok"] is True

    # 线上组合确认之后再发布收紧精度的候选定义
    new = sg.publish_definition(topic=ORDERS, definition=MONEY_TIGHT, version="v2", status="候选")["definition"]

    candidate = tf.create_trial(
        table=ORDERS, candidate_format="hudi", write_mode="insert",
        definition_id=new["definition_id"], config_entry_id=int(config["id"]),
    )["trial"]["trial_id"]

    result = tf.compare_samples(candidate, samples())
    assert result["ok"] is True
    assert result["baseline"]["definition_id"] == old["definition_id"]
    assert result["candidate"]["definition_id"] == new["definition_id"]
    # 旧格式 18,2 能写第 2 行；新格式 10,2 会拒绝
    assert result["baseline"]["passed_indexes"] == [0, 1]
    assert result["candidate"]["passed_indexes"] == [0]
    assert result["regression_row_indexes"] == [1]
    assert "1 行" in result["verdict"]
    # 预演不产生正式试写记录
    assert store.trial(candidate)["round"] == 0
    assert store.trial_attempts(candidate) == []


def test_compare_without_live_strategy_uses_empty_baseline():
    tf, sg = services()
    config = orders_config()
    definition = sg.publish_definition(topic=ORDERS, definition=MONEY_V1, version="v1", status="候选")["definition"]
    trial_id = tf.create_trial(
        table=ORDERS, candidate_format="iceberg", write_mode="append",
        definition_id=definition["definition_id"], config_entry_id=int(config["id"]),
    )["trial"]["trial_id"]

    store.set_strategy(ORDERS, None)
    result = tf.compare_samples(trial_id, samples())
    assert result["baseline"]["release_id"] is None
    assert result["baseline"]["passed_count"] == 0
    assert result["candidate"]["passed_indexes"] == [0, 1]
    assert result["improvement_row_indexes"] == [0, 1]
    assert result["regression_row_indexes"] == []
