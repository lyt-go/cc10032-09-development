"""HTTP 层契约测试：用 FastAPI TestClient 走通试写与治理接口。

依赖 httpx（TestClient 需要）；当前环境未安装时整组跳过，避免影响纯领域测试。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("httpx", reason="需要 httpx 才能使用 FastAPI TestClient")
from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.store import store  # noqa: E402

ORDERS = "dwd_orders_merged"
MONEY_V1 = {
    "fields": [
        {"name": "order_id", "type": "string", "nullable": False},
        {"name": "amount", "type": "decimal", "precision": 18, "scale": 2, "nullable": False},
        {"name": "order_date", "type": "date", "nullable": True},
    ]
}


@pytest.fixture()
def client() -> TestClient:
    store._trials.clear()
    store._trial_writes.clear()
    store._format_releases.clear()
    store._conflicts.clear()
    store._confirmed_strategies.clear()
    store._definitions = [d for d in store._definitions if d["topic"] != ORDERS]
    store._definition_index = {d["definition_id"]: d for d in store._definitions}
    store._topic_revisions.pop(ORDERS, None)
    return TestClient(app)


@pytest.fixture()
def definition_id(client: TestClient) -> str:
    response = client.post("/api/field-definitions", json={
        "topic": ORDERS, "definition": MONEY_V1, "version": "v1", "status": "候选",
    })
    assert response.status_code == 200, response.text
    return response.json()["definition"]["definition_id"]


class TestTrialHttpFlow:
    def test_full_flow_over_http(self, client: TestClient, definition_id: str):
        samples = [
            {"order_id": "ORD-1", "amount": "10.00"},
            {"order_id": "ORD-2", "amount": "10.001"},
            {"order_id": "ORD-3"},
        ]
        created = client.post("/api/format-trials", json={
            "table": ORDERS,
            "candidate_format": "iceberg",
            "write_mode": "append",
            "definition_id": definition_id,
            "samples": samples,
        })
        assert created.status_code == 200, created.text
        body = created.json()
        assert body["ok"] is True
        trial_id = body["trial"]["trial_id"]
        assert body["attempt"]["passed_count"] == 1
        assert body["attempt"]["rejected_count"] == 2
        assert body["release"]["status"] == "试写中"

        submitted = client.post(f"/api/format-trials/{trial_id}/submit")
        assert submitted.json()["trial"]["status"] == "待确认"

        confirmed = client.post(f"/api/format-trials/{trial_id}/confirm")
        assert confirmed.json()["ok"] is True
        assert confirmed.json()["release"]["status"] == "已发布"

        strategy = client.get(f"/api/format-strategies/{ORDERS}")
        payload = strategy.json()
        assert payload["definition_id"] == definition_id
        assert payload["write_mode"] == "append"

        review = client.get(f"/api/format-trials/{trial_id}/review").json()
        assert review["adopted_definition"]["definition_id"] == definition_id
        assert len(review["rejected_samples"]) == 2
        assert review["next_resolution"]["release_id"] == confirmed.json()["release"]["release_id"]

    def test_amend_then_confirm_is_rejected_over_http(self, client: TestClient, definition_id: str):
        created = client.post("/api/format-trials", json={
            "table": ORDERS,
            "candidate_format": "iceberg",
            "write_mode": "append",
            "definition_id": definition_id,
            "samples": [{"order_id": "ORD-1", "amount": "10.00"}],
        }).json()
        trial_id = created["trial"]["trial_id"]
        client.post(f"/api/format-trials/{trial_id}/submit")

        amended = client.post(f"/api/field-definitions/{definition_id}/amend", json={
            "changes": [{"field": "amount", "precision": 18, "scale": 4}],
            "version": "v2",
            "reason": "确认前调整精度",
        })
        assert amended.json()["ok"] is True

        failed = client.post(f"/api/format-trials/{trial_id}/confirm").json()
        assert failed["ok"] is False
        assert failed["failure_kind"] == "版本冲突"
        assert failed["config_status"] == "草稿"

    def test_deprecation_protection_over_http(self, client: TestClient, definition_id: str):
        created = client.post("/api/format-trials", json={
            "table": ORDERS,
            "candidate_format": "iceberg",
            "write_mode": "append",
            "definition_id": definition_id,
            "samples": [{"order_id": "ORD-1", "amount": "10.00"}],
        }).json()
        trial_id = created["trial"]["trial_id"]

        blocked = client.post(f"/api/field-definitions/{definition_id}/deprecate", json={})
        payload = blocked.json()
        assert payload["ok"] is False
        assert payload["protected"] is True
        assert payload["references"]["in_flight_trials"][0]["trial_id"] == trial_id

        cancelled = client.post(
            f"/api/field-definitions/{definition_id}/deprecate",
            json={"cancel_trials": True, "reason": "清理候选定义"},
        ).json()
        assert cancelled["ok"] is True
        assert cancelled["cancelled_trials"] == [trial_id]

        definitions = client.get(f"/api/field-definitions?topic={ORDERS}").json()
        target = next(d for d in definitions["items"] if d["definition_id"] == definition_id)
        assert target["status"] == "已废弃"
