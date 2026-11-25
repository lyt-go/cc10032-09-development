"""治理接口：格式试写、格式发布清单、字段定义版本与引用保护。

通用资源路由只提供列表/登记/动作；换格式试写需要带样本、多轮尝试和发布结论，
这些是跨表、跨模块的治理动作，单独挂在这里，避免把业务判断塞进路由层。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services import service_for
from app.services.schema_govern import SchemaGovernService
from app.services.table_format import TableFormatService
from app.store import store

router = APIRouter(prefix="/api", tags=["格式治理"])


def table_format_service() -> TableFormatService:
    service = service_for("table_format")
    assert isinstance(service, TableFormatService)
    return service


def schema_service() -> SchemaGovernService:
    service = service_for("schema_govern")
    assert isinstance(service, SchemaGovernService)
    return service


# ---------------------------------------------------------------- 试写
@router.post("/format-trials")
def create_format_trial(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """登记一次换格式试写；带 samples 时立即执行第一轮写入尝试。"""
    service = table_format_service()
    result = service.create_trial(
        table=str(payload.get("table", "")),
        candidate_format=str(payload.get("candidate_format", "")),
        write_mode=str(payload.get("write_mode", "append")),
        merge_engine=str(payload.get("merge_engine", "copy-on-write")),
        definition_id=payload.get("definition_id"),
        config_entry_id=payload.get("config_entry_id"),
        operator=str(payload.get("operator", "数据工程组")),
        note=str(payload.get("note", "")),
    )
    if not result.get("ok"):
        return result
    samples = payload.get("samples")
    if samples is not None:
        trial_id = result["trial"]["trial_id"]
        attempt = service.execute_trial_attempt(trial_id, samples, origin="创建试写时提交样本")
        if not attempt.get("ok"):
            return attempt
        result["trial"] = attempt["trial"]
        result["release"] = attempt["release"]
        result["attempt"] = attempt["attempt"]
    return result


@router.get("/format-trials")
def list_format_trials(
    table: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    service = table_format_service()
    trials = [service.trial_view(t) for t in store.trials(table=table, status=status)]
    return {"ok": True, "items": trials, "total": len(trials)}


@router.get("/format-trials/{trial_id}")
def get_format_trial(trial_id: str) -> dict[str, Any]:
    service = table_format_service()
    trial = store.trial(trial_id)
    if trial is None:
        raise HTTPException(status_code=404, detail=f"试写 {trial_id} 不存在")
    return {"ok": True, "trial": service.trial_view(trial)}


@router.get("/format-trials/{trial_id}/review")
def review_format_trial(trial_id: str) -> dict[str, Any]:
    """复核：当次采用的字段定义、被拒样本、试写结论与下一次策略解析结果。"""
    service = table_format_service()
    trial = store.trial(trial_id)
    if trial is None:
        raise HTTPException(status_code=404, detail=f"试写 {trial_id} 不存在")

    view = service.trial_view(trial)
    attempts = view["attempts"]
    latest = attempts[-1] if attempts else None
    definition = store.definition(trial["definition_id"])
    latest_definition = store.latest_definition(trial["definition_topic"])
    release = service.release_view(store.release(trial.get("release_id")))
    resolution = service.resolve_strategy_for_table(trial["table"])

    rejected_samples = []
    for attempt in attempts:
        for row in attempt["rejected_rows"]:
            rejected_samples.append({
                "attempt_id": attempt["attempt_id"],
                "round": attempt["round"],
                "definition_id": attempt["definition_id"],
                "candidate_format": attempt["candidate_format"],
                "write_mode": attempt["write_mode"],
                **row,
            })

    current_revision = store.topic_revision(trial["definition_topic"])
    return {
        "ok": True,
        "trial": view,
        "adopted_definition": service._definition_summary(definition),
        "definition_revision_at_trial": trial["definition_revision"],
        "definition_revision_current": current_revision,
        "definition_drifted": current_revision != trial["definition_revision"],
        "latest_definition": (
            service._definition_summary(latest_definition) if latest_definition else None
        ),
        "release": release,
        "supporting_round": latest,
        "conclusion": trial.get("conclusion"),
        "failure_reason": trial.get("failure_reason") or None,
        "rejected_samples": rejected_samples,
        "next_resolution": resolution,
        "conflicts": store.conflicts(trial["table"]),
        "retry_of": trial.get("retry_of") or trial.get("supersedes"),
    }


@router.post("/format-trials/{trial_id}/attempts")
def run_trial_attempt(trial_id: str, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    service = table_format_service()
    return service.execute_trial_attempt(
        trial_id,
        payload.get("samples"),
        origin=str(payload.get("origin", "补充样本")),
    )


@router.post("/format-trials/{trial_id}/compare")
def compare_trial_samples(trial_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """同一批样本分别跑线上组合与候选格式，只预演不写入。"""
    return table_format_service().compare_samples(trial_id, payload.get("samples") or [])


@router.post("/format-trials/{trial_id}/submit")
def submit_trial(trial_id: str) -> dict[str, Any]:
    return table_format_service().submit_trial(trial_id)


@router.post("/format-trials/{trial_id}/confirm")
def confirm_trial(trial_id: str) -> dict[str, Any]:
    return table_format_service().confirm_trial(trial_id)


@router.post("/format-trials/{trial_id}/retry")
def retry_trial(trial_id: str, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return table_format_service().retry_trial(trial_id, payload.get("definition_id"))


@router.post("/format-trials/{trial_id}/abandon")
def abandon_trial(trial_id: str, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return table_format_service().abandon_trial(trial_id, str(payload.get("reason", "人工放弃")))


# ---------------------------------------------------------------- 发布清单 / 已确认组合
@router.get("/format-releases")
def list_format_releases(
    table: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    service = table_format_service()
    releases = [service.release_view(r) for r in store.releases(table=table, status=status)]
    return {"ok": True, "items": releases, "total": len(releases)}


@router.get("/format-releases/{release_id}")
def get_format_release(release_id: str) -> dict[str, Any]:
    service = table_format_service()
    release = store.release(release_id)
    if release is None:
        raise HTTPException(status_code=404, detail=f"发布记录 {release_id} 不存在")
    return {"ok": True, "release": service.release_view(release)}


@router.get("/format-strategies/{table}")
def get_format_strategy(table: str) -> dict[str, Any]:
    return table_format_service().resolve_strategy_for_table(table)


@router.get("/format-conflicts")
def list_format_conflicts(table: str | None = Query(default=None)) -> dict[str, Any]:
    conflicts = store.conflicts(table=table)
    return {"ok": True, "items": conflicts, "total": len(conflicts)}


# ---------------------------------------------------------------- 字段定义
@router.get("/field-definitions")
def list_field_definitions(
    topic: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    return schema_service().list_definitions(topic=topic, status=status)


@router.post("/field-definitions")
def publish_field_definition(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    return schema_service().publish_definition(
        topic=str(payload.get("topic", "")),
        definition=payload.get("definition") or {},
        version=str(payload.get("version", "v1")),
        source_entry_id=payload.get("source_entry_id"),
        status=str(payload.get("status", "候选")),
        replace_definition_id=payload.get("replace_definition_id"),
        reason=str(payload.get("reason", "")),
    )


@router.get("/field-definitions/{definition_id}")
def get_field_definition(definition_id: str) -> dict[str, Any]:
    service = schema_service()
    record = store.definition(definition_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"字段定义 {definition_id} 不存在")
    return {"ok": True, "definition": service.definition_view(record)}


@router.get("/field-definitions/{definition_id}/references")
def get_definition_references(definition_id: str) -> dict[str, Any]:
    return schema_service().definition_references(definition_id)


@router.post("/field-definitions/{definition_id}/amend")
def amend_field_definition(definition_id: str, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    return schema_service().amend_definition(
        definition_id,
        changes=payload.get("changes"),
        definition=payload.get("definition"),
        version=payload.get("version"),
        reason=str(payload.get("reason", "")),
        source_entry_id=payload.get("source_entry_id"),
    )


@router.post("/field-definitions/{definition_id}/deprecate")
def deprecate_field_definition(
    definition_id: str, payload: dict[str, Any] = Body(default={})
) -> dict[str, Any]:
    return schema_service().request_deprecation(
        definition_id,
        reason=str(payload.get("reason", "")),
        cancel_trials=bool(payload.get("cancel_trials", False)),
    )


@router.post("/field-definitions/{definition_id}/sample-check")
def sample_check_definition(
    definition_id: str, payload: dict[str, Any] = Body(...)
) -> dict[str, Any]:
    return schema_service().sample_definition_check(definition_id, payload.get("samples") or [])
