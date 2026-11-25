"""业务路由汇总：通用资源路由 + 格式治理路由。"""
from __future__ import annotations

from app.routers import governance, resource

ROUTERS = [governance, resource]
