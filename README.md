# 流批一体数据集成平台

面向实时流接入、批流合并、变更捕获与分区治理的流批一体数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/FormatTrialView.vue    换格式试写与新旧对照
│   ├── src/views/FieldDefinitionsView.vue 字段定义版本与引用保护
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py    通用查询/导出/登记/动作接口
│   ├── app/routers/governance.py  格式试写/发布清单/字段定义治理接口
│   ├── app/services/<key>.py      每个模块独立领域逻辑
│   ├── app/services/base.py       共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 换格式试写

新表格式不再“启用即改写入策略”，而是先在候选格式 + 候选字段定义上跑样本：

1. **草稿**：`POST /api/format-trials` 登记试写，同时生成一条格式发布清单记录；线上组合不动。
2. **试写中**：`POST /api/format-trials/{id}/attempts` 每次实际写入尝试都会记录表名、候选格式、
   字段定义编号与 revision，并分别保留 `passed_rows` 与 `rejected_rows`
   （拒绝原因按“类型 / 缺列”分类计数）。
3. **待确认**：`POST /api/format-trials/{id}/submit` 把结论回写发布清单，并标注支持生效的那一轮样本。
4. **已生效**：`POST /api/format-trials/{id}/confirm` 校验字段定义未被修订、线上基线未变化后，
   发布清单转 `已发布`，写入策略解析改为读取“格式 + 字段定义”组合。

配套能力：

- `POST /api/format-trials/{id}/compare`：同一批样本分别跑旧组合与候选格式，只预演不写入，
  输出旧格式能写、候选会拒绝的行号。
- `GET /api/format-trials/{id}/review`：复核当次采用的字段定义、被拒样本与下一次策略解析结果。
- `GET /api/format-strategies/{table}`：按表解析线上写入策略；配置失败退回草稿时，
  仍返回上一已确认组合，保证线上可恢复写入。
- `POST /api/format-trials/{id}/retry`：确认失败后按最新字段定义登记新一轮试写。
- `GET /api/format-conflicts`：字段版本冲突与多轮试写冲突的回查记录。

## 字段定义治理

字段定义是不可变版本（`DEF-xxxxxx`），每次字段变更推进主题 revision：

- `POST /api/field-definitions` / `/{id}/amend`：发布或修订字段定义。
- `GET /api/field-definitions/{id}/references`：查看被哪些在途试写、待确认发布、线上组合引用。
- `POST /api/field-definitions/{id}/deprecate`：被引用时拒绝并列出引用来源；
  `cancel_trials: true` 会先取消在途试写，成功后才允许废弃。

确认生效时要求：最近一轮试写有通过行、字段定义指纹未变、主题 revision 与试写时一致、
表上线上组合与试写登记时的基线一致。任何一项不满足都会让试写转“已失败”、
配置退回“草稿”、发布清单记为“已驳回”，并保留失败原因与冲突记录。


## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式数据源 | `stream_source` | 流式源配置 | 源标识、消息中间件、Topic名称 |
| 批次分片 | `batch_slice` | 分片批次 | 批次编号、源表名称、分片键列 |
| 变更捕获 | `change_capture` | 变更捕获配置 | 配置编号、源数据库、目标表 |
| Schema治理 | `schema_govern` | Schema版本 | 主题名称、Schema定义、版本号 |
| 水位管理 | `watermark` | 消费水位 | 分区标识、消费组、当前位点 |
| 迟到数据 | `late_data` | 迟到数据策略 | 策略编号、关联源、允许延迟秒数 |
| 流式关联 | `stream_lookup` | 关联规则 | 规则编号、左流来源、右表来源 |
| 合并策略 | `merge_plan` | 合并计划 | 计划编号、流式来源、批式来源 |
| 分区扩缩 | `partition_scale` | 分区策略 | 策略编号、目标表、分区字段 |
| 快照管理 | `snapshot_mgr` | 数据快照 | 快照编号、源表名称、快照时间 |
| 数据对账 | `data_reconcile` | 对账任务 | 任务编号、左源表、右源表 |
| 回填作业 | `backfill_job` | 回填任务 | 任务编号、目标表、回填起始时间 |
| 冷热分层 | `cold_archive` | 归档策略 | 策略编号、源表名称、热数据天数 |
| 压缩合并 | `compaction` | 压缩任务 | 任务编号、目标表、目标分区 |
| 血缘图 | `lineage` | 血缘关系 | 关系编号、上游表、下游表 |
| 变更日志 | `changelog` | 变更记录 | 记录编号、目标表、变更类型 |
| 保留管理 | `retention` | 保留策略 | 策略编号、目标表、保留天数 |
| 表格式 | `table_format` | 表格式配置 | 配置编号、目标表、表格式类型 |
| 换格式试写 | `format-trials` | 格式试写 | 试写编号、候选格式、字段定义编号 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
