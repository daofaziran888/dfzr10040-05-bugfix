# 熔炉工坊 · 实时数据融合与宽表构建平台

面向多源异构数据的实时接入融合、宽表映射构建、维度关联编排、数据质量门禁与分区快照管理的实时数据融合平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

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
| 数据源定义 | `source_def` | 数据源 | 源编号、源名称、源类型 |
| Schema映射 | `schema_map` | 映射方案 | 方案编号、源表名称、目标宽表 |
| 维度表 | `dim_table` | 维度定义 | 维度编号、维度名称、主键字段 |
| 融合作业 | `fusion_job` | 融合任务 | 作业编号、目标宽表、参与数据源 |
| 冲突仲裁 | `merge_conflict` | 冲突规则 | 规则编号、目标宽表、冲突字段 |
| 质量门禁 | `quality_gate` | 门禁规则 | 规则编号、目标宽表、检查类型 |
| 分区快照 | `snapshot_mgr` | 快照分区 | 分区编号、目标宽表、分区键值 |
| 数据回填 | `backfill_task` | 回填任务 | 任务编号、目标宽表、回填区间 |
| 数据血缘 | `lineage_edge` | 血缘边 | 边编号、上游节点、下游节点 |
| 重放队列 | `replay_queue` | 重放记录 | 记录编号、融合作业、失败原因 |
| 水位追踪 | `watermark_track` | 水位记录 | 记录编号、数据源、当前水位 |
| 分区策略 | `partition_rule` | 分区定义 | 策略编号、目标宽表、分区类型 |
| 宽表定义 | `wide_table_def` | 宽表元数据 | 表编号、宽表名称、字段列表 |
| 增量归并 | `delta_merge` | 增量批次 | 批次编号、融合作业、归并键 |
| 冷热分层 | `cold_store` | 分层策略 | 策略编号、目标宽表、热数据天数 |
| 快照导出 | `export_snapshot` | 导出计划 | 计划编号、目标宽表、导出格式 |
| 指标聚合 | `metric_aggr` | 聚合规则 | 规则编号、目标宽表、聚合函数 |
| 交叉校验 | `cross_validate` | 校验方案 | 方案编号、主体宽表、参照宽表 |
| 数据漂移 | `data_drift` | 漂移记录 | 记录编号、宽表名称、漂移类型 |
| 融合监控 | `fusion_monitor` | 监控指标 | 指标编号、融合作业、监控维度 |
| 变更审计 | `audit_trail` | 审计记录 | 记录编号、变更目标、操作人 |
| 目录检索 | `catalog_search` | 数据目录 | 目录编号、宽表名称、标签列表 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 重放终态回执与回填交付

重放队列的「已恢复 / 永久跳过」是终态，只能由重放终态回执确认；
迟到回执不会覆盖已经落定的终态。实现分三层：

- `app/recovery.py`：`ReplayTerminalLedger` 按 (片段, 尝试) 只确认一个终态，
  冲突回执保留原出错片段与尝试编号；`ReplayQueue` 的终态不可被迟到动作或回执改写。
- `app/backfill_replay.py`：回填端暂存行 + 写入凭据、领取范围、宽表幂等交付。
  领取范围只由生效终态回执与已落地证据决定；成功终态里同一片段同一行只记一份有效回填行。
- `app/services/{replay_queue,backfill_task}.py`：模块动作与查询入口。

接口（`backend` 起服务后可直接调用）：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/backfill_task/staged-rows` | 登记暂存行与写入凭据（先于终态回执） |
| POST | `/api/backfill_task/receipts` | 提交重放终态回执（成功/耗尽/永久跳过） |
| POST | `/api/backfill_task/hold-terminal` | 暂扣某次尝试的终态确认 |
| POST | `/api/backfill_task/resolve-terminal` | 写入证据齐全时落定唯一终态 |
| GET | `/api/backfill_task/next-range` | 下一次可领的数据范围 |
| POST | `/api/backfill_task/claims` | 领取后续工作（只给范围） |
| POST | `/api/backfill_task/claims/{code}/execute` | 按领取补采/回填，宽表幂等交付 |
| GET | `/api/backfill_task/segments/{code}/detail` | 回填详情：终态、两份回执、生效行 |
| GET | `/api/backfill_task/segments/{code}/wide-table` | 读取宽表行明细核对行数 |
| GET | `/api/replay_queue/receipts` | 查看终态回执（含未生效的冲突证据） |
| GET | `/api/replay_queue/held` | 查看暂扣中的终态确认 |

宽表交付按 (片段, 行标识) 去重：任一端先确认，另一端都不会再次交付；
同一份成功回执重复投递只登记一次生效行，其余记为重复证据。

领域：数据工程
子方向：数仓与数据湖
