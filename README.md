# 本地块存储演练：只读模板盘 + COW 克隆盘 + 模板退役

一个用 FastAPI 实现的本地块存储演练网页。预置一个**只读模板盘**和两个
**写时复制（COW）克隆盘**（`clone-a`、`clone-b`），块大小固定 4 KiB，
块内容以**内容寻址**方式存放在本地数据卷的文件里。支持按块号读取、覆盖、
丢弃，以及模板盘的后台退役，全程可在崩溃后恢复。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据保存在命名卷 `blockstore-data`（容器内 `/data`），容器重启后数据不丢。
停止并清空数据卷：

```bash
docker compose down -v
```

不使用 Docker 时的本地运行方式：

```bash
pip install -r requirements.txt
DATA_DIR=./data BLOCK_COUNT=32 uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 数据卷布局

```
/data
├── template/index.json      # 模板索引：块号 -> 内容 sha256（退役前只读）
├── store/<sha256>           # 内容寻址的 4 KiB 块物理文件（去重存储）
├── clones/clone-a.json      # clone-a 元数据：覆盖/丢弃的块号 -> {kind, hash}
├── clones/clone-b.json      # clone-b 元数据
└── retirement.json          # 退役状态机（含游标，支持中断续跑）
```

## 核心语义

### 读 / 覆盖 / 丢弃（4 KiB 块）

- **读取**：克隆盘自有块优先；未改动的块通过**模板索引**读取；丢弃块和无映射块读全零。
  响应里的 `source` 标明来源：`clone` / `template` / `zero_discard` / `zero_unmapped`。
- **覆盖**：写入内容按 sha256 存入 `store/`，只在所选克隆盘的元数据里建立引用，
  **另一块盘完全不受影响**。
- **丢弃**：把该块标记为 `zero`，之后读取返回 4 KiB 全零；模板与另一盘不受影响。
- 覆盖/丢弃都采用「临时文件 + fsync + 原子 rename + 目录 fsync」落盘，
  **元数据持久化后接口才返回成功**，因此进程崩溃不会丢失已确认的改动。

### 去重

`store/` 是内容寻址存储：相同内容只有一个物理文件。两个克隆盘（以及模板）
引用同一内容时，物理上**只存一份**。覆盖某块后若旧内容再无任何引用，
会在同一把全局锁内立即回收；退役 GC 负责兜底清理。

### 模板盘退役（后台执行）

点击网页「开始 / 继续退役」或 `POST /api/retirement/start`，后台线程依次执行：

1. `materializing`：逐块把模板引用物化进两个克隆盘（每批 `RETIRE_BATCH_SIZE` 块，
   分批提交，期间两盘正常读写；克隆盘自己的覆盖/丢弃始终优先，不会被物化覆盖）。
2. `index_removed`：校验两盘已覆盖全部原模板块后，**删除模板索引文件**。
   此后两盘只靠各自元数据独立读写，保留各自最新内容。
3. `gc_sweep`：在全局锁内扫描引用集合，**删除所有无引用的 store 文件**，
   仍被引用的模板块（现由克隆盘引用）保留单份。
4. `completed`：仅当索引已移除且无引用块清理完毕，网页才显示「退役完成」。

- 退役期间两盘始终可读写、丢弃。
- 任一步骤失败：状态置 `failed` 并记录 `error` 原因，网页展示失败原因，**两盘照常使用**；
  可再次点击开始，从持久化的阶段/游标继续。
- 进程中断后重启：启动时自动加载 `retirement.json` 并**续跑未完成的退役**；
  已确认的覆盖和丢弃因元数据已落盘而继续有效。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 演练网页 |
| GET | `/api/status` | 卷、模板、两盘、去重、退役总览 |
| GET | `/api/clones` | 克隆盘列表与容量 |
| GET | `/api/clones/{name}/blocks/{no}` | 读块（含来源、sha256、hex/base64） |
| PUT | `/api/clones/{name}/blocks/{no}` | 覆盖块（body: `data_hex` / `data_base64` / `pattern`） |
| DELETE | `/api/clones/{name}/blocks/{no}` | 丢弃块（之后读零） |
| GET | `/api/template/blocks/{no}` | 直接读模板块（索引移除后 404） |
| GET | `/api/retirement` | 退役状态机详情 |
| POST | `/api/retirement/start` | 开始 / 继续退役 |
| GET | `/healthz` | 健康检查 |

`name` 取 `clone-a` 或 `clone-b`；`no` 取 `[0, BLOCK_COUNT-1]`。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据卷目录 |
| `BLOCK_COUNT` | `32` | 每盘块数（模板与克隆容量一致） |
| `TEMPLATE_SEED` | `template-disk-v1` | 模板内容种子（决定初始块内容） |
| `RETIRE_BATCH_SIZE` | `8` | 物化阶段每批块数 |
| `RETIRE_STEP_DELAY` | `0` | 退役每步延迟秒数，调大可慢速观察后台进度 |
| `FAILPOINT` | 空 | 故障演练：`before_index_remove` 或 `before_gc_delete` |

## 故障演练示例

```bash
# 模拟 GC 阶段失败：网页会显示失败原因，两盘照常使用；去掉该变量重启后重试即完成
FAILPOINT=before_gc_delete docker compose up
```

## 测试

```bash
pip install pytest httpx
python -m pytest -q
```

覆盖：模板索引读取、COW 隔离、丢弃读零、内容去重、退役全流程、退役期间并发读写、
两类注入失败与重试、SIGKILL 式中断后的重启续跑、重启后已完成状态的稳定性。
