# 气象观测数据湖

FastAPI + PyArrow(Parquet) + SQLite 的观测数据湖示例。网页维护一张观测表：

- 固定列：`station_id`（站号）、`observed_at`（UTC 时刻，写入前统一换算为 UTC）；
- 最多 8 个可空 `int64` 测量列，可新增 / 改名 / 删除；
- 每批上传采集站导出的 CSV（≤1000 行），转换为 Parquet 入湖；
- 可按**当前**列名查看、下载任一批次（CSV / Parquet）；
- 可下载整湖备份 ZIP，并在管理员误改字段 / 误收批次后整体恢复。

## 设计要点（对应需求）

| 需求 | 实现 |
| --- | --- |
| 改名沿用旧值 | Parquet 的物理列名是永久 `field_id`（`f_<uuid4>`），列名只存在于 `fields` 表；改名只更新 `fields.name`，历史文件不动 |
| 删除后同名新增不串值 | 删除是软删除（`status='deleted'`，保留 id）；同名新增生成新的 `field_id`；历史批次中没有新 id 的列在读取时补 `NULL` |
| 历史 Parquet 不得重写 | 只做“读 + 内存投影”，从不以写方式打开历史文件；新文件以临时文件写入后 rename |
| 结构版本 | SQLite `meta.schema_version` 单调递增；每次新增/改名/删除提交时 +1 |
| 过期整次拒绝 | 表单/API 必须携带页面所见版本；写事务内 `BEGIN IMMEDIATE` 先比对版本，不等则整次拒绝（HTTP 409 / 网页提示） |
| 转换期间仍可改结构 | 上传路径只在解析前后短暂读库，写 Parquet 时不持锁；提交登记时二次比对版本，若结构已变则拒绝登记并删除刚写的文件 |
| 双进程并发 | SQLite WAL + `busy_timeout` + `BEGIN IMMEDIATE` 串行化写入；两实例共享数据目录 |
| 失败/重启后一致 | 批次登记与结构变更同一事务；文件先落盘再登记，登记失败删文件；启动时清理陈旧临时文件与（超过 1 小时的）孤儿 Parquet |
| 查询只用同一已提交结构 | `get_batch` / 页面在单个读事务中读取结构与批次登记；不读未提交数据 |
| 失败保留已有批次 | 任何失败路径都不改已有登记与文件；`/api/integrity` 可核对登记与文件 sha256 |
| 整湖备份 | `/backup` 页或 `/api/backup` 下载 ZIP：一个已提交读事务取字段身份（含已删除）、结构、批次清单，再打包全部批次原件；打包期间不持写锁，可继续上传 / 改列 |
| 整包校验 | 恢复包必须含 `manifest.json`；逐个核对目录、原件长度与 SHA-256，要求登记与文件一一对应；路径越界 / 绝对路径 / 符号链接 / 加密成员 / 重复条目一律整包拒绝；只重放清单，不重新解析 CSV |
| 原子切换 | 先在 `staging/` 完整构建并 fsync 新代际，再 rename 进 `generations/` 并在控制库一次提交指针；查询只绑定一个代际，永远读到完整旧湖或完整新湖 |
| 恢复时仍可读写 | 校验与暂存构建阶段不接触当前湖，普通上传 / 改列照常；只有最后“rename + 指针提交”持控制库写锁 |
| 双实例至多一份成功 | 恢复在控制库写锁内做代际 CAS；另有进程级 `flock`。两个实例基于同一旧湖同时恢复，至多一个 201，另一个 409 |
| 旧页面 / 在途上传过期 | 每次切换 epoch+1、代际 id 变化；所有写操作校验“结构版本 + 代际 token”。切换后旧页面即使版本数字相同也会被 409 `stale_generation` 拒绝，切换瞬间未提交的上传同样整体回滚 |
| 恢复失败 / 中断 | 切换提交失败会把新代际移回暂存，控制指针保持旧湖；启动时清理未引用的暂存目录与过期旧代际，绝不开放半份数据 |
| 恢复后规则不变 | 沿用备份中的 `field_id`、删除状态与批次登记；下载、改名、删除后同名新增等行为与恢复前一致 |

## 本机运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data INSTANCE_NAME=node-a uvicorn app.main:app --port 8000
```

另一个终端用相同 `DATA_DIR`、`INSTANCE_NAME=node-b` 起第二个实例（端口 8001），
分别打开网页即可演示双进程共享结构与批次。

## 备份 / 恢复

打开 `http://localhost:8001/backup`（或任一实例的 `/backup`）：

1. **下载整湖备份**：得到 `metlake-backup-v<版本>-<代际>.zip`，内含
   `manifest.json` 与 `parquet/<batch_id>.parquet` 原件。打包期间其他实例仍可上传、改列；
2. **恢复整湖**：选择该 ZIP 上传。服务先在 `staging/` 校验并构建新代际，
   通过后原子切换；缺件、损坏或路径越界会整包拒绝，当前湖不变；
3. 恢复后回到首页 / 批次页即可正常取数；旧标签页里的表单再次提交会提示
   “数据湖已切换”，需要刷新。

命令行等价操作：

```bash
curl -OJ http://localhost:8001/api/backup
curl -F "generation=<当前代际>" -F "file=@metlake-backup-....zip" http://localhost:8001/api/backup
curl http://localhost:8001/api/backup/restores   # 恢复审计记录
```

数据目录布局（`DATA_DIR`）：

```
control.sqlite3          # 当前代际指针 + 恢复审计（跨进程 CAS 协调点）
generations/g_<uuid>/    # 一个已提交代的 lake.sqlite3 + parquet/
staging/                 # 恢复构建中；未切换前不影响当前湖
restore.lock             # 跨进程恢复互斥
lake.sqlite3、parquet/   # 兼容旧路径的符号链接，指向当前代际
```

## Docker Compose（双实例 + 共享持久卷）

```bash
docker compose up --build -d
# 浏览器打开 http://localhost:8001 (node-a) 与 http://localhost:8002 (node-b)
docker compose down          # 保留命名卷 lake-data
docker compose down -v       # 连同数据一起删除
```

两个服务共用命名卷 `lake-data`，`DATA_DIR=/data`。两个网页都可备份、
恢复和取数；两个实例同时恢复时，至多一份成功。

## 网页演示流程

1. `http://localhost:8001/` 新增测量列 `temp_c`、`humidity`；
2. 下载“示例 CSV”，或自行准备：

   ```csv
   station_id,observed_at,temp_c,humidity
   54511,2026-01-01T00:00:00Z,7,40
   54512,2026-01-01T01:00:00Z,9,
   ```

3. 上传后进入批次页查看；
4. 在 `node-b` 把 `temp_c` 改名为 `temperature_c`，回到 `node-a` 刷新批次页：列名新、值仍为旧值；
5. 删除 `temp_c`（或已改名的列）后新增同名 `temp_c`，再上传新批次：
   旧批次该列显示 `NULL`，新批次显示新值；旧 Parquet 文件字节未变；
6. 打开两个标签页（或分别用两个实例）同时提交基于同一版本的结构修改：
   一个成功、另一个提示“结构版本已过期”并被整次拒绝；
7. `/api/integrity` 或首页“一致性自检”核对登记与文件一致；
8. 打开 `/backup` 下载整湖 ZIP，然后继续上传 / 改列；再用该 ZIP 恢复：
   批次与结构回到备份时点，备份后新增的批次消失；
9. 恢复前打开的另一标签页提交改名 / 上传：因代际已变被拒绝，即使结构版本
   数字与恢复后相同也不会写入新湖。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 结构快照 + 批次列表 |
| GET | `/healthz` | 实例名、pid、版本 |
| POST | `/api/fields` | `{"schema_version": v, "name": "..."}` |
| PATCH | `/api/fields/{field_id}` | 改名，body 同上 |
| DELETE | `/api/fields/{field_id}` | 软删除，body `{"schema_version": v}` |
| POST | `/api/batches` | multipart：`version` + `file` |
| GET | `/api/batches/{id}?format=json\|csv\|parquet` | 按当前结构读取/下载 |
| GET | `/api/integrity` | 文件与登记一致性自检 |
| GET | `/backup` | 备份 / 恢复网页 |
| GET | `/backup/download`、`/api/backup` | 下载整湖备份 ZIP |
| POST | `/backup/restore`、`/api/backup` | multipart：`file`（ZIP）+ 可选 `generation` |
| GET | `/api/backup/restores` | 最近恢复审计记录 |

结构写入（新增 / 改名 / 删除 / 上传）同时携带 `schema_version` 与页面所见
`generation`。过期版本返回 409 `stale_schema`；代际过期返回 409
`stale_generation`；恢复包不合格返回 400 `backup_rejected`；文件缺失 500
`integrity_error`。旧调用方不带 `generation` 仍然可用（只按版本校验），
网页表单与新版 API 客户端会自动携带。

恢复包格式（`manifest.json` 关键字段）：

- `format` = `metlake-backup`，`format_version` = `1`；
- `schema_version`、`fields[]`（`id`/`name`/`ordinal`/`status`/版本）、`batches[]`（含
  `file_name`、`file_sha256`、`row_count`、上传时结构）与 `files[]`（路径 / 长度 / SHA-256）。

## 实现与验证（本次交付）

**已实现**

- 代际化存储：`control.sqlite3` 保存当前代际 / epoch / 恢复审计；
  数据按 `generations/g_<uuid>/` 不可变代际组织；旧版
  `data_dir/lake.sqlite3 + parquet/` 在启动时自动迁移（中断可续）。
- 备份：`/backup` 页与 `/backup/download`、`/api/backup`。ZIP 内含
  `manifest.json`（字段身份含已删除项、结构版本、批次登记）与全部批次
  Parquet 原件；打包在单个已提交读事务取清单，打包期间不持写锁，可继续上传 / 改列。
- 恢复：`/backup/restore`、`POST /api/backup`。在 `staging/` 完整校验并构建，
  逐件核对目录、清单路径、长度与 SHA-256，拒绝缺件、CRC / 解压损坏、重复条目、
  非本服务 ZIP、绝对路径 / `..` 越界、符号链接、登记与文件不一致、行数不符，
  并确认每个原件可被 Parquet 读取；全程不重新解析 CSV。
- 原子切换与可见性：新代际 fsync 后 rename + 控制库指针一次提交；查询在打开
  连接前固定代际，因此每次只能看到完整旧湖或完整新湖。
- 并发与过期：控制库写锁内做代际 CAS，另有跨进程 `flock`；双实例基于同一
  旧湖同时恢复至多一份成功。所有写入校验“结构版本 + 代际 token”，恢复后旧
  页面修改与切换时未提交的上传即使版本数字相同也会被 409 `stale_generation`
  拒绝，不会写入新湖。
- 故障一致性：切换提交失败把新代际移回暂存，指针保持旧湖；启动时清理未引用
  暂存 / 过期旧代际；恢复中重启后仍是完整旧湖或完整新湖。
- 恢复沿革：沿用备份中的 `field_id`、删除状态、批次登记与下载规则；不重写原件。
- 可启动双实例共卷 Compose（`docker-compose.yml`，两服务共享 `lake-data` 卷，
  网页可备份 / 恢复 / 取数）。

**实际验证**

- `python -m pytest -q`：54 项全部通过（含两个真实 uvicorn 进程共享目录的
  并发恢复、旧页面过期、恢复后取数、损坏包逐字节拒绝、迁移中断续传等）。
- `python scripts/selftest_dual.py`：45 项端到端检查全部 PASS（真实双进程、
  共享目录、网页备份 ZIP、并发恢复 201/409、旧页面与在途上传 409、恢复后
  下载与新增列 / 上传、损坏包整包拒绝）。
- 额外手工验证：恢复锁跨进程互斥（fork 子进程被拒）、并发初始化只产生一个
  代际、恢复构建库为 WAL、缺失当前代际库不会被静默重建。

**未完成 / 限制**

- 沙箱无 `docker`，Compose 与镜像构建未实际运行，仅做了 YAML 解析与同构
  双进程验证；需在装有 Docker 的机器上执行 `docker compose up --build` 复核。
- 备份在上传前需完整落到服务端临时文件（默认上限 4 GiB），超大湖需要调高
  上限或改为对象存储直传。
- 恢复不分批、不合并，不能只恢复单个批次或字段。
- 旧代际保留 1 小时后删除；超过该时长的超慢查询会失败并提示刷新。
- 网页无鉴权，备份 / 恢复权限应由部署层（反向代理 / 内网）控制。

## 测试

```bash
python -m pytest -q                 # 54 项：核心/网页/一致性 + 备份恢复；含两个真实 uvicorn 进程的并发与恢复测试
python scripts/selftest_dual.py     # 端到端演示：起两个真实进程 + 共享目录，覆盖备份、并发恢复、旧页面过期与取数
```

## 已知限制

- 单机 SQLite + 共享卷方案适用于演示与中小规模；跨主机需换成服务端数据库与对象存储。
- 网页为服务端渲染，无前端路由；并发冲突通过版本号拒绝，不做自动合并。
- 删除只做逻辑删除，Parquet 中历史字段的物理列会一直保留（这是“不重写历史”的代价）。
- 沙箱环境无 `docker`，Compose 配置与镜像构建未在本机实际运行；已用两个真实
  uvicorn 进程共享目录完成同等并发与恢复验证（`scripts/selftest_dual.py`）。
- 备份在服务端临时目录完整生成后再返回，备份体积受磁盘剩余空间限制；
  恢复上传默认上限为 4 GiB（`lake.MAX_BACKUP_BYTES`）。
- 旧代际在切换后保留 1 小时后清理；此刻仍在读的请求若超过 1 小时会失败，
  而不是读到半份数据。
- 整湖恢复不支持“合并”或“部分批次恢复”；恢复即回到备份时点。
