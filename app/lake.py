"""数据湖核心：结构版本、字段身份、批次登记与不可变 Parquet。

设计要点
--------
* 测量列的物理标识是永久不变的 ``field_id``；Parquet 中保存的就是 field_id。
  列名只是 ``fields`` 表上的一个可修改属性。
* 删除字段仅把状态置为 deleted。之后用同名新增会得到新的 field_id，
  旧批次里没有新 id 的列，读取时补 NULL，因此不会串值。
* 结构变更与批次登记都在 SQLite ``BEGIN IMMEDIATE`` 事务中完成，并校验
  调用方看到的 schema_version；过期则整次拒绝。
* 新 Parquet 先写临时文件、fsync、rename，再登记数据库。数据库提交失败会
  删除刚写的文件。历史 Parquet 永不以写方式打开。
* 一次页面/API 查询只在一个 SQLite 读事务里取得“结构 + 批次登记”快照。

整湖备份 / 恢复
--------------
* 数据按“代际目录”组织：``data_dir/generations/g_<uuid>/`` 内含一个
  ``lake.sqlite3`` 与 ``parquet/``。``data_dir/control.sqlite3`` 只保存当前
  代际、epoch 与恢复审计，是跨进程的 CAS 协调点。
* 备份在一个已提交读事务中取字段身份、结构与批次清单，再打包各批次不可变
  Parquet 原件；打包期间其他实例仍可提交上传 / 改列。
* 恢复先在 ``data_dir/staging/`` 完整校验并构建新代际，随后持控制库写锁，
  用目录 rename + 控制指针更新原子切换。读者要么解析到旧代际，要么解析到
  新代际，永远看不到半份数据。
* 每次切换 epoch + 1；页面 / API 写入必须同时携带结构版本与代际 token。
  旧页面在切换后即使版本号相同也会被拒绝，切换瞬间尚未提交的上传同样被拒。
"""

from __future__ import annotations

import csv
import errno
import fcntl
import hashlib
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import stat
import time
import unicodedata
import uuid
import zipfile
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("metlake")

MAX_FIELDS = 8
MAX_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_STATION_LENGTH = 256
MAX_FIELD_NAME_LENGTH = 64

# 备份 / 恢复资源上限（防止 ZIP 炸弹、磁盘耗尽）。
MAX_BACKUP_BYTES = 4 * 1024 * 1024 * 1024
MAX_BACKUP_UNPACKED_BYTES = 8 * 1024 * 1024 * 1024
MAX_BACKUP_ENTRIES = 100_000
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
BACKUP_FORMAT = "metlake-backup"
BACKUP_FORMAT_VERSION = 1

STATION_COLUMN = "station_id"
TIME_COLUMN = "observed_at"
RESERVED_NAMES = {STATION_COLUMN.casefold(), TIME_COLUMN.casefold()}
FIELD_ID_RE = re.compile(r"^f_[0-9a-f]{32}$")
GENERATION_ID_RE = re.compile(r"^g_[0-9a-f]{32}$")
BATCH_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
INT_RE = re.compile(r"^[+-]?\d+$")
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

SCHEMA_VERSION_KEY = "schema_version"
UPLOAD_METADATA_PREFIX = b"metlake."


class LakeError(Exception):
    """可安全展示给用户的领域错误。"""

    status_code = 400
    code = "bad_request"


class ValidationError(LakeError):
    status_code = 400
    code = "validation"


class StaleSchema(LakeError):
    status_code = 409
    code = "stale_schema"

    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"结构版本已过期：本次操作基于 v{expected}，当前已提交结构为 v{actual}。"
            "为避免旧结构写入，整次操作已被拒绝，请刷新页面后重试。"
        )


class NotFound(LakeError):
    status_code = 404
    code = "not_found"


class StorageError(LakeError):
    status_code = 500
    code = "storage_error"


class BatchIntegrityError(LakeError):
    status_code = 500
    code = "integrity_error"


class StaleGeneration(LakeError):
    status_code = 409
    code = "stale_generation"

    def __init__(self, detail: str) -> None:
        super().__init__(detail)


class BackupRejected(LakeError):
    status_code = 400
    code = "backup_rejected"


# ---------------------------------------------------------------------------
# 配置 / 数据库基础
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    instance_name: str = "local"

    @classmethod
    def from_env(
        cls,
        data_dir: str | os.PathLike[str] | None = None,
        instance_name: str | None = None,
    ) -> "Settings":
        resolved = Path(data_dir or os.environ.get("DATA_DIR", "./data")).expanduser()
        name = instance_name or os.environ.get("INSTANCE_NAME", "local")
        return cls(data_dir=resolved.resolve(), instance_name=name)

    @property
    def control_db_path(self) -> Path:
        """跨进程协调库：当前代际、epoch、恢复审计。"""

        return self.data_dir / "control.sqlite3"

    @property
    def generations_dir(self) -> Path:
        return self.data_dir / "generations"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    def generation_dir(self, generation_id: str) -> Path:
        return self.generations_dir / generation_id

    def generation_db_path(self, generation_id: str) -> Path:
        return self.generation_dir(generation_id) / "lake.sqlite3"

    def generation_parquet_dir(self, generation_id: str) -> Path:
        return self.generation_dir(generation_id) / "parquet"

    def ensure_dirs(self) -> None:
        for path in (
            self.data_dir,
            self.generations_dir,
            self.staging_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    # 兼容旧代码/测试：指向“当前代际”的路径。写路径内部一律使用显式代际。
    @property
    def db_path(self) -> Path:
        return self.generation_db_path(current_generation(self).id)

    @property
    def parquet_dir(self) -> Path:
        return self.generation_parquet_dir(current_generation(self).id)


@dataclass(frozen=True)
class Generation:
    id: str
    epoch: int
    created_at: str

    @property
    def token(self) -> str:
        return self.id


@dataclass(frozen=True)
class Field:
    id: str
    name: str
    ordinal: int
    status: str
    created_version: int
    deleted_version: int | None


@dataclass(frozen=True)
class Schema:
    version: int
    fields: tuple[Field, ...]

    @property
    def active(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "active")

    @property
    def deleted(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "deleted")


@dataclass(frozen=True)
class Batch:
    id: str
    created_at: str
    schema_version: int
    row_count: int
    file_name: str
    file_sha256: str
    fields_at_upload: tuple[dict, ...]
    schema_json: str = "[]"

    @property
    def short_id(self) -> str:
        return self.id[:8]

    def schema_json_for_backup(self) -> str:
        return self.schema_json


@dataclass(frozen=True)
class LakeState:
    schema: Schema
    batches: tuple[Batch, ...]
    generation: Generation

    @property
    def version(self) -> int:
        return self.schema.version

    @property
    def token(self) -> str:
        return self.generation.id


@dataclass(frozen=True)
class BatchView:
    schema: Schema
    batch: Batch
    table: pa.Table
    generation: Generation


@dataclass(frozen=True)
class ParsedCsv:
    stations: tuple[str, ...]
    times: tuple[datetime, ...]
    values: dict[str, tuple[int | None, ...]]
    header_fields: tuple[Field, ...]


@dataclass(frozen=True)
class IntegrityReport:
    ok: bool
    batches: int
    missing_files: tuple[str, ...]
    hash_mismatches: tuple[str, ...]
    orphan_files: tuple[str, ...]
    temp_files: tuple[str, ...]


@dataclass(frozen=True)
class BackupSnapshot:
    generation: Generation
    schema: Schema
    fields: tuple[dict, ...]
    batches: tuple[Batch, ...]


@dataclass(frozen=True)
class RestoreResult:
    generation: Generation
    previous_generation: Generation
    schema_version: int
    batches: int


@dataclass(frozen=True)
class _RestorePlan:
    directory: Path
    generation: Generation
    schema_version: int
    batch_count: int


# 仅测试使用：Parquet 已写完、数据库提交前的钩子。
_before_commit_hook: Callable[[], None] | None = None


def set_before_commit_hook(hook: Callable[[], None] | None) -> None:
    """测试钩子：在转换完成、批次登记提交前执行。生产路径不设置。"""

    global _before_commit_hook
    _before_commit_hook = hook


# ---------------------------------------------------------------------------
# SQLite 连接
# ---------------------------------------------------------------------------

CONTROL_CURRENT_KEY = "current_generation"
CONTROL_EPOCH_KEY = "epoch"
CONTROL_CREATED_KEY = "generation_created_at"

# 旧代际在切换后保留一段时间，供已在进行中的查询读完；之后才清理。
GENERATION_RETENTION_SECONDS = 3600
STAGING_RETENTION_SECONDS = 3600


def _open_sqlite(
    path: Path, *, wal: bool = False, retry: int = 0
) -> sqlite3.Connection:
    last_error: Exception | None = None
    for attempt in range(max(1, retry)):
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                path,
                timeout=30.0,
                isolation_level=None,  # 手动控制事务
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 15000")
            if wal:
                mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if str(mode).lower() != "wal":
                    log.warning("SQLite journal_mode=%s (期望 wal) %s", mode, path)
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except sqlite3.Error as exc:
            last_error = exc
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            if attempt + 1 < retry:
                time.sleep(min(0.05 * (attempt + 1), 0.5))
    raise StorageError(f"无法打开数据库 {path}：{last_error}") from last_error


def _connect_control(settings: Settings) -> sqlite3.Connection:
    return _open_sqlite(settings.control_db_path)


def _connect_generation(
    settings: Settings, generation: Generation
) -> sqlite3.Connection:
    path = settings.generation_db_path(generation.id)
    if not path.is_file():
        # 不能让 sqlite3.connect 顺手创建一个空库：那会把“代际已被回收”
        # 伪装成“空结构”。
        raise StorageError(
            f"数据湖代际 {generation.id} 的数据库不存在；请刷新后重试。"
        )
    return _open_sqlite(path)


# ---------------------------------------------------------------------------
# 控制库：当前代际指针与恢复审计
# ---------------------------------------------------------------------------


def _init_control_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS control (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS restores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            at TEXT NOT NULL,
            instance TEXT NOT NULL,
            source_generation TEXT,
            new_generation TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            schema_version INTEGER NOT NULL,
            batches INTEGER NOT NULL,
            detail TEXT NOT NULL
        )
        """
    )


def _query_generation(conn: sqlite3.Connection) -> Generation | None:
    """在调用方事务内读取当前代际；没有则返回 None。"""

    values = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM control").fetchall()
    }
    generation_id = values.get(CONTROL_CURRENT_KEY)
    if not generation_id:
        return None
    if not GENERATION_ID_RE.fullmatch(generation_id):
        raise BatchIntegrityError(f"控制库中的代际标识非法：{generation_id!r}")
    try:
        epoch = int(values.get(CONTROL_EPOCH_KEY, "0"))
    except ValueError as exc:
        raise BatchIntegrityError("控制库中的 epoch 非整数。") from exc
    return Generation(
        id=generation_id,
        epoch=epoch,
        created_at=values.get(CONTROL_CREATED_KEY, ""),
    )


def current_generation(settings: Settings) -> Generation:
    """返回当前已提交代际；调用一次后，本次操作始终使用该代际。"""

    conn = _connect_control(settings)
    try:
        conn.execute("BEGIN")
        try:
            generation = _query_generation(conn)
        finally:
            conn.execute("COMMIT")
    except sqlite3.Error as exc:
        raise StorageError("读取当前数据湖代际失败。") from exc
    finally:
        conn.close()
    if generation is None:
        raise StorageError("数据湖尚未初始化。")
    return generation


def _set_current_generation(
    conn: sqlite3.Connection, generation: Generation
) -> None:
    for key, value in (
        (CONTROL_CURRENT_KEY, generation.id),
        (CONTROL_EPOCH_KEY, str(generation.epoch)),
        (CONTROL_CREATED_KEY, generation.created_at),
    ):
        conn.execute(
            "INSERT INTO control(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


# ---------------------------------------------------------------------------
# 代际数据库
# ---------------------------------------------------------------------------


def _init_generation_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fields (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
            created_version INTEGER NOT NULL,
            deleted_version INTEGER,
            created_at TEXT NOT NULL,
            deleted_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_fields_active_name
        ON fields(name COLLATE NOCASE)
        WHERE status = 'active'
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_fields_status_ordinal "
        "ON fields(status, ordinal)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS batches (
            id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            file_name TEXT NOT NULL UNIQUE,
            file_sha256 TEXT NOT NULL,
            schema_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_batches_created_at "
        "ON batches(created_at DESC)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES (?, '0')",
        (SCHEMA_VERSION_KEY,),
    )


def _initialize_generation_db(settings: Settings, generation: Generation) -> None:
    path = settings.generation_db_path(generation.id)
    settings.generation_parquet_dir(generation.id).mkdir(parents=True, exist_ok=True)
    conn = _open_sqlite(path, wal=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_generation_schema(conn)
        conn.execute("COMMIT")
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise StorageError(f"无法初始化数据湖代际 {generation.id}。") from exc
    finally:
        conn.close()


def _migrate_legacy_layout(settings: Settings) -> Generation | None:
    """把旧版 data_dir/lake.sqlite3 + parquet/ 迁入代际目录。

    只在控制库尚无当前代际时调用，且调用方已持有控制库写锁。先 rename 数据库
    （数据库是权威标记），再 rename 目录；若在两步之间崩溃，下次启动会找回已
    rename 的数据库并把遗留的 parquet 目录补进同一代际。
    """

    legacy_db = settings.data_dir / "lake.sqlite3"
    legacy_parquet = settings.data_dir / "parquet"
    pending_marker = settings.generations_dir / "pending-migration.json"

    # 兼容符号链接不是旧布局数据，绝不能当作迁移源。
    legacy_db_is_real = legacy_db.is_file() and not legacy_db.is_symlink()
    legacy_parquet_is_real = legacy_parquet.is_dir() and not legacy_parquet.is_symlink()

    if not legacy_db_is_real:
        # 崩溃恢复：数据库已迁走、parquet 目录可能还没迁。
        if pending_marker.is_file():
            try:
                pending = json.loads(pending_marker.read_text("utf-8"))
                generation = Generation(
                    id=pending["generation"],
                    epoch=int(pending.get("epoch", 0)),
                    created_at=pending.get("created_at", _utc_now_iso()),
                )
            except (OSError, ValueError, KeyError, TypeError):
                pending_marker.unlink(missing_ok=True)
                return None
            if not settings.generation_db_path(generation.id).is_file():
                pending_marker.unlink(missing_ok=True)
                return None
            target_parquet = settings.generation_parquet_dir(generation.id)
            if legacy_parquet_is_real and not target_parquet.exists():
                os.replace(legacy_parquet, target_parquet)
            target_parquet.mkdir(parents=True, exist_ok=True)
            pending_marker.unlink(missing_ok=True)
            log.warning("已完成中断的旧布局迁移：%s", generation.id)
            return generation
        return None

    generation = Generation(
        id=f"g_{uuid.uuid4().hex}", epoch=0, created_at=_utc_now_iso()
    )
    target = settings.generation_dir(generation.id)
    target.mkdir(parents=True, exist_ok=False)
    try:
        temp = pending_marker.with_name(pending_marker.name + ".tmp")
        temp.write_text(
            json.dumps(
                {
                    "generation": generation.id,
                    "epoch": generation.epoch,
                    "created_at": generation.created_at,
                }
            ),
            "utf-8",
        )
        _fsync_path(temp)
        os.replace(temp, pending_marker)
        _fsync_dir(settings.generations_dir)

        conn = _open_sqlite(legacy_db)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
        moved_db = False
        try:
            os.replace(legacy_db, settings.generation_db_path(generation.id))
            moved_db = True
            _fsync_dir(target)
            target_parquet = settings.generation_parquet_dir(generation.id)
            if legacy_parquet_is_real:
                os.replace(legacy_parquet, target_parquet)
            else:
                target_parquet.mkdir(parents=True, exist_ok=True)
            for suffix in ("-wal", "-shm"):
                _safe_unlink(Path(str(legacy_db) + suffix))
            _fsync_dir(target)
            _fsync_dir(settings.data_dir)
        except BaseException:
            if moved_db:
                # 数据库已经搬进目标代际：保留它和 pending 标记，下次启动续完
                # 迁移；此时删除目标目录会把唯一一份湖数据一起删掉。
                log.exception("旧布局迁移中断，已保留迁移标记供下次启动续完")
            else:
                shutil.rmtree(target, ignore_errors=True)
                pending_marker.unlink(missing_ok=True)
            raise
        pending_marker.unlink(missing_ok=True)
    except BaseException:
        # 标记文件之前失败：目标代际尚未承载任何旧数据，可安全回收。
        shutil.rmtree(target, ignore_errors=True)
        raise
    log.warning("已将旧布局迁移到代际 %s", generation.id)
    return generation


def initialize(settings: Settings) -> None:
    """幂等初始化；两个实例同时启动也安全。"""

    settings.ensure_dirs()
    conn = _open_sqlite(settings.control_db_path, wal=True, retry=60)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_control_schema(conn)
        generation = _query_generation(conn)
        if generation is None:
            generation = _migrate_legacy_layout(settings)
            if generation is None:
                generation = Generation(
                    id=f"g_{uuid.uuid4().hex}",
                    epoch=0,
                    created_at=_utc_now_iso(),
                )
                _initialize_generation_db(settings, generation)
            _set_current_generation(conn, generation)
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()

    _init_generation_db_safely(settings, current_generation(settings))
    _recover_after_restart(settings)


def _init_generation_db_safely(settings: Settings, generation: Generation) -> None:
    """确保代际数据库表结构存在（迁移或异常中断后）。"""

    path = settings.generation_db_path(generation.id)
    if not path.is_file():
        raise StorageError(
            f"当前代际 {generation.id} 的数据库缺失，拒绝以半份数据启动。"
        )
    conn = _connect_generation(settings, generation)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_generation_schema(conn)
        conn.execute("COMMIT")
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise StorageError("校验数据湖代际结构失败。") from exc
    finally:
        conn.close()


def _recover_after_restart(settings: Settings) -> None:
    """清理恢复中断留下的暂存目录，并回收过期代际。

    恢复流程保证：控制指针只会指向已完整落盘并 fsync 的代际；因此这里删除
    未引用的 staging / 旧代际不会让任何读者看到半份数据。
    """

    try:
        generation = current_generation(settings)
    except LakeError:
        return
    _refresh_compat_paths(settings, generation)
    _cleanup_staging(settings)
    _cleanup_generations(settings, generation)
    try:
        _cleanup_stale_temps(settings, generation)
        _cleanup_orphan_files(settings, generation)
    except LakeError:
        log.warning("启动清理未完成；数据湖仍保持当前代际。")


def _refresh_compat_paths(settings: Settings, generation: Generation) -> None:
    """把旧布局的两个路径做成指向当前代际的符号链接。

    数据湖内部只通过代际目录访问数据；这两个链接仅用于兼容旧的运维脚本
    与文档路径，不参与事务，因此刷新失败不会影响一致性。
    """

    links = {
        "lake.sqlite3": settings.generation_db_path(generation.id),
        "parquet": settings.generation_parquet_dir(generation.id),
    }
    for name, target in links.items():
        link = settings.data_dir / name
        relative = os.path.relpath(target, settings.data_dir)
        try:
            if link.is_symlink():
                if os.readlink(link) == relative:
                    continue
                tmp = settings.data_dir / f".{name}.{uuid.uuid4().hex}.link"
                os.symlink(relative, tmp)
                os.replace(tmp, link)
            elif link.exists():
                # 兼容路径被真实文件/目录占用时不动它，避免误删用户数据。
                continue
            else:
                os.symlink(relative, link)
        except OSError:
            log.warning("刷新兼容路径 %s 失败", link)


@contextmanager
def _restore_lock(settings: Settings) -> Iterator[None]:
    """跨进程恢复互斥：同一时刻只允许一个实例执行整湖恢复。

    锁只覆盖“校验 + 切换”，不覆盖普通读写；准备期间当前湖仍可读写。
    """

    lock_path = settings.data_dir / "restore.lock"
    handle = open(lock_path, "a+b")
    locked = False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            if exc.errno in (errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOLCK):
                log.warning("文件系统不支持恢复互斥锁，改用代际 CAS：%s", exc)
            else:
                raise StaleGeneration(
                    "另一个实例正在进行整湖恢复，本次恢复未执行。"
                    "请等待其完成并刷新页面。"
                ) from exc
        yield
    finally:
        if locked:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _cleanup_staging(settings: Settings, retention: float = STAGING_RETENTION_SECONDS) -> None:
    cutoff = time.time() - retention
    try:
        candidates = list(settings.staging_dir.iterdir())
    except OSError:
        return
    for path in candidates:
        try:
            if path.stat().st_mtime < cutoff:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                log.warning("清理恢复暂存：%s", path.name)
        except OSError:
            log.warning("无法清理恢复暂存 %s", path)


def _cleanup_generations(
    settings: Settings,
    current: Generation,
    retention: float = GENERATION_RETENTION_SECONDS,
) -> None:
    cutoff = time.time() - retention
    try:
        candidates = [p for p in settings.generations_dir.iterdir() if p.is_dir()]
    except OSError:
        return
    for path in candidates:
        if path.name == current.id:
            continue
        if not GENERATION_ID_RE.fullmatch(path.name):
            continue
        try:
            if path.stat().st_mtime < cutoff:
                shutil.rmtree(path, ignore_errors=True)
                log.warning("回收已切换的旧代际：%s", path.name)
        except OSError:
            log.warning("无法回收旧代际 %s", path)


@contextmanager
def _mutation_transaction(
    settings: Settings,
    generation: Generation,
    generation_token: str | None,
) -> Iterator[sqlite3.Connection]:
    """受代际指针保护的写事务。

    锁顺序固定为“代际库 -> 控制库”。恢复切换只持有控制库写锁、不再获取代际
    库锁，因此不会死锁。持有控制库写锁直到代际库 COMMIT，保证恢复切换不会
    发生在“已校验代际”与“写提交”之间：切换后旧代际的写入会整体回滚并以
    409 拒绝，绝不会悄悄写进已被替换的湖。
    """

    if generation_token is not None and generation_token != generation.id:
        raise StaleGeneration(
            "数据湖已被整体恢复或替换，本页面对应的旧湖已失效。"
            "本次修改未写入任何数据湖，请刷新页面后重试。"
        )
    gen_conn = _connect_generation(settings, generation)
    ctrl_conn: sqlite3.Connection | None = None
    committed = False
    try:
        gen_conn.execute("BEGIN IMMEDIATE")
        ctrl_conn = _connect_control(settings)
        ctrl_conn.execute("BEGIN IMMEDIATE")
        current = _query_generation(ctrl_conn)
        if current is None or current.id != generation.id:
            raise StaleGeneration(
                "数据湖在本次操作提交前已被整体恢复或替换，旧湖写事务已拒绝。"
                "本次修改未写入新湖，请刷新页面后重试。"
            )
        yield gen_conn
        gen_conn.execute("COMMIT")
        committed = True
        # 控制库事务没有数据改动，提交仅用于释放写锁。
        try:
            ctrl_conn.execute("COMMIT")
        except sqlite3.Error:
            # 代际写已提交且代际仍是当前代际；控制库提交失败不影响结果。
            log.exception("释放控制库写锁失败")
    except BaseException:
        if not committed:
            try:
                gen_conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        if ctrl_conn is not None:
            try:
                if not committed:
                    ctrl_conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            try:
                ctrl_conn.close()
            except sqlite3.Error:
                pass
        try:
            gen_conn.close()
        except sqlite3.Error:
            log.exception("关闭数据库连接失败")


@contextmanager
def _read_transaction(
    settings: Settings, generation: Generation | None = None
) -> Iterator[tuple[Generation, sqlite3.Connection]]:
    """在单个已提交读事务中读取某个代际；代际在打开连接前固定。"""

    resolved = generation or current_generation(settings)
    conn = _connect_generation(settings, resolved)
    try:
        conn.execute("BEGIN")
        try:
            yield resolved, conn
        finally:
            conn.execute("COMMIT")
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            log.exception("关闭数据库连接失败")


def _rows_to_batch(row: sqlite3.Row) -> Batch:
    raw_schema = row["schema_json"]
    try:
        fields = tuple(json.loads(raw_schema))
    except (TypeError, ValueError):
        fields = ()
    return Batch(
        id=row["id"],
        created_at=row["created_at"],
        schema_version=int(row["schema_version"]),
        row_count=int(row["row_count"]),
        file_name=row["file_name"],
        file_sha256=row["file_sha256"],
        fields_at_upload=fields,
        schema_json=raw_schema,
    )


def _query_schema(conn: sqlite3.Connection) -> Schema:
    version_row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if version_row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    version = int(version_row["value"])
    rows = conn.execute(
        "SELECT id, name, ordinal, status, created_version, deleted_version "
        "FROM fields ORDER BY ordinal ASC, created_at ASC"
    ).fetchall()
    fields = tuple(
        Field(
            id=r["id"],
            name=r["name"],
            ordinal=int(r["ordinal"]),
            status=r["status"],
            created_version=int(r["created_version"]),
            deleted_version=(
                int(r["deleted_version"]) if r["deleted_version"] is not None else None
            ),
        )
        for r in rows
    )
    return Schema(version=version, fields=fields)


def read_state(
    settings: Settings, generation: Generation | None = None
) -> LakeState:
    """在一个读事务中取得一致的结构 + 批次登记快照。

    代际在打开连接前固定：恢复切换后，本次查询仍读取完整旧代际；新查询读取
    完整新代际，不会混合。
    """

    try:
        with _read_transaction(settings, generation) as (resolved, conn):
            schema = _query_schema(conn)
            rows = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()
            batches = tuple(_rows_to_batch(r) for r in rows)
    except sqlite3.Error as exc:
        raise StorageError("读取数据湖状态失败。") from exc
    return LakeState(schema=schema, batches=batches, generation=resolved)


def read_schema(settings: Settings, generation: Generation | None = None) -> Schema:
    try:
        with _read_transaction(settings, generation) as (_, conn):
            return _query_schema(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取结构失败。") from exc


# ---------------------------------------------------------------------------
# 字段结构变更
# ---------------------------------------------------------------------------


def normalize_field_name(raw: str | None) -> str:
    if raw is None:
        raise ValidationError("字段名不能为空。")
    name = unicodedata.normalize("NFC", str(raw).strip())
    if not name:
        raise ValidationError("字段名不能为空。")
    if len(name) > MAX_FIELD_NAME_LENGTH:
        raise ValidationError(f"字段名最长 {MAX_FIELD_NAME_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        raise ValidationError("字段名不能包含控制字符。")
    if name.casefold() in RESERVED_NAMES:
        raise ValidationError(
            f"“{name}”是固定列名（{STATION_COLUMN} / {TIME_COLUMN}），不能用作测量列名。"
        )
    return name


def _read_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    return int(row["value"])


def _check_expected_version(conn: sqlite3.Connection, expected: int) -> int:
    actual = _read_version(conn)
    if expected != actual:
        raise StaleSchema(expected, actual)
    return actual


def _bump_version(conn: sqlite3.Connection, current: int) -> int:
    new_version = current + 1
    conn.execute(
        "UPDATE meta SET value = ? WHERE key = ?",
        (str(new_version), SCHEMA_VERSION_KEY),
    )
    return new_version


def _next_ordinal(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(ordinal), -1) AS m FROM fields").fetchone()
    return int(row["m"]) + 1


def _ensure_name_free(
    conn: sqlite3.Connection, name: str, exclude_id: str | None = None
) -> None:
    rows = conn.execute(
        "SELECT id, name FROM fields WHERE status = 'active'"
    ).fetchall()
    folded = name.casefold()
    for row in rows:
        if exclude_id is not None and row["id"] == exclude_id:
            continue
        if row["name"].casefold() == folded:
            raise ValidationError(f"测量列“{name}”已存在。")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def add_field(
    settings: Settings,
    expected_version: int,
    raw_name: str,
    generation_token: str | None = None,
) -> Schema:
    name = normalize_field_name(raw_name)
    generation = current_generation(settings)
    try:
        with _mutation_transaction(
            settings, generation, generation_token
        ) as conn:
            version = _check_expected_version(conn, expected_version)
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM fields WHERE status = 'active'"
            ).fetchone()["n"]
            if int(active) >= MAX_FIELDS:
                raise ValidationError(
                    f"测量列最多 {MAX_FIELDS} 个，请先删除不再使用的列。"
                )
            _ensure_name_free(conn, name)
            new_version = _bump_version(conn, version)
            conn.execute(
                "INSERT INTO fields(id, name, ordinal, status, created_version, "
                "deleted_version, created_at, deleted_at) "
                "VALUES (?, ?, ?, 'active', ?, NULL, ?, NULL)",
                (
                    f"f_{uuid.uuid4().hex}",
                    name,
                    _next_ordinal(conn),
                    new_version,
                    _utc_now_iso(),
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("新增测量列失败，结构未改变。") from exc
    return read_schema(settings, generation)


def rename_field(
    settings: Settings,
    expected_version: int,
    field_id: str,
    raw_name: str,
    generation_token: str | None = None,
) -> Schema:
    name = normalize_field_name(raw_name)
    generation = current_generation(settings)
    try:
        with _mutation_transaction(
            settings, generation, generation_token
        ) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id, name FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要改名的测量列不存在或已删除。")
            if row["name"] == name:
                raise ValidationError("新名称与当前名称相同。")
            _ensure_name_free(conn, name, exclude_id=field_id)
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET name = ? WHERE id = ?", (name, field_id)
            )
            log.info("renamed %s -> %s at v%s", field_id, name, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("测量列改名失败，结构未改变。") from exc
    return read_schema(settings, generation)


def delete_field(
    settings: Settings,
    expected_version: int,
    field_id: str,
    generation_token: str | None = None,
) -> Schema:
    generation = current_generation(settings)
    try:
        with _mutation_transaction(
            settings, generation, generation_token
        ) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要删除的测量列不存在或已删除。")
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET status = 'deleted', deleted_version = ?, "
                "deleted_at = ? WHERE id = ?",
                (new_version, _utc_now_iso(), field_id),
            )
            log.info("deleted %s at v%s", field_id, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("删除测量列失败，结构未改变。") from exc
    return read_schema(settings, generation)


# ---------------------------------------------------------------------------
# CSV -> Parquet
# ---------------------------------------------------------------------------


def parse_csv(data: bytes, schema: Schema) -> ParsedCsv:
    if not data:
        raise ValidationError("上传文件为空。")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValidationError(
            f"CSV 文件不能超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MiB。"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须是 UTF-8 编码（可带 BOM）。") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise ValidationError("CSV 缺少表头。") from None

    header = [unicodedata.normalize("NFC", h.strip()) for h in raw_header]
    if not header or all(not h for h in header):
        raise ValidationError("CSV 表头为空。")
    if any(not h for h in header):
        raise ValidationError("CSV 表头中存在空列名。")
    if len(set(header)) != len(header):
        raise ValidationError("CSV 表头中存在重复列名。")

    fixed: dict[str, int] = {}
    for required in (STATION_COLUMN, TIME_COLUMN):
        try:
            fixed[required] = header.index(required)
        except ValueError:
            raise ValidationError(
                f"CSV 必须包含固定列“{required}”。"
            ) from None
        if header.count(required) != 1:
            raise ValidationError(f"固定列“{required}”只能出现一次。")

    active = schema.active
    by_name = {f.name: f for f in active}
    selected: dict[str, Field] = {}
    unknown: list[str] = []
    for idx, name in enumerate(header):
        if idx in fixed.values():
            continue
        field = by_name.get(name)
        if field is None:
            unknown.append(name)
        else:
            selected[name] = field
    if unknown:
        available = "、".join(f.name for f in active) or "（无）"
        raise ValidationError(
            "CSV 含当前结构之外的测量列："
            + "、".join(unknown)
            + f"。当前可接收的测量列为：{available}。"
            "请刷新页面使用最新结构导出/上传。"
        )

    stations: list[str] = []
    times: list[datetime] = []
    values: dict[str, list[int | None]] = {
        field.id: [] for field in selected.values()
    }

    station_idx = header.index(STATION_COLUMN)
    time_idx = header.index(TIME_COLUMN)
    selected_by_index = {
        idx: field for idx, name in enumerate(header) if (field := selected.get(name))
    }

    row_count = 0
    for row in reader:
        if not row:
            continue
        row_count += 1
        if row_count > MAX_ROWS:
            raise ValidationError(f"每批最多 {MAX_ROWS} 行数据。")
        line_no = reader.line_num
        if len(row) != len(header):
            raise ValidationError(
                f"CSV 第 {line_no} 行列数为 {len(row)}，表头为 {len(header)} 列。"
            )

        station = row[station_idx].strip()
        if not station:
            raise ValidationError(f"CSV 第 {line_no} 行站号为空。")
        if len(station) > MAX_STATION_LENGTH:
            raise ValidationError(f"CSV 第 {line_no} 行站号过长。")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
            raise ValidationError(f"CSV 第 {line_no} 行站号含控制字符。")
        stations.append(station)

        raw_time = row[time_idx].strip()
        if not raw_time:
            raise ValidationError(f"CSV 第 {line_no} 行 UTC 时刻为空。")
        try:
            parsed_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(
                f"CSV 第 {line_no} 行时刻“{raw_time}”不是 ISO 8601 格式。"
            ) from None
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        else:
            parsed_time = parsed_time.astimezone(timezone.utc)
        times.append(parsed_time)

        for idx, field in selected_by_index.items():
            raw_value = row[idx].strip()
            if raw_value == "":
                parsed_value: int | None = None
            else:
                if not INT_RE.fullmatch(raw_value):
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值“{raw_value}”"
                        "不是整数。空值请留空。"
                    )
                parsed_value = int(raw_value)
                if parsed_value < INT64_MIN or parsed_value > INT64_MAX:
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值超出 64 位整数范围。"
                    )
            values[field.id].append(parsed_value)

    if row_count == 0:
        raise ValidationError("CSV 至少需要 1 行数据。")

    filled: dict[str, tuple[int | None, ...]] = {}
    for field in active:
        if field.id in values:
            filled[field.id] = tuple(values[field.id])
        else:
            filled[field.id] = tuple([None] * row_count)

    return ParsedCsv(
        stations=tuple(stations),
        times=tuple(times),
        values=filled,
        header_fields=tuple(selected.values()),
    )


def _build_upload_table(parsed: ParsedCsv, schema: Schema) -> pa.Table:
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        pa.array(parsed.stations, type=pa.string()),
        pa.array(parsed.times, type=pa.timestamp("us", tz="UTC")),
    ]
    for field in schema.active:
        names.append(field.id)
        arrays.append(pa.array(parsed.values[field.id], type=pa.int64()))
    return pa.Table.from_arrays(arrays, names=names)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    try:
        _fsync_path(path)
    except OSError as exc:  # 某些文件系统不支持目录 fsync
        log.warning("目录 fsync 失败 %s: %s", path, exc)


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        log.exception("清理文件失败 %s", path)


def _cleanup_stale_temps(settings: Settings, generation: Generation) -> None:
    """清理某代际中超过 1 小时的临时文件。

    新写入的临时文件命名以点号开头（``.xxx.tmp``），这里的 glob 覆盖两种形态；
    用“足够旧”阈值避免误删另一实例正在写入的临时文件。
    """

    cutoff = time.time() - 3600
    parquet_dir = settings.generation_parquet_dir(generation.id)
    try:
        candidates = [
            p
            for p in parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描临时文件失败")
        return
    for path in candidates:
        try:
            path.unlink(missing_ok=True)
            log.warning("清理陈旧临时文件：%s", path.name)
        except OSError:
            log.warning("无法清理临时文件 %s", path)


def _cleanup_orphan_files(settings: Settings, generation: Generation) -> None:
    """清理某代际中崩溃遗留的、未被任何批次登记的 Parquet。

    只处理超过 1 小时的文件，避免误删另一实例正在转换 / 即将登记的新文件。
    """

    cutoff = time.time() - 3600
    parquet_dir = settings.generation_parquet_dir(generation.id)
    try:
        candidates = [
            p
            for p in parquet_dir.glob("*.parquet")
            if p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描孤立 Parquet 失败")
        return
    if not candidates:
        return
    try:
        registered = {b.file_name for b in read_state(settings, generation).batches}
    except LakeError:
        return
    for path in candidates:
        if path.name in registered:
            continue
        try:
            # 再确认一次：另一实例可能刚登记该文件
            if path.name in {
                b.file_name for b in read_state(settings, generation).batches
            }:
                continue
            path.unlink(missing_ok=True)
            log.warning("清理未登记的孤立 Parquet：%s", path.name)
        except (OSError, LakeError):
            log.warning("无法清理孤立 Parquet：%s", path.name)


def _write_parquet_atomic(
    table: pa.Table, final_path: Path, metadata: dict[bytes, bytes]
) -> str:
    """临时文件 -> fsync -> rename -> fsync 目录；返回内容 sha256。"""

    table = table.replace_schema_metadata(metadata)
    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path, compression="snappy")
        _fsync_path(tmp_path)
        digest = _sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return digest
    except BaseException:
        _safe_unlink(tmp_path)
        raise


# ---------------------------------------------------------------------------
# 批次上传与读取
# ---------------------------------------------------------------------------


def upload_csv(
    settings: Settings,
    expected_version: int,
    data: bytes,
    generation_token: str | None = None,
) -> Batch:
    """解析并转换后，在最终提交前再次校验结构版本与代际。

    转换/写文件期间不持有数据库写锁，因此结构仍可被另一进程修改；提交时
    若版本已变化或整湖已被恢复切换，则批次绝不登记，刚写的 Parquet 也会被
    清理。上传开始时所在的代际全程固定：切换后不会把文件写进新湖。
    """

    generation = current_generation(settings)
    if generation_token is not None and generation_token != generation.id:
        raise StaleGeneration(
            "数据湖已被整体恢复或替换，本页面对应的旧湖已失效；"
            "上传未开始，请刷新页面后重试。"
        )
    schema = read_schema(settings, generation)
    if schema.version != expected_version:
        raise StaleSchema(expected_version, schema.version)

    parsed = parse_csv(data, schema)
    table = _build_upload_table(parsed, schema)

    batch_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    file_name = f"{batch_id}.parquet"
    final_path = settings.generation_parquet_dir(generation.id) / file_name
    fields_at_upload = [
        {"id": f.id, "name": f.name, "ordinal": f.ordinal} for f in schema.active
    ]
    schema_json = json.dumps(fields_at_upload, ensure_ascii=False, separators=(",", ":"))
    metadata = {
        UPLOAD_METADATA_PREFIX + b"format": b"1",
        UPLOAD_METADATA_PREFIX + b"batch_id": batch_id.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"schema_version": str(schema.version).encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"created_at": created_at.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"fields": schema_json.encode("utf-8"),
    }

    try:
        file_sha256 = _write_parquet_atomic(table, final_path, metadata)
    except OSError as exc:
        raise StorageError("写入 Parquet 失败，批次未登记。") from exc

    try:
        if _before_commit_hook is not None:
            _before_commit_hook()
        batch = _commit_batch(
            settings=settings,
            generation=generation,
            expected_version=expected_version,
            batch_id=batch_id,
            created_at=created_at,
            row_count=len(parsed.stations),
            file_name=file_name,
            file_sha256=file_sha256,
            schema_json=schema_json,
            fields_at_upload=fields_at_upload,
        )
    except BaseException:
        _safe_unlink(final_path)
        raise
    log.info(
        "batch %s committed rows=%s schema=v%s generation=%s",
        batch_id,
        len(parsed.stations),
        schema.version,
        generation.id,
    )
    return batch


def _commit_batch(
    *,
    settings: Settings,
    generation: Generation,
    expected_version: int,
    batch_id: str,
    created_at: str,
    row_count: int,
    file_name: str,
    file_sha256: str,
    schema_json: str,
    fields_at_upload: Sequence[dict],
) -> Batch:
    try:
        with _mutation_transaction(settings, generation, generation.id) as conn:
            _check_expected_version(conn, expected_version)
            conn.execute(
                "INSERT INTO batches(id, created_at, schema_version, row_count, "
                "file_name, file_sha256, schema_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    created_at,
                    expected_version,
                    row_count,
                    file_name,
                    file_sha256,
                    schema_json,
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("批次登记失败，未写入新批次。") from exc
    return Batch(
        id=batch_id,
        created_at=created_at,
        schema_version=expected_version,
        row_count=row_count,
        file_name=file_name,
        file_sha256=file_sha256,
        fields_at_upload=tuple(fields_at_upload),
    )


def get_batch(
    settings: Settings, batch_id: str, generation: Generation | None = None
) -> BatchView:
    """读取批次；结构与批次登记来自同一代际的同一个已提交读事务。"""

    try:
        with _read_transaction(settings, generation) as (resolved, conn):
            schema = _query_schema(conn)
            row = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc

    if row is None:
        raise NotFound("批次不存在。")
    batch = _rows_to_batch(row)
    physical = _read_physical_table(settings, resolved, batch)
    current = _materialize_current_table(physical, schema)
    return BatchView(
        schema=schema, batch=batch, table=current, generation=resolved
    )


def _read_physical_table(
    settings: Settings, generation: Generation, batch: Batch
) -> pa.Table:
    path = settings.generation_parquet_dir(generation.id) / batch.file_name
    if not path.is_file():
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的 Parquet 文件缺失：{batch.file_name}"
        )
    try:
        table = pq.read_table(path)
    except (OSError, pa.ArrowException) as exc:
        raise BatchIntegrityError(f"批次 {batch.id[:8]} 的 Parquet 无法读取。") from exc
    for fixed in (STATION_COLUMN, TIME_COLUMN):
        if fixed not in table.column_names:
            raise BatchIntegrityError(f"批次 {batch.id[:8]} 缺少固定列 {fixed}。")
    return table


def _materialize_current_table(physical: pa.Table, schema: Schema) -> pa.Table:
    """按当前已提交结构生成只读视图；不重写历史文件。

    当前字段 id 在历史文件中不存在（新字段，或删除后同名重建）时补 NULL。
    """

    row_count = physical.num_rows
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        physical.column(STATION_COLUMN).combine_chunks().cast(pa.string()),
        physical.column(TIME_COLUMN).combine_chunks().cast(
            pa.timestamp("us", tz="UTC")
        ),
    ]
    for field in schema.active:
        names.append(field.name)
        if field.id in physical.column_names:
            try:
                column = physical.column(field.id).combine_chunks().cast(pa.int64())
            except (pa.ArrowException, KeyError) as exc:
                raise BatchIntegrityError(
                    f"历史 Parquet 的列 {field.id} 类型异常。"
                ) from exc
        else:
            column = pa.nulls(row_count, type=pa.int64())
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def csv_bytes(table: pa.Table) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(table.column_names)
    for record in table.to_pylist():
        row: list[str] = []
        for name in table.column_names:
            value = record[name]
            if value is None:
                row.append("")
            elif isinstance(value, datetime):
                row.append(format_utc(value))
            else:
                row.append(str(value))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def display_cell(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return format_utc(value)
    return str(value)


def rows_as_display(table: pa.Table) -> tuple[list[str], list[list[str | None]]]:
    columns = list(table.column_names)
    rows = [
        [display_cell(record[name]) for name in columns]
        for record in table.to_pylist()
    ]
    return columns, rows


def sample_csv(schema: Schema, rows: int = 5) -> bytes:
    """按当前结构生成可直接上传的示例 CSV，用于网页演示。"""

    stations = ["54511", "54512", "58362", "50953", "45007"]
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    active = schema.active
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow([STATION_COLUMN, TIME_COLUMN, *[f.name for f in active]])
    for i in range(rows):
        row = [
            stations[i % len(stations)],
            format_utc(base.replace(hour=i % 24, minute=(i * 7) % 60)),
        ]
        for j, _field in enumerate(active):
            # 留一个空值，演示可空整数列
            row.append("" if (i + j) % 4 == 3 else str(10 + (i * 3 + j * 5) % 40))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def check_integrity(settings: Settings) -> IntegrityReport:
    state = read_state(settings)
    parquet_dir = settings.generation_parquet_dir(state.generation.id)
    registered = {b.file_name for b in state.batches}
    missing: list[str] = []
    mismatched: list[str] = []
    try:
        disk_files = {
            p.name for p in parquet_dir.glob("*.parquet") if p.is_file()
        }
        temp_files = sorted(
            p.name
            for p in parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file()
        )
    except OSError as exc:
        raise StorageError("扫描 Parquet 目录失败。") from exc

    for batch in state.batches:
        path = parquet_dir / batch.file_name
        if not path.is_file():
            missing.append(batch.file_name)
            continue
        try:
            if _sha256_file(path) != batch.file_sha256:
                mismatched.append(batch.file_name)
        except OSError:
            missing.append(batch.file_name)

    # 正在转换、尚未登记的新文件是正常瞬时状态；只把 60 秒以上的未登记文件
    # 视为孤立文件，避免并发上传时误报。
    orphan_cutoff = time.time() - 60
    orphans: list[str] = []
    for name in sorted(disk_files - registered):
        try:
            if (parquet_dir / name).stat().st_mtime < orphan_cutoff:
                orphans.append(name)
        except OSError:
            continue
    return IntegrityReport(
        ok=not missing and not mismatched,
        batches=len(state.batches),
        missing_files=tuple(missing),
        hash_mismatches=tuple(mismatched),
        orphan_files=tuple(orphans),
        temp_files=tuple(temp_files),
    )


# ---------------------------------------------------------------------------
# 整湖备份
# ---------------------------------------------------------------------------

BACKUP_MANIFEST_NAME = "manifest.json"
BACKUP_PARQUET_PREFIX = "parquet/"


def _snapshot_payload(state: LakeState, batches: Sequence[Batch]) -> dict:
    return {
        "format": BACKUP_FORMAT,
        "format_version": BACKUP_FORMAT_VERSION,
        "created_at": _utc_now_iso(),
        "source_generation": state.generation.id,
        "source_epoch": state.generation.epoch,
        "schema_version": state.schema.version,
        "fixed_columns": [STATION_COLUMN, TIME_COLUMN],
        "fields": [
            {
                "id": f.id,
                "name": f.name,
                "ordinal": f.ordinal,
                "status": f.status,
                "created_version": f.created_version,
                "deleted_version": f.deleted_version,
            }
            for f in state.schema.fields
        ],
        "batches": [
            {
                "id": b.id,
                "created_at": b.created_at,
                "schema_version": b.schema_version,
                "row_count": b.row_count,
                "file_name": b.file_name,
                "file_sha256": b.file_sha256,
                "schema_json": b.schema_json_for_backup(),
            }
            for b in batches
        ],
    }


class _HashingReader:
    """边读边计数 / 计算 sha256，避免为打包二次读取。"""

    def __init__(self, handle: BinaryIO) -> None:
        self._handle = handle
        self._digest = hashlib.sha256()
        self.size = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._handle.read(size)
        if chunk:
            self._digest.update(chunk)
            self.size += len(chunk)
        return chunk

    @property
    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def write_backup(settings: Settings, out: BinaryIO) -> dict:
    """把同一已提交快照的字段身份、结构与全部批次原件写成 ZIP。

    不持有数据库写锁：打包期间其他实例仍可上传或改列。快照中的批次文件是
    不可变的，因此打包期间不会被改写。
    """

    state = read_state(settings)
    generation = state.generation
    parquet_dir = settings.generation_parquet_dir(generation.id)
    if not parquet_dir.is_dir():
        raise StorageError("当前代际的 Parquet 目录缺失，无法备份。")

    entries: list[dict] = []
    try:
        with zipfile.ZipFile(
            out,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True,
        ) as archive:
            for batch in state.batches:
                path = parquet_dir / batch.file_name
                if not path.is_file():
                    raise StorageError(
                        f"批次 {batch.short_id} 的原件缺失，备份中止；"
                        "当前湖未被修改。"
                    )
                member = f"{BACKUP_PARQUET_PREFIX}{batch.file_name}"
                with path.open("rb") as source:
                    reader = _HashingReader(source)
                    with archive.open(member, "w") as target:
                        shutil.copyfileobj(reader, target, length=1024 * 1024)
                if reader.hexdigest != batch.file_sha256:
                    raise StorageError(
                        f"批次 {batch.short_id} 的原件校验失败，备份中止；"
                        "当前湖未被修改。"
                    )
                entries.append(
                    {
                        "path": member,
                        "size": reader.size,
                        "sha256": reader.hexdigest,
                    }
                )
            manifest = _snapshot_payload(state, state.batches)
            manifest["files"] = entries
            payload = json.dumps(
                manifest, ensure_ascii=False, indent=2, sort_keys=True
            ).encode("utf-8")
            info = zipfile.ZipInfo(BACKUP_MANIFEST_NAME)
            info.date_time = time.localtime()[:6]
            info.external_attr = 0o644 << 16
            archive.writestr(info, payload, compress_type=zipfile.ZIP_DEFLATED)
    except OSError as exc:
        raise StorageError("写入备份包失败；当前湖未被修改。") from exc
    log.info(
        "backup generation=%s schema=v%s batches=%s bytes=%s",
        generation.id,
        state.schema.version,
        len(state.batches),
        len(entries),
    )
    return manifest


def backup_bytes(settings: Settings) -> bytes:
    sink = io.BytesIO()
    write_backup(settings, sink)
    return sink.getvalue()


def backup_filename(settings: Settings) -> str:
    state = read_state(settings)
    return f"metlake-backup-v{state.schema.version}-{state.generation.id[:10]}.zip"


# ---------------------------------------------------------------------------
# 整湖恢复
# ---------------------------------------------------------------------------


class _BackupProblems(Exception):
    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = list(problems)
        super().__init__("；".join(self.problems))


def _require_int(value: object, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _BackupProblems([f"{label} 必须是整数。"])
    if value < minimum:
        raise _BackupProblems([f"{label} 不能小于 {minimum}。"])
    return value


def _require_str(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise _BackupProblems([f"{label} 必须是非空字符串。"])
    return value


def _validate_batch_member(member: str) -> str | None:
    """返回规范化后的文件名；越界 / 非法路径返回 None。"""

    if not member.startswith(BACKUP_PARQUET_PREFIX):
        return None
    name = member[len(BACKUP_PARQUET_PREFIX):]
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        return None
    if Path(name).name != name:
        return None
    if name.startswith(".") or name != name.strip():
        return None
    if not name.endswith(".parquet"):
        return None
    if not BATCH_ID_RE.fullmatch(name[: -len(".parquet")]):
        return None
    return name


def _validate_manifest(manifest: object) -> dict:
    if not isinstance(manifest, dict):
        raise _BackupProblems(["manifest.json 必须是 JSON 对象。"])
    if manifest.get("format") != BACKUP_FORMAT:
        raise _BackupProblems(["不是本服务的备份包（format 不匹配）。"])
    if manifest.get("format_version") != BACKUP_FORMAT_VERSION:
        raise _BackupProblems(
            [f"备份格式版本 {manifest.get('format_version')!r} 不受支持。"]
        )

    fixed = manifest.get("fixed_columns")
    if fixed != [STATION_COLUMN, TIME_COLUMN]:
        raise _BackupProblems(["备份的固定列定义与本服务不一致。"])

    schema_version = _require_int(manifest.get("schema_version"), "schema_version")

    raw_fields = manifest.get("fields")
    if not isinstance(raw_fields, list):
        raise _BackupProblems(["fields 必须是数组。"])
    fields: list[dict] = []
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for index, item in enumerate(raw_fields):
        label = f"fields[{index}]"
        if not isinstance(item, dict):
            raise _BackupProblems([f"{label} 必须是对象。"])
        field_id = _require_str(item.get("id"), f"{label}.id")
        if not FIELD_ID_RE.fullmatch(field_id):
            raise _BackupProblems([f"{label}.id 非法：{field_id!r}"])
        if field_id in seen_ids:
            raise _BackupProblems([f"字段 id 重复：{field_id}"])
        seen_ids.add(field_id)
        try:
            name = normalize_field_name(item.get("name"))
        except LakeError as exc:
            raise _BackupProblems([f"{label}.name 非法：{exc}"]) from None
        status = item.get("status")
        if status not in ("active", "deleted"):
            raise _BackupProblems([f"{label}.status 必须是 active / deleted。"])
        if status == "active":
            folded = name.casefold()
            if folded in seen_names:
                raise _BackupProblems([f"活动字段名重复：{name}"])
            seen_names.add(folded)
        ordinal = _require_int(item.get("ordinal"), f"{label}.ordinal")
        created_version = _require_int(
            item.get("created_version"), f"{label}.created_version"
        )
        deleted_version = item.get("deleted_version")
        if deleted_version is not None:
            deleted_version = _require_int(
                deleted_version, f"{label}.deleted_version"
            )
        fields.append(
            {
                "id": field_id,
                "name": name,
                "ordinal": ordinal,
                "status": status,
                "created_version": created_version,
                "deleted_version": deleted_version,
            }
        )
    active_count = sum(1 for f in fields if f["status"] == "active")
    if active_count > MAX_FIELDS:
        raise _BackupProblems([f"活动测量列 {active_count} 个，超过上限 {MAX_FIELDS}。"])

    raw_batches = manifest.get("batches")
    if not isinstance(raw_batches, list):
        raise _BackupProblems(["batches 必须是数组。"])
    batches: list[dict] = []
    seen_batch_ids: set[str] = set()
    seen_files: set[str] = set()
    for index, item in enumerate(raw_batches):
        label = f"batches[{index}]"
        if not isinstance(item, dict):
            raise _BackupProblems([f"{label} 必须是对象。"])
        batch_id = _require_str(item.get("id"), f"{label}.id")
        if not BATCH_ID_RE.fullmatch(batch_id):
            raise _BackupProblems([f"{label}.id 非法：{batch_id!r}"])
        if batch_id in seen_batch_ids:
            raise _BackupProblems([f"批次 id 重复：{batch_id}"])
        seen_batch_ids.add(batch_id)
        file_name = _require_str(item.get("file_name"), f"{label}.file_name")
        if file_name != f"{batch_id}.parquet":
            raise _BackupProblems(
                [f"{label}.file_name 必须与批次 id 对应：{file_name!r}"]
            )
        if _validate_batch_member(f"{BACKUP_PARQUET_PREFIX}{file_name}") is None:
            raise _BackupProblems([f"{label}.file_name 非法：{file_name!r}"])
        if file_name in seen_files:
            raise _BackupProblems([f"批次文件名重复：{file_name}"])
        seen_files.add(file_name)
        created_at = _require_str(item.get("created_at"), f"{label}.created_at")
        row_count = _require_int(item.get("row_count"), f"{label}.row_count")
        if row_count > MAX_ROWS:
            raise _BackupProblems(
                [f"{label}.row_count={row_count} 超过单批上限 {MAX_ROWS}。"]
            )
        batch_version = _require_int(
            item.get("schema_version"), f"{label}.schema_version"
        )
        if batch_version > schema_version:
            raise _BackupProblems(
                [f"{label}.schema_version 大于备份结构版本 {schema_version}。"]
            )
        sha = _require_str(item.get("file_sha256"), f"{label}.file_sha256")
        if not SHA256_RE.fullmatch(sha.lower()):
            raise _BackupProblems([f"{label}.file_sha256 不是 SHA-256。"])
        schema_json = item.get("schema_json")
        if not isinstance(schema_json, str):
            raise _BackupProblems([f"{label}.schema_json 必须是字符串。"])
        try:
            parsed_schema = json.loads(schema_json)
        except ValueError as exc:
            raise _BackupProblems(
                [f"{label}.schema_json 不是合法 JSON：{exc}"]
            ) from None
        if not isinstance(parsed_schema, list):
            raise _BackupProblems([f"{label}.schema_json 必须是数组。"])
        for entry in parsed_schema:
            if not isinstance(entry, dict) or not FIELD_ID_RE.fullmatch(
                str(entry.get("id", ""))
            ):
                raise _BackupProblems(
                    [f"{label}.schema_json 含非法字段身份。"]
                )
        batches.append(
            {
                "id": batch_id,
                "created_at": created_at,
                "schema_version": batch_version,
                "row_count": row_count,
                "file_name": file_name,
                "file_sha256": sha.lower(),
                "schema_json": schema_json,
            }
        )

    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise _BackupProblems(["files 必须是数组。"])
    files: dict[str, dict] = {}
    for index, item in enumerate(raw_files):
        label = f"files[{index}]"
        if not isinstance(item, dict):
            raise _BackupProblems([f"{label} 必须是对象。"])
        member = _require_str(item.get("path"), f"{label}.path")
        name = _validate_batch_member(member)
        if name is None:
            raise _BackupProblems([f"{label}.path 越界或非法：{member!r}"])
        if name in files:
            raise _BackupProblems([f"files 中路径重复：{member}"])
        size = _require_int(item.get("size"), f"{label}.size")
        sha = _require_str(item.get("sha256"), f"{label}.sha256").lower()
        if not SHA256_RE.fullmatch(sha):
            raise _BackupProblems([f"{label}.sha256 不是 SHA-256。"])
        files[name] = {"member": member, "size": size, "sha256": sha}

    expected = {b["file_name"]: b for b in batches}
    if set(files) != set(expected):
        missing = sorted(set(expected) - set(files))
        extra = sorted(set(files) - set(expected))
        problems = []
        if missing:
            problems.append("files 缺少批次原件：" + "、".join(missing))
        if extra:
            problems.append("files 含未登记的原件：" + "、".join(extra))
        raise _BackupProblems(problems)
    for name, info in files.items():
        if info["sha256"] != expected[name]["file_sha256"]:
            raise _BackupProblems(
                [f"清单内 {name} 的 SHA-256 与批次登记不一致。"]
            )

    return {
        "schema_version": schema_version,
        "fields": fields,
        "batches": batches,
        "files": files,
        "source_generation": manifest.get("source_generation"),
    }


def _safe_zip_member(info: zipfile.ZipInfo) -> str | None:
    name = info.filename
    if "\\" in name or "\x00" in name:
        return None
    if name.startswith("/") or name.startswith("../"):
        return None
    if info.is_dir():
        # 允许工具生成的显式目录条目（如 "parquet/"），但不得有其它空段。
        name = name.rstrip("/")
        if not name:
            return None
    parts = name.split("/")
    if any(part in ("", ".", "..") for part in parts):
        return None
    if info.flag_bits & 0x1:  # 加密成员
        return None
    mode = (info.external_attr >> 16) & 0o170000
    if mode == stat.S_IFLNK:
        return None
    return name


def _check_zip_members(
    archive: zipfile.ZipFile, files: dict[str, dict]
) -> tuple[str, dict[str, zipfile.ZipInfo]]:
    infos: dict[str, zipfile.ZipInfo] = {}
    total = 0
    file_entries = 0
    manifest_info: zipfile.ZipInfo | None = None
    for info in archive.infolist():
        name = _safe_zip_member(info)
        if name is None:
            raise _BackupProblems([f"备份包含不安全路径：{info.filename!r}"])
        if info.is_dir():
            continue
        file_entries += 1
        if file_entries > MAX_BACKUP_ENTRIES:
            raise _BackupProblems(
                [f"备份包文件条目过多（>{MAX_BACKUP_ENTRIES}）。"]
            )
        if name == BACKUP_MANIFEST_NAME:
            if manifest_info is not None:
                raise _BackupProblems(["备份包含重复的 manifest.json。"])
            if info.file_size > MAX_MANIFEST_BYTES:
                raise _BackupProblems(["manifest.json 过大。"])
            manifest_info = info
            continue
        if not name.startswith(BACKUP_PARQUET_PREFIX):
            raise _BackupProblems([f"备份包含未知条目：{name}"])
        base = name[len(BACKUP_PARQUET_PREFIX):]
        if base not in files:
            raise _BackupProblems([f"备份包含清单之外的条目：{name}"])
        if base in infos:
            raise _BackupProblems([f"备份包含重复的原件条目：{name}"])
        total += info.file_size
        if total > MAX_BACKUP_UNPACKED_BYTES:
            raise _BackupProblems(["备份解压后体积超过上限。"])
        infos[base] = info
    if manifest_info is None:
        raise _BackupProblems(["备份包缺少 manifest.json。"])
    missing = sorted(set(files) - set(infos))
    if missing:
        raise _BackupProblems(["备份包缺少批次原件：" + "、".join(missing)])
    for name, info in infos.items():
        if info.file_size != files[name]["size"]:
            raise _BackupProblems(
                [f"备份包中 {name} 的长度与清单不符。"]
            )
    return BACKUP_MANIFEST_NAME, infos


def _read_manifest(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo
) -> dict:
    with archive.open(info) as handle:
        payload = handle.read(MAX_MANIFEST_BYTES + 1)
    if len(payload) > MAX_MANIFEST_BYTES:
        raise _BackupProblems(["manifest.json 过大。"])
    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise _BackupProblems([f"manifest.json 无法解析：{exc}"]) from None
    return _validate_manifest(manifest)


def _stage_backup(
    settings: Settings,
    archive: zipfile.ZipFile,
    infos: dict[str, zipfile.ZipInfo],
    plan_data: dict,
    staging: Path,
) -> None:
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=False)
    for name, info in infos.items():
        expected = plan_data["files"][name]
        target = parquet_dir / name
        if target.parent != parquet_dir or target.name != name:
            raise _BackupProblems([f"拒绝越界写入：{name}"])
        digest = hashlib.sha256()
        size = 0
        try:
            with archive.open(info) as source, target.open("wb") as sink:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > expected["size"]:
                        raise _BackupProblems(
                            [f"备份包中 {name} 的实际长度超过清单。"]
                        )
                    digest.update(chunk)
                    sink.write(chunk)
                sink.flush()
                os.fsync(sink.fileno())
        except _BackupProblems:
            raise
        except (OSError, zipfile.BadZipFile, RuntimeError, zlib.error, ValueError, EOFError) as exc:
            raise _BackupProblems([f"备份包中 {name} 无法解压/写入：{exc}"]) from None
        if size != expected["size"]:
            raise _BackupProblems(
                [f"备份包中 {name} 的长度为 {size}，清单为 {expected['size']}。"]
            )
        if digest.hexdigest() != expected["sha256"]:
            raise _BackupProblems([f"备份包中 {name} 的 SHA-256 校验失败。"])
    _fsync_dir(parquet_dir)

    # 构建代际数据库：只按清单恢复字段身份与登记，不重新解析 CSV。
    # 与新建代际一致使用 WAL，保证切换后双实例并发读写行为不变。
    conn = _open_sqlite(staging / "lake.sqlite3", wal=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_generation_schema(conn)
        conn.execute(
            "UPDATE meta SET value = ? WHERE key = ?",
            (str(plan_data["schema_version"]), SCHEMA_VERSION_KEY),
        )
        restored_at = _utc_now_iso()
        for field in plan_data["fields"]:
            conn.execute(
                "INSERT INTO fields(id, name, ordinal, status, created_version, "
                "deleted_version, created_at, deleted_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    field["id"],
                    field["name"],
                    field["ordinal"],
                    field["status"],
                    field["created_version"],
                    field["deleted_version"],
                    restored_at,
                    restored_at if field["status"] == "deleted" else None,
                ),
            )
        for batch in plan_data["batches"]:
            conn.execute(
                "INSERT INTO batches(id, created_at, schema_version, row_count, "
                "file_name, file_sha256, schema_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch["id"],
                    batch["created_at"],
                    batch["schema_version"],
                    batch["row_count"],
                    batch["file_name"],
                    batch["file_sha256"],
                    batch["schema_json"],
                ),
            )
        conn.execute("COMMIT")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise _BackupProblems([f"构建恢复库失败：{exc}"]) from None
    finally:
        conn.close()

    # 强校验：清单登记与落盘文件一一对应，且每个原件可被 Parquet 读取。
    conn = _open_sqlite(staging / "lake.sqlite3")
    try:
        conn.execute("BEGIN")
        schema = _query_schema(conn)
        rows = conn.execute(
            "SELECT id, created_at, schema_version, row_count, file_name, "
            "file_sha256, schema_json FROM batches"
        ).fetchall()
        restored_batches = tuple(_rows_to_batch(r) for r in rows)
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
        conn.close()
    if schema.version != plan_data["schema_version"]:
        raise _BackupProblems(["恢复库结构版本与清单不一致。"])
    if len(restored_batches) != len(plan_data["batches"]):
        raise _BackupProblems(["恢复库批次数量与清单不一致。"])
    on_disk = {p.name for p in parquet_dir.iterdir() if p.is_file()}
    registered = {b.file_name for b in restored_batches}
    if on_disk != registered:
        raise _BackupProblems(["恢复目录中的原件与登记不一致。"])
    for batch in restored_batches:
        try:
            table = pq.read_table(parquet_dir / batch.file_name)
        except (OSError, pa.ArrowException) as exc:
            raise _BackupProblems(
                [f"批次 {batch.short_id} 的原件不是可读 Parquet：{exc}"]
            ) from None
        for fixed in (STATION_COLUMN, TIME_COLUMN):
            if fixed not in table.column_names:
                raise _BackupProblems(
                    [f"批次 {batch.short_id} 缺少固定列 {fixed}。"]
                )
        if table.num_rows != batch.row_count:
            raise _BackupProblems(
                [
                    f"批次 {batch.short_id} 行数为 {table.num_rows}，"
                    f"登记为 {batch.row_count}。"
                ]
            )
        # 只读校验当前结构投影不炸，确保恢复后网页可读。
        _materialize_current_table(table, schema)
    _fsync_dir(parquet_dir)
    _fsync_dir(staging)


def _prepare_restore(
    settings: Settings, source: bytes | BinaryIO
) -> _RestorePlan:
    if isinstance(source, (bytes, bytearray)):
        view = memoryview(source)
        if not view:
            raise ValidationError("恢复包为空。")
        if len(view) > MAX_BACKUP_BYTES:
            raise ValidationError(
                f"恢复包不能超过 {MAX_BACKUP_BYTES // (1024 * 1024)} MiB。"
            )
        stream: BinaryIO = io.BytesIO(view)
    else:
        stream = source
        try:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(0)
        except (OSError, ValueError) as exc:
            raise ValidationError("无法读取恢复包。") from exc
        if size == 0:
            raise ValidationError("恢复包为空。")
        if size > MAX_BACKUP_BYTES:
            raise ValidationError(
                f"恢复包不能超过 {MAX_BACKUP_BYTES // (1024 * 1024)} MiB。"
            )
    staging = settings.staging_dir / f"restore-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    # 恢复构建可能在处理中途失败；除显式成功外，所有异常都回收暂存并归一
    # 为“整包拒绝 / 写盘失败”，绝不留下半份新代际。
    try:
        with zipfile.ZipFile(stream, "r") as archive:
            for info in archive.infolist():
                if (
                    not info.is_dir()
                    and info.file_size > MAX_BACKUP_UNPACKED_BYTES
                ):
                    raise _BackupProblems(["备份包中单个条目过大。"])
            # 逐个成员读取时会校验 CRC；同时以逐文件 SHA-256 为准（见 _stage_backup），
            # 不需要再完整解压一遍做 testzip。
            manifest_info = None
            for info in archive.infolist():
                safe = _safe_zip_member(info)
                if safe is None:
                    raise _BackupProblems(
                        [f"备份包含不安全路径：{info.filename!r}"]
                    )
                if safe == BACKUP_MANIFEST_NAME and not info.is_dir():
                    manifest_info = info
            if manifest_info is None:
                raise _BackupProblems(["备份包缺少 manifest.json。"])
            manifest = _read_manifest(archive, manifest_info)
            _, infos = _check_zip_members(archive, manifest["files"])
            _stage_backup(settings, archive, infos, manifest, staging)
    except _BackupProblems:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    except (zipfile.BadZipFile, zlib.error, EOFError, ValueError, RuntimeError) as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise _BackupProblems([f"备份包损坏或不是有效 ZIP：{exc}"]) from None
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise StorageError("恢复准备期间写盘失败；当前湖保持不变。") from exc

    return _RestorePlan(
        directory=staging,
        generation=Generation(
            id=f"g_{uuid.uuid4().hex}",
            epoch=0,
            created_at=_utc_now_iso(),
        ),
        schema_version=manifest["schema_version"],
        batch_count=len(manifest["batches"]),
    )


def _atomic_switch(
    settings: Settings, plan: _RestorePlan, expected_token: str | None = None
) -> tuple[Generation, Generation]:
    """持控制库写锁完成原子切换。

    构建阶段完全不接触当前湖，因此准备期间仍可读写；切换只有“目录 rename +
    控制指针更新”两小步，期间读者要么看到完整旧代际，要么看到完整新代际。
    期望代际的比对在控制库写锁内完成：两个实例基于同一旧湖同时恢复时至多
    一个成功，另一个按过期拒绝。
    """

    staging = plan.directory
    final_dir = settings.generation_dir(plan.generation.id)
    ctrl_conn = _connect_control(settings)
    moved = False
    try:
        ctrl_conn.execute("BEGIN IMMEDIATE")
        previous = _query_generation(ctrl_conn)
        if previous is None:
            raise StorageError("控制库缺少当前代际，拒绝恢复。")
        if expected_token is not None and expected_token != previous.id:
            raise StaleGeneration(
                "数据湖已被其他实例恢复或替换，本次恢复未执行；"
                "请刷新页面后重试。"
            )
        epoch = previous.epoch + 1
        generation = Generation(
            id=plan.generation.id,
            epoch=epoch,
            created_at=plan.generation.created_at,
        )
        _fsync_dir(staging)
        if final_dir.exists():
            raise StorageError("恢复目标代际已存在，拒绝覆盖。")
        os.replace(staging, final_dir)
        moved = True
        _fsync_dir(settings.generations_dir)
        _set_current_generation(ctrl_conn, generation)
        ctrl_conn.execute(
            "INSERT INTO restores(at, instance, source_generation, new_generation, "
            "epoch, schema_version, batches, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                _utc_now_iso(),
                settings.instance_name,
                previous.id,
                generation.id,
                generation.epoch,
                plan.schema_version,
                plan.batch_count,
                "atomic-switch",
            ),
        )
        ctrl_conn.execute("COMMIT")
        old_dir = settings.generation_dir(previous.id)
        try:
            # 让旧代际的保留期从“切换时刻”起算，保护切换前已在进行的查询。
            os.utime(old_dir, None)
        except OSError:
            pass
    except sqlite3.Error as exc:
        try:
            ctrl_conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        if moved:
            # 指针未更新（COMMIT 失败）时，把新代际移回暂存以便下次启动清理。
            # 目录 rename 本身可逆，当前湖仍是完整旧代际。
            try:
                os.replace(final_dir, staging)
            except OSError:
                log.exception("切回暂存目录失败；启动时会回收未引用代际")
        raise StorageError("恢复切换提交失败；当前湖保持完整旧代际。") from exc
    except BaseException:
        try:
            ctrl_conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        if moved:
            try:
                os.replace(final_dir, staging)
            except OSError:
                log.exception("切回暂存目录失败；启动时会回收未引用代际")
        raise
    finally:
        ctrl_conn.close()
    _refresh_compat_paths(settings, generation)
    return generation, previous


def restore_backup(
    settings: Settings,
    data: bytes | BinaryIO,
    *,
    expected_token: str | None = None,
) -> RestoreResult:
    """从本服务备份包整体替换数据湖；任何校验失败都整包拒绝。"""

    with _restore_lock(settings):
        # 以“开始恢复时看到的代际”作为 CAS 期望值。即使两个请求来自不同实例、
        # 且调用方没带 token，后完成者也只会看到代际已变而拒绝。
        start_generation = current_generation(settings)
        expected = expected_token or start_generation.id
        try:
            plan = _prepare_restore(settings, data)
        except _BackupProblems as exc:
            raise BackupRejected(
                "恢复包校验失败，已整包拒绝；当前湖未改变。" + "；".join(exc.problems)
            ) from None
        try:
            generation, previous = _atomic_switch(settings, plan, expected)
        except BaseException:
            shutil.rmtree(plan.directory, ignore_errors=True)
            raise
    log.warning(
        "restored generation epoch=%s schema=v%s batches=%s",
        generation.epoch,
        plan.schema_version,
        plan.batch_count,
    )
    return RestoreResult(
        generation=generation,
        previous_generation=previous,
        schema_version=plan.schema_version,
        batches=plan.batch_count,
    )


def restore_history(settings: Settings, limit: int = 20) -> list[dict]:
    conn = _connect_control(settings)
    try:
        rows = conn.execute(
            "SELECT at, instance, source_generation, new_generation, epoch, "
            "schema_version, batches, detail FROM restores "
            "ORDER BY id DESC LIMIT ?",
            (max(1, min(limit, 200)),),
        ).fetchall()
    except sqlite3.Error as exc:
        raise StorageError("读取恢复记录失败。") from exc
    finally:
        conn.close()
    return [dict(row) for row in rows]
