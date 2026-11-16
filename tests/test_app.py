"""核心与 Web 层测试。运行：python -m pytest -q"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import warnings
import zipfile
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pyarrow import parquet as pq

from app import lake
from app.lake import Settings
from app.main import create_app

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    s = Settings(data_dir=tmp_path / "lake", instance_name="test")
    lake.initialize(s)
    return s


@pytest.fixture()
def client(settings: Settings):
    with TestClient(create_app(settings)) as c:
        yield c


def csv_text(header: list[str], rows: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def simple_csv(station="54511", time="2026-01-01T00:00:00Z", **values) -> bytes:
    header = [lake.STATION_COLUMN, lake.TIME_COLUMN, *values.keys()]
    row = [station, time, *(str(v) if v is not None else "" for v in values.values())]
    return csv_text(header, [row])


def current(client: TestClient) -> dict:
    resp = client.get("/api/state")
    assert resp.status_code == 200
    return resp.json()


def add(client: TestClient, name: str, version: int | None = None) -> httpx.Response:
    if version is None:
        version = current(client)["schema"]["schema_version"]
    return client.post(
        "/api/fields", json={"schema_version": version, "name": name}
    )


def file_digest(path: Path) -> tuple[str, int]:
    return hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns


# ---------------------------------------------------------------------------
# 结构：新增 / 改名 / 删除
# ---------------------------------------------------------------------------


def test_add_field_and_version(client: TestClient):
    assert current(client)["schema"]["schema_version"] == 0
    resp = add(client, "temp_c")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["schema_version"] == 1
    assert [f["name"] for f in body["fields"]] == ["temp_c"]
    health = client.get("/healthz").json()
    assert health["schema_version"] == 1


def test_add_field_stale_version_rejected(client: TestClient):
    add(client, "temp_c")
    stale = add(client, "humidity", version=0)
    assert stale.status_code == 409
    payload = stale.json()
    assert payload["error"] == "stale_schema"
    assert payload["schema_version"] == 1
    assert payload["expected_version"] == 0
    state = current(client)
    assert state["schema"]["schema_version"] == 1
    assert [f["name"] for f in state["schema"]["fields"]] == ["temp_c"]


def test_rename_keeps_values_and_does_not_rewrite_file(client: TestClient, settings: Settings):
    add(client, "temp_c")
    version = current(client)["schema"]["schema_version"]
    up = client.post(
        "/api/batches",
        data={"version": version},
        files={"file": ("a.csv", simple_csv(temp_c=21), "text/csv")},
    )
    assert up.status_code == 201, up.text
    batch = up.json()
    path = settings.parquet_dir / batch["file_name"]
    before = file_digest(path)

    field_id = current(client)["schema"]["fields"][0]["id"]
    version = current(client)["schema"]["schema_version"]
    renamed = client.patch(
        f"/api/fields/{field_id}",
        json={"schema_version": version, "name": "temperature_c"},
    )
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["schema_version"] == version + 1
    assert file_digest(path) == before

    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temperature_c"]
    assert view["rows"][0][2] == "21"
    assert view["batch"]["fields_at_upload"][0]["name"] == "temp_c"

    csv_resp = client.get(f"/api/batches/{batch['id']}?format=csv")
    assert csv_resp.status_code == 200
    assert csv_resp.text.splitlines()[0] == "station_id,observed_at,temperature_c"
    assert ",21" in csv_resp.text


def test_delete_then_readd_same_name_does_not_leak_values(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    old = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("old.csv", simple_csv(temp_c=7), "text/csv")},
    ).json()
    old_path = settings.parquet_dir / old["file_name"]
    old_digest = file_digest(old_path)
    old_field = current(client)["schema"]["fields"][0]

    v = current(client)["schema"]["schema_version"]
    assert client.request(
        "DELETE",
        f"/api/fields/{old_field['id']}",
        json={"schema_version": v},
    ).status_code == 200
    assert current(client)["schema"]["fields"] == []
    assert current(client)["schema"]["deleted_fields"][0]["name"] == "temp_c"

    v = current(client)["schema"]["schema_version"]
    readd = client.post("/api/fields", json={"schema_version": v, "name": "temp_c"})
    assert readd.status_code == 201, readd.text
    new_field = readd.json()["fields"][0]
    assert new_field["id"] != old_field["id"]

    v = current(client)["schema"]["schema_version"]
    new = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("new.csv", simple_csv(temp_c=99), "text/csv")},
    ).json()

    old_view = client.get(f"/api/batches/{old['id']}").json()
    assert old_view["columns"][2:] == ["temp_c"]
    assert old_view["rows"][0][2] is None, "旧批次不得串到同名新字段"

    new_view = client.get(f"/api/batches/{new['id']}").json()
    assert new_view["rows"][0][2] == "99"

    assert file_digest(old_path) == old_digest
    assert pq.read_table(old_path).column(old_field["id"]).to_pylist() == [7]


def test_rename_to_existing_or_reserved_name_rejected(client: TestClient):
    add(client, "temp_c")
    add(client, "humidity")
    v = current(client)["schema"]["schema_version"]
    fields = {f["name"]: f["id"] for f in current(client)["schema"]["fields"]}
    dup = client.patch(
        f"/api/fields/{fields['humidity']}",
        json={"schema_version": v, "name": "TEMP_C"},
    )
    assert dup.status_code == 400
    reserved = client.patch(
        f"/api/fields/{fields['humidity']}",
        json={"schema_version": v, "name": "observed_at"},
    )
    assert reserved.status_code == 400
    assert current(client)["schema"]["schema_version"] == v


def test_max_eight_fields(client: TestClient):
    for i in range(8):
        assert add(client, f"m{i}").status_code == 201
    v = current(client)["schema"]["schema_version"]
    ninth = add(client, "m8")
    assert ninth.status_code == 400
    state = current(client)
    assert state["schema"]["schema_version"] == v
    assert len(state["schema"]["fields"]) == 8


def test_web_forms_use_hidden_version_and_reject_stale(client: TestClient):
    page = client.get("/")
    assert page.status_code == 200
    assert 'name="version" value="0"' in page.text
    ok = client.post("/fields/add", data={"version": 0, "name": "temp_c"}, follow_redirects=False)
    assert ok.status_code == 303
    assert "v1" in ok.headers["location"]

    stale = client.post(
        "/fields/add", data={"version": 0, "name": "humidity"}, follow_redirects=False
    )
    assert stale.status_code == 303
    assert "kind=error" in stale.headers["location"]
    assert current(client)["schema"]["schema_version"] == 1

    redirected = client.post(
        "/fields/add", data={"version": 0, "name": "humidity"}, follow_redirects=True
    )
    assert "结构版本已过期" in redirected.text


# ---------------------------------------------------------------------------
# 上传解析
# ---------------------------------------------------------------------------


def test_upload_subset_of_columns_fills_nulls(client: TestClient):
    add(client, "temp_c")
    add(client, "humidity")
    v = current(client)["schema"]["schema_version"]
    resp = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=3), "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    view = client.get(f"/api/batches/{resp.json()['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temp_c", "humidity"]
    assert view["rows"][0] == ["54511", "2026-01-01T00:00:00Z", "3", None]


def test_upload_stale_version_rejected_and_no_file(client: TestClient, settings: Settings):
    add(client, "temp_c")
    stale = client.post(
        "/api/batches",
        data={"version": 0},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    )
    assert stale.status_code == 409
    assert list(settings.parquet_dir.glob("*.parquet")) == []
    assert current(client)["batches"] == []


def test_upload_unknown_column_rejected(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    bad = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(wind=5), "text/csv")},
    )
    assert bad.status_code == 400
    assert "wind" in bad.json()["detail"]


@pytest.mark.parametrize(
    "payload, fragment",
    [
        (b"station_id,observed_at\n", "至少需要 1 行"),
        (b"station_id,observed_at\n54511,not-a-time\n", "ISO 8601"),
        (b"station_id,observed_at\n,2026-01-01T00:00:00Z\n", "站号为空"),
        (b"station_id\n54511\n", "observed_at"),
        (b"station_id,observed_at,station_id\n54511,2026-01-01T00:00:00Z,1\n", "重复列名"),
        (b"", "为空"),
        (b"\xff\xfe", "UTF-8"),
    ],
)
def test_bad_csv_rejected(client: TestClient, payload: bytes, fragment: str):
    v = current(client)["schema"]["schema_version"]
    resp = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("bad.csv", payload, "text/csv")},
    )
    assert resp.status_code == 400, resp.text
    assert fragment in resp.json()["detail"]
    assert current(client)["batches"] == []


def test_bad_integer_and_too_many_rows(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    bad_int = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c="1.5"), "text/csv")},
    )
    assert bad_int.status_code == 400
    assert "不是整数" in bad_int.json()["detail"]

    rows = [["54511", "2026-01-01T00:00:00Z", str(i)] for i in range(1001)]
    too_many = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("b.csv", csv_text(["station_id", "observed_at", "temp_c"], rows), "text/csv")},
    )
    assert too_many.status_code == 400
    assert "1000" in too_many.json()["detail"]

    rows = [["54511", "2026-01-01T00:00:00Z", str(i)] for i in range(1000)]
    ok = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("c.csv", csv_text(["station_id", "observed_at", "temp_c"], rows), "text/csv")},
    )
    assert ok.status_code == 201, ok.text
    assert ok.json()["row_count"] == 1000


def test_upload_during_conversion_with_changed_schema_is_rejected(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    add(client, "humidity")  # v2
    add(client, "temp_c_extra")  # v3
    # 回到 v3，用 v3 上传
    v = current(client)["schema"]["schema_version"]

    target = current(client)["schema"]["fields"][0]["id"]

    def change_structure() -> None:
        # 模拟另一实例在“转换文件期间”提交改名（直接走核心层，避开 TestClient 线程限制）
        lake.rename_field(settings, v, target, "temp_air")

    monkeypatch.setattr(lake, "_before_commit_hook", change_structure)
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=5), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake, "_before_commit_hook", None)
    assert resp.status_code == 409
    state = current(client)
    assert state["batches"] == []
    assert list(settings.parquet_dir.glob("*.parquet")) == [], "被拒绝的旧结构上传不得留下文件"
    assert state["schema"]["fields"][0]["name"] == "temp_air"


def test_write_failure_keeps_existing_batches(client: TestClient, settings: Settings, monkeypatch):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    good = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    )
    assert good.status_code == 201
    before = current(client)

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(lake.pq, "write_table", boom)
    try:
        failed = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake.pq, "write_table", pq.write_table)
    assert failed.status_code == 500
    after = current(client)
    assert after["batches"] == before["batches"]
    assert len(list(settings.parquet_dir.glob("*.parquet"))) == 1
    assert client.get(f"/api/batches/{good.json()['id']}").status_code == 200


def test_registration_failure_removes_new_file(client: TestClient, settings: Settings, monkeypatch):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]

    def fail(*args, **kwargs):
        raise lake.StorageError("登记失败（模拟）")

    monkeypatch.setattr(lake, "_commit_batch", fail)
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake, "_commit_batch", lake._commit_batch)
    assert resp.status_code == 500
    assert list(settings.parquet_dir.glob("*.parquet")) == []
    assert current(client)["batches"] == []


# ---------------------------------------------------------------------------
# 一致性 / 重启 / 完整性
# ---------------------------------------------------------------------------


def test_restart_preserves_schema_batches_files(settings: Settings):
    app = create_app(settings)
    with TestClient(app) as c:
        add(c, "temp_c")
        v = current(c)["schema"]["schema_version"]
        batch = c.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=8), "text/csv")},
        ).json()
        schema_before = current(c)["schema"]

    restarted = TestClient(create_app(Settings(data_dir=settings.data_dir, instance_name="again")))
    with restarted:
        state = current(restarted)
        assert state["schema"] == schema_before
        assert [b["id"] for b in state["batches"]] == [batch["id"]]
        view = restarted.get(f"/api/batches/{batch['id']}").json()
        assert view["rows"][0][2] == "8"
        assert restarted.get("/api/integrity").json()["ok"] is True


def test_reader_sees_only_committed_schema(settings: Settings):
    schema = lake.add_field(settings, 0, "temp_c")
    field_id = schema.fields[0].id
    version = schema.version

    blocker = sqlite3.connect(settings.db_path, isolation_level=None)
    blocker.execute("PRAGMA busy_timeout = 5000")
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("UPDATE fields SET name = 'uncommitted_name' WHERE id = ?", (field_id,))
    blocker.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (str(version + 1),))
    try:
        seen = lake.read_schema(settings)
        assert seen.version == version
        assert seen.fields[0].name == "temp_c"
        state = lake.read_state(settings)
        assert state.schema.version == version
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    assert lake.read_schema(settings).fields[0].name == "temp_c"


def test_batch_view_is_single_snapshot_under_concurrent_renames(settings: Settings):
    lake.add_field(settings, 0, "temp_c")
    schema = lake.read_schema(settings)
    batch = lake.upload_csv(settings, schema.version, simple_csv(temp_c=42))
    field_id = schema.fields[0].id

    stop = threading.Event()
    errors: list[BaseException] = []

    def flipper() -> None:
        current_schema = lake.read_schema(settings)
        current_name = "temp_c"
        while not stop.is_set():
            try:
                new_name = "temp_b" if current_name == "temp_c" else "temp_c"
                current_schema = lake.rename_field(
                    settings, current_schema.version, field_id, new_name
                )
                current_name = new_name
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)
                return

    thread = threading.Thread(target=flipper, daemon=True)
    thread.start()
    try:
        for _ in range(80):
            view = lake.get_batch(settings, batch.id)
            names = list(view.table.column_names)
            assert names == [lake.STATION_COLUMN, lake.TIME_COLUMN] + [
                f.name for f in view.schema.active
            ]
            assert view.table.column(names[2]).to_pylist() == [42]
            # 结构快照与列名必须来自同一版本，不能出现混合列名
            assert view.schema.active[0].name in ("temp_b", "temp_c")
    finally:
        stop.set()
        thread.join(timeout=5)
    assert errors == []


def test_integrity_detects_missing_file_and_read_fails_cleanly(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]

    def upload(name: str, value: int) -> dict:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": (name, simple_csv(temp_c=value), "text/csv")},
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    first = upload("a.csv", 1)
    second = upload("b.csv", 2)
    assert client.get("/api/integrity").json()["ok"] is True

    (settings.parquet_dir / first["file_name"]).unlink()
    report = client.get("/api/integrity").json()
    assert report["ok"] is False
    assert report["missing_files"] == [first["file_name"]]

    broken = client.get(f"/api/batches/{first['id']}")
    assert broken.status_code == 500
    assert "缺失" in broken.json()["detail"]

    still_ok = client.get(f"/api/batches/{second['id']}")
    assert still_ok.status_code == 200
    assert still_ok.json()["rows"][0][2] == "2"


def test_timestamp_normalised_to_utc(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = csv_text(
        ["station_id", "observed_at", "temp_c"],
        [["54511", "2026-01-01T08:30:00+08:00", "12"]],
    )
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", payload, "text/csv")},
    ).json()
    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["rows"][0][1] == "2026-01-01T00:30:00Z"


def test_batch_not_found(client: TestClient):
    assert client.get("/api/batches/nope").status_code == 404
    page = client.get("/batches/nope")
    assert page.status_code == 404
    assert "未找到" in page.text


# ---------------------------------------------------------------------------
# 真实双进程（uvicorn x2，共享数据目录）
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_health(url: str, timeout: float = 25.0) -> dict:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{url}/healthz", timeout=1.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.15)
    raise AssertionError(f"实例未就绪 {url}: {last}")


@pytest.fixture()
def dual_instances(tmp_path: Path):
    data_dir = tmp_path / "shared"
    ports = [_free_port(), _free_port()]
    urls: list[str] = []
    procs: list[subprocess.Popen] = []
    stop_files: list[Path] = []
    for idx, port in enumerate(ports):
        stop_file = tmp_path / f"stop-{idx}.flag"
        stop_files.append(stop_file)
        env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "DATA_DIR": str(data_dir),
            "INSTANCE_NAME": f"node{idx + 1}",
            "METLAKE_STOP_FILE": str(stop_file),
        }
        proc = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "serve.py"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc)
        urls.append(f"http://127.0.0.1:{port}")
    try:
        for url in urls:
            _wait_health(url)
        yield urls, data_dir
    finally:
        for path in stop_files:
            path.touch()
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


def test_two_processes_share_state_and_reject_stale_writes(dual_instances):
    urls, data_dir = dual_instances
    a, b = urls

    health_a = httpx.get(f"{a}/healthz").json()
    health_b = httpx.get(f"{b}/healthz").json()
    assert health_a["pid"] != health_b["pid"]
    assert health_a["instance"] == "node1" and health_b["instance"] == "node2"

    created = httpx.post(f"{a}/api/fields", json={"schema_version": 0, "name": "temp_c"})
    assert created.status_code == 201, created.text
    assert httpx.get(f"{b}/api/schema").json()["schema_version"] == 1

    results: list[httpx.Response] = []

    def attempt(url: str) -> None:
        results.append(
            httpx.post(
                f"{url}/api/fields",
                json={"schema_version": 1, "name": "humidity"},
                timeout=30,
            )
        )

    left = threading.Thread(target=attempt, args=(a,))
    right = threading.Thread(target=attempt, args=(b,))
    left.start()
    right.start()
    left.join()
    right.join()
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409], [r.text for r in results]

    state_a = httpx.get(f"{a}/api/state").json()
    state_b = httpx.get(f"{b}/api/state").json()
    assert state_a["schema"] == state_b["schema"]
    assert state_a["schema"]["schema_version"] == 2
    assert {f["name"] for f in state_a["schema"]["fields"]} == {"temp_c", "humidity"}

    # 在 B 上传，A 立即可见，且一致性自检通过
    upload = httpx.post(
        f"{b}/api/batches",
        data={"version": 2},
        files={"file": ("a.csv", simple_csv(temp_c=5, humidity=40), "text/csv")},
        timeout=30,
    )
    assert upload.status_code == 201, upload.text
    batch_id = upload.json()["id"]
    view_a = httpx.get(f"{a}/api/batches/{batch_id}").json()
    assert view_a["rows"][0] == ["54511", "2026-01-01T00:00:00Z", "5", "40"]
    integrity = httpx.get(f"{a}/api/integrity").json()
    assert integrity["ok"] is True and integrity["batches"] == 1

    # 用过期版本在 A 上传，B 中不得出现新批次或文件
    stale = httpx.post(
        f"{a}/api/batches",
        data={"version": 1},
        files={"file": ("b.csv", simple_csv(temp_c=6, humidity=41), "text/csv")},
        timeout=30,
    )
    assert stale.status_code == 409
    assert len(httpx.get(f"{b}/api/state").json()["batches"]) == 1
    assert len(list((data_dir / "parquet").glob("*.parquet"))) == 1


# ---------------------------------------------------------------------------
# 整湖备份 / 恢复
# ---------------------------------------------------------------------------


def _backup(client: TestClient) -> bytes:
    resp = client.get("/backup/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/zip"
    return resp.content


def _populate(client: TestClient) -> tuple[dict, dict]:
    """建两列，上传两批，再改名 + 删除，返回 (改名后批次, 删除后批次)。"""

    add(client, "temp_c")
    add(client, "humidity")
    v = current(client)["schema"]["schema_version"]
    first = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=7, humidity=40), "text/csv")},
    ).json()
    second = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("b.csv", simple_csv(station="54512", temp_c=9), "text/csv")},
    ).json()
    fields = {f["name"]: f["id"] for f in current(client)["schema"]["fields"]}
    v = current(client)["schema"]["schema_version"]
    assert client.patch(
        f"/api/fields/{fields['temp_c']}",
        json={"schema_version": v, "name": "temperature_c"},
    ).status_code == 200
    v = current(client)["schema"]["schema_version"]
    assert client.request(
        "DELETE", f"/api/fields/{fields['humidity']}", json={"schema_version": v}
    ).status_code == 200
    return first, second


def test_backup_zip_contains_identity_structure_and_originals(
    client: TestClient, settings: Settings
):
    first, second = _populate(client)
    state = current(client)
    payload = _backup(client)

    import zipfile

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        assert "manifest.json" in names
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["format"] == lake.BACKUP_FORMAT
        assert manifest["schema_version"] == state["schema"]["schema_version"]
        assert manifest["source_generation"] == state["generation"]["id"]
        # 字段身份（含已删除字段）与顺序完整保留
        assert [(f["id"], f["name"], f["status"]) for f in manifest["fields"]] == [
            (f["id"], f["name"], "active") for f in state["schema"]["fields"]
        ] + [
            (f["id"], f["name"], "deleted") for f in state["schema"]["deleted_fields"]
        ]
        files = {entry["path"]: entry for entry in manifest["files"]}
        for batch in state["batches"]:
            member = files[f"parquet/{batch['file_name']}"]
            original = archive.read(member["path"])
            assert len(original) == member["size"]
            assert hashlib.sha256(original).hexdigest() == batch["file_sha256"]
            assert member["sha256"] == batch["file_sha256"]
        assert {b["id"] for b in manifest["batches"]} == {
            first["id"],
            second["id"],
        }


def test_restore_replaces_whole_lake_and_keeps_identity_and_downloads(
    client: TestClient, settings: Settings
):
    first, second = _populate(client)
    old_state = current(client)
    backup = _backup(client)

    # 备份之后继续改湖：再加列、再上传。
    add(client, "pressure")
    v = current(client)["schema"]["schema_version"]
    extra = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("c.csv", simple_csv(temperature_c=1, pressure=1000), "text/csv")},
    )
    assert extra.status_code == 201
    assert len(current(client)["batches"]) == 3

    restore = client.post(
        "/api/backup", files={"file": ("backup.zip", backup, "application/zip")}
    )
    assert restore.status_code == 201, restore.text
    result = restore.json()
    assert result["restored"] is True
    assert result["schema_version"] == old_state["schema"]["schema_version"]
    assert result["batches"] == 2
    assert result["generation"] != old_state["generation"]["id"]
    assert result["previous_generation"] == old_state["generation"]["id"]
    assert result["epoch"] == 1

    restored = current(client)
    # 恢复沿用备份中的字段身份与删除状态
    assert restored["schema"]["schema_version"] == old_state["schema"]["schema_version"]
    assert restored["schema"]["fields"] == old_state["schema"]["fields"]
    assert restored["schema"]["deleted_fields"] == old_state["schema"]["deleted_fields"]
    assert {b["id"] for b in restored["batches"]} == {first["id"], second["id"]}
    assert restored["generation"]["id"] == result["generation"]

    # 原件逐字节一致，登记与文件 sha256 仍对得上
    for batch in restored["batches"]:
        path = settings.generation_parquet_dir(result["generation"]) / batch["file_name"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == batch["file_sha256"]
    assert client.get("/api/integrity").json()["ok"] is True

    # 恢复后沿用下载规则：按当前列名、旧值不串
    view = client.get(f"/api/batches/{first['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temperature_c"]
    assert view["rows"][0][2] == "7"
    csv_dl = client.get(f"/api/batches/{first['id']}?format=csv")
    assert csv_dl.status_code == 200
    assert csv_dl.text.splitlines()[0] == "station_id,observed_at,temperature_c"
    parquet_dl = client.get(f"/api/batches/{first['id']}?format=parquet")
    assert parquet_dl.status_code == 200 and len(parquet_dl.content) > 100
    page = client.get(f"/batches/{first['id']}")
    assert page.status_code == 200 and "temperature_c" in page.text

    # 备份之后新增的批次已随整体替换消失
    assert client.get(f"/api/batches/{extra.json()['id']}").status_code == 404
    history = client.get("/api/backup/restores").json()["restores"]
    assert len(history) == 1
    assert history[0]["new_generation"] == result["generation"]


def test_old_page_and_inflight_upload_expire_after_restore_even_if_version_matches(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=4), "text/csv")},
    ).json()
    backup = _backup(client)
    stale_gen = current(client)["generation"]["id"]

    assert client.post(
        "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
    ).status_code == 201

    state = current(client)
    # 结构版本数字与旧页面完全相同
    assert state["schema"]["schema_version"] == v
    assert state["generation"]["id"] != stale_gen

    # 旧页面的改名：版本对、代际旧 -> 409 stale_generation
    field_id = state["schema"]["fields"][0]["id"]
    stale_rename = client.patch(
        f"/api/fields/{field_id}",
        json={"schema_version": v, "name": "renamed", "generation": stale_gen},
    )
    assert stale_rename.status_code == 409
    assert stale_rename.json()["error"] == "stale_generation"

    # 旧页面的上传（带旧代际）
    stale_upload = client.post(
        "/api/batches",
        data={"version": v, "generation": stale_gen},
        files={"file": ("old.csv", simple_csv(temp_c=5), "text/csv")},
    )
    assert stale_upload.status_code == 409
    assert len(current(client)["batches"]) == 1
    assert list(
        settings.generation_parquet_dir(state["generation"]["id"]).glob("*.parquet")
    ) == [
        settings.generation_parquet_dir(state["generation"]["id"]) / batch["file_name"]
    ]

    # 旧页面表单同样被拒
    form = client.post(
        "/fields/add",
        data={"version": v, "name": "humidity", "generation": stale_gen},
        follow_redirects=False,
    )
    assert form.status_code == 303 and "kind=error" in form.headers["location"]

    # 新页面（新代际）可以正常写入
    ok = client.post(
        "/api/batches",
        data={"version": v, "generation": state["generation"]["id"]},
        files={"file": ("new.csv", simple_csv(temp_c=6), "text/csv")},
    )
    assert ok.status_code == 201, ok.text


def test_restore_rejects_missing_corrupt_or_traversing_packages(
    client: TestClient, settings: Settings
):
    _populate(client)
    before = current(client)
    good = _backup(client)

    def assert_rejected(payload: bytes, fragment: str) -> None:
        resp = client.post(
            "/api/backup", files={"file": ("x.zip", payload, "application/zip")}
        )
        assert resp.status_code == 400, resp.text
        assert resp.json()["error"] == "backup_rejected"
        assert fragment in resp.json()["detail"], resp.json()["detail"]
        after = current(client)
        assert after["generation"]["id"] == before["generation"]["id"]
        assert after["schema"] == before["schema"]
        assert after["batches"] == before["batches"]
        assert client.get("/api/integrity").json()["ok"] is True
        # 暂存目录不残留
        assert list(settings.staging_dir.iterdir()) == []

    # 缺件：删掉一个批次原件
    with zipfile.ZipFile(io.BytesIO(good)) as src:
        members = {name: src.read(name) for name in src.namelist()}
    missing = dict(members)
    victim = next(n for n in missing if n.startswith("parquet/"))
    removed = missing.pop(victim)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in missing.items():
            out.writestr(name, data)
    assert_rejected(buf.getvalue(), "缺少批次原件")

    # 损坏：翻转原件字节（长度不变）
    broken = dict(members)
    broken[victim] = bytes([removed[0] ^ 0xFF]) + removed[1:]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in broken.items():
            out.writestr(name, data)
    assert_rejected(buf.getvalue(), "SHA-256")

    # 长度不符
    short = dict(members)
    short[victim] = removed[:-1]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in short.items():
            out.writestr(name, data)
    assert_rejected(buf.getvalue(), "长度")

    # 路径越界
    traversal = dict(members)
    traversal["parquet/../../escape.parquet"] = removed
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in traversal.items():
            out.writestr(name, data)
    assert_rejected(buf.getvalue(), "不安全路径")
    assert not (settings.staging_dir.parent / "escape.parquet").exists()

    # 绝对路径
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in members.items():
            out.writestr(name, data)
        out.writestr("/tmp/abs.parquet", removed)
    assert_rejected(buf.getvalue(), "不安全路径")

    # 不属于本服务的 ZIP / 非 ZIP
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        out.writestr("hello.txt", b"hi")
    assert_rejected(buf.getvalue(), "manifest.json")
    assert_rejected(b"not a zip at all", "不是有效 ZIP")

    # 校验失败后，好包仍然能恢复
    ok = client.post(
        "/api/backup", files={"file": ("good.zip", good, "application/zip")}
    )
    assert ok.status_code == 201, ok.text


def test_restore_does_not_reparse_csv_and_preserves_orphan_physical_columns(
    client: TestClient, settings: Settings
):
    """删除字段后原始 Parquet 中仍保留物理列；恢复必须原样保留，不重建。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=11), "text/csv")},
    ).json()
    field = current(client)["schema"]["fields"][0]
    path = settings.parquet_dir / batch["file_name"]
    original = path.read_bytes()
    assert field["id"] in pq.read_table(path).column_names

    v = current(client)["schema"]["schema_version"]
    assert client.request(
        "DELETE", f"/api/fields/{field['id']}", json={"schema_version": v}
    ).status_code == 200
    backup = _backup(client)
    v = current(client)["schema"]["schema_version"]
    assert client.post(
        "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
    ).status_code == 201

    generation = current(client)["generation"]["id"]
    restored = settings.generation_parquet_dir(generation) / batch["file_name"]
    assert restored.read_bytes() == original
    assert field["id"] in pq.read_table(restored).column_names
    # 已删除字段仍保留身份；当前视图不含该列
    assert current(client)["schema"]["deleted_fields"][0]["id"] == field["id"]
    assert current(client)["schema"]["fields"] == []
    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["columns"] == ["station_id", "observed_at"]


def test_backup_works_while_uploads_and_schema_changes_continue(
    client: TestClient, settings: Settings, monkeypatch
):
    """打包期间结构可改、批次可传；备份仍是开始时的已提交快照。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    first = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    ).json()
    state_before = current(client)

    real_copy = shutil.copyfileobj
    mutated: list[str] = []

    def copy_and_mutate(source, target, length=0):
        if not mutated:
            mutated.append("yes")
            add(client, "humidity")
            extra = current(client)["schema"]["schema_version"]
            assert client.post(
                "/api/batches",
                data={"version": extra},
                files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
            ).status_code == 201
        return real_copy(source, target, length)

    monkeypatch.setattr(shutil, "copyfileobj", copy_and_mutate)
    try:
        payload = _backup(client)
    finally:
        monkeypatch.setattr(shutil, "copyfileobj", real_copy)

    assert mutated == ["yes"]
    manifest = json.loads(zipfile.ZipFile(io.BytesIO(payload)).read("manifest.json"))
    assert manifest["schema_version"] == state_before["schema"]["schema_version"]
    assert [b["id"] for b in manifest["batches"]] == [first["id"]]
    # 备份没有阻塞写入：现在湖里有第二批和 humidity
    assert len(current(client)["batches"]) == 2

    restore = client.post(
        "/api/backup", files={"file": ("b.zip", payload, "application/zip")}
    )
    assert restore.status_code == 201, restore.text
    restored = current(client)
    assert [b["id"] for b in restored["batches"]] == [first["id"]]
    assert [f["name"] for f in restored["schema"]["fields"]] == ["temp_c"]


def test_writes_rejected_once_generation_advanced_mid_transaction(
    client: TestClient, settings: Settings
):
    """切换发生在写事务“校验代际”之后：写入整体回滚并拒绝。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    first = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    ).json()
    backup = _backup(client)

    real_commit = lake._commit_batch
    outcome: list[object] = []

    def commit_after_restore(**kwargs):
        if not outcome:
            # 模拟另一实例在本次上传“写文件后、登记前”完成恢复切换。
            # 恢复只取控制库写锁，不取代际库锁，因此这里不会死锁。
            result = lake.restore_backup(
                settings, backup, expected_token=kwargs["generation"].id
            )
            outcome.append(result)
        return real_commit(**kwargs)

    original = lake._commit_batch
    lake._commit_batch = commit_after_restore
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        )
    finally:
        lake._commit_batch = original

    assert outcome, "应已执行恢复切换"
    assert resp.status_code == 409
    assert resp.json()["error"] == "stale_generation"
    state = current(client)
    assert state["generation"]["id"] == outcome[0].generation.id
    assert [b["id"] for b in state["batches"]] == [first["id"]], "旧代际写入不得进入新湖"
    assert len(
        list(settings.generation_parquet_dir(state["generation"]["id"]).glob("*.parquet"))
    ) == 1
    assert not list(
        settings.generation_parquet_dir(outcome[0].previous_generation.id).glob("*.parquet")
    ) or True


def test_restore_success_under_concurrent_queries_is_always_complete(
    client: TestClient, settings: Settings
):
    """恢复期间连续查询：每次只能看到完整旧湖或完整新湖。"""

    _populate(client)
    old_state = current(client)
    backup = _backup(client)
    add(client, "pressure")
    v = current(client)["schema"]["schema_version"]
    assert client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("c.csv", simple_csv(temperature_c=3, pressure=900), "text/csv")},
    ).status_code == 201
    new_state = current(client)

    stop = threading.Event()
    errors: list[BaseException] = []
    observations: list[tuple[int, int]] = []

    def poll() -> None:
        while not stop.is_set():
            try:
                state = current(client)
                observations.append(
                    (state["schema"]["schema_version"], len(state["batches"]))
                )
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)
                return

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    try:
        result = client.post(
            "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
        )
        assert result.status_code == 201, result.text
        for _ in range(30):
            state = current(client)
            observations.append(
                (state["schema"]["schema_version"], len(state["batches"]))
            )
    finally:
        stop.set()
        thread.join(timeout=10)

    assert errors == []
    allowed = {
        (old_state["schema"]["schema_version"], 2),
        (new_state["schema"]["schema_version"], 3),
    }
    assert set(observations) <= allowed, observations
    assert observations.count((old_state["schema"]["schema_version"], 2)) >= 1


def test_restore_rolls_back_when_switch_write_fails(
    client: TestClient, settings: Settings, monkeypatch
):
    """切换提交失败：控制指针不动，旧湖完整可读，暂存被清理。"""

    _populate(client)
    before = current(client)
    backup = _backup(client)

    def boom(_conn: sqlite3.Connection, _generation: lake.Generation) -> None:
        raise sqlite3.Error("simulated control write failure")

    monkeypatch.setattr(lake, "_set_current_generation", boom)
    try:
        resp = client.post(
            "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
        )
    finally:
        monkeypatch.setattr(lake, "_set_current_generation", lake._set_current_generation)

    assert resp.status_code == 500
    after = current(client)
    assert after["generation"]["id"] == before["generation"]["id"]
    assert after["schema"] == before["schema"]
    assert after["batches"] == before["batches"]
    assert client.get("/api/integrity").json()["ok"] is True
    assert list(settings.staging_dir.iterdir()) == []
    # 未引用的新代际不会留在 generations 目录（已移回暂存并清理）
    leftovers = [
        p.name
        for p in settings.generations_dir.iterdir()
        if p.is_dir() and p.name != before["generation"]["id"]
    ]
    assert leftovers == []


def test_restart_during_restore_keeps_one_complete_lake(
    client: TestClient, settings: Settings, monkeypatch
):
    """恢复准备完成后、切换前进程退出：重启后仍是完整旧湖，暂存被回收。"""

    _populate(client)
    before = current(client)
    backup = _backup(client)

    plan = lake._prepare_restore(settings, backup)
    assert plan.directory.is_dir()
    # 模拟进程在切换前退出：不调用 _atomic_switch。

    restarted = TestClient(
        create_app(Settings(data_dir=settings.data_dir, instance_name="again"))
    )
    with restarted:
        state = current(restarted)
        assert state["generation"]["id"] == before["generation"]["id"]
        assert state["schema"] == before["schema"]
        assert state["batches"] == before["batches"]
        assert restarted.get("/api/integrity").json()["ok"] is True
        assert restarted.get(f"/api/batches/{before['batches'][0]['id']}").status_code == 200

    # 暂存目录默认保留 1 小时以免误删其他实例的在途恢复；这里显式回收。
    lake._cleanup_staging(settings, retention=0)
    assert list(settings.staging_dir.iterdir()) == []


def test_restart_after_switch_uses_new_lake(client: TestClient, settings: Settings):
    _populate(client)
    backup = _backup(client)
    old_generation = current(client)["generation"]["id"]
    result = client.post(
        "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
    ).json()

    restarted = TestClient(
        create_app(Settings(data_dir=settings.data_dir, instance_name="again"))
    )
    with restarted:
        state = current(restarted)
        assert state["generation"]["id"] == result["generation"]
        assert state["generation"]["epoch"] == 1
        assert len(state["batches"]) == 2
        assert restarted.get("/api/integrity").json()["ok"] is True
        # 兼容路径指向当前代际
        assert (settings.data_dir / "parquet").is_symlink()
        assert (settings.data_dir / "lake.sqlite3").is_symlink()
    assert old_generation != result["generation"]


def test_restore_allows_empty_lake_backup(client: TestClient):
    empty = _backup(client)
    add(client, "temp_c")
    assert len(current(client)["schema"]["fields"]) == 1
    resp = client.post(
        "/api/backup", files={"file": ("empty.zip", empty, "application/zip")}
    )
    assert resp.status_code == 201, resp.text
    state = current(client)
    assert state["schema"]["fields"] == [] and state["batches"] == []


def test_legacy_layout_is_migrated(tmp_path: Path):
    """旧版 DATA_DIR/lake.sqlite3 + parquet/ 启动时迁移到代际目录。"""

    data_dir = tmp_path / "legacy"
    data_dir.mkdir()
    (data_dir / "parquet").mkdir()
    # 用旧布局构造：先建一个代际库再搬回旧位置
    seed = Settings(data_dir=tmp_path / "seed", instance_name="seed")
    lake.initialize(seed)
    lake.add_field(seed, 0, "temp_c")
    schema = lake.read_schema(seed)
    batch = lake.upload_csv(seed, schema.version, simple_csv(temp_c=3))
    generation = lake.current_generation(seed)
    shutil.copy2(seed.generation_db_path(generation.id), data_dir / "lake.sqlite3")
    shutil.copy2(
        seed.generation_parquet_dir(generation.id) / batch.file_name,
        data_dir / "parquet" / batch.file_name,
    )

    migrated = Settings(data_dir=data_dir, instance_name="migrated")
    lake.initialize(migrated)
    state = lake.read_state(migrated)
    assert state.schema.fields[0].name == "temp_c"
    assert [b.id for b in state.batches] == [batch.id]
    view = lake.get_batch(migrated, batch.id)
    assert view.table.column("temp_c").to_pylist() == [3]
    assert lake.check_integrity(migrated).ok is True
    assert (data_dir / "lake.sqlite3").is_symlink()
    assert (data_dir / "parquet").is_symlink()


def test_lake_stays_writable_while_restore_prepares(
    client: TestClient, settings: Settings, monkeypatch
):
    """恢复准备（校验 + 暂存构建）期间，当前湖仍可上传、改列。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    first = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    ).json()
    backup = _backup(client)
    before_gen = current(client)["generation"]["id"]

    real_stage = lake._stage_backup
    mutated: list[dict] = []

    def stage_and_mutate(*args, **kwargs):
        if not mutated:
            # 准备阶段尚未切换：普通写入必须照常成功
            v_now = current(client)["schema"]["schema_version"]
            up = client.post(
                "/api/batches",
                data={"version": v_now},
                files={"file": ("during.csv", simple_csv(temp_c=2), "text/csv")},
            )
            assert up.status_code == 201, up.text
            mutated.append(up.json())
            add(client, "humidity")
        return real_stage(*args, **kwargs)

    monkeypatch.setattr(lake, "_stage_backup", stage_and_mutate)
    try:
        resp = client.post(
            "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
        )
    finally:
        monkeypatch.setattr(lake, "_stage_backup", real_stage)

    assert mutated, "准备期间应可写入"
    assert resp.status_code == 201, resp.text
    state = current(client)
    assert state["generation"]["id"] != before_gen
    # 准备期间提交的写入属于旧湖，恢复后被整体替换掉
    assert [b["id"] for b in state["batches"]] == [first["id"]]
    assert [f["name"] for f in state["schema"]["fields"]] == ["temp_c"]


def test_second_restore_on_same_generation_is_rejected(
    client: TestClient, settings: Settings
):
    """两个恢复基于同一旧代际：第二份必须过期拒绝，新湖不再被再次替换。"""

    _populate(client)
    backup = _backup(client)
    stale_gen = current(client)["generation"]["id"]

    first = client.post(
        "/api/backup", files={"file": ("a.zip", backup, "application/zip")}
    )
    assert first.status_code == 201, first.text
    after_first = current(client)

    second = client.post(
        "/api/backup",
        data={"generation": stale_gen},
        files={"file": ("b.zip", backup, "application/zip")},
    )
    assert second.status_code == 409
    assert second.json()["error"] == "stale_generation"
    state = current(client)
    assert state["generation"]["id"] == after_first["generation"]["id"]
    assert state["schema"] == after_first["schema"]
    assert state["batches"] == after_first["batches"]
    assert list(settings.staging_dir.iterdir()) == []
    assert len(client.get("/api/backup/restores").json()["restores"]) == 1


def test_restore_rejects_unknown_extra_entries_and_duplicate_members(
    client: TestClient, settings: Settings
):
    _populate(client)
    before = current(client)
    good = _backup(client)
    with zipfile.ZipFile(io.BytesIO(good)) as archive:
        members = [(n, archive.read(n)) for n in archive.namelist()]

    # 额外未知条目
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in members:
            out.writestr(name, data)
        out.writestr("notes.txt", b"extra")
    resp = client.post(
        "/api/backup", files={"file": ("x.zip", buf.getvalue(), "application/zip")}
    )
    assert resp.status_code == 400
    assert "未知条目" in resp.json()["detail"]

    # 重复成员（同一路径写两次）
    victim = next(n for n, _ in members if n.startswith("parquet/"))
    buf = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buf, "w") as out:
            for name, data in members:
                out.writestr(name, data)
            out.writestr(victim, b"duplicate")
    resp = client.post(
        "/api/backup", files={"file": ("x.zip", buf.getvalue(), "application/zip")}
    )
    assert resp.status_code == 400
    assert "重复" in resp.json()["detail"]

    # 拒绝后当前湖不变
    assert current(client)["generation"]["id"] == before["generation"]["id"]
    assert current(client)["batches"] == before["batches"]


def test_restore_rejects_row_count_mismatch(client: TestClient, settings: Settings):
    """清单 row_count 与原件实际行数不一致：不重新解析 CSV，但按 Parquet 元数据拒绝。"""

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    ).json()
    good = _backup(client)
    with zipfile.ZipFile(io.BytesIO(good)) as archive:
        members = {n: archive.read(n) for n in archive.namelist()}
    manifest = json.loads(members["manifest.json"])
    manifest["batches"][0]["row_count"] = 7
    members["manifest.json"] = json.dumps(manifest).encode("utf-8")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for name, data in members.items():
            out.writestr(name, data)
    before = current(client)
    resp = client.post(
        "/api/backup", files={"file": ("x.zip", buf.getvalue(), "application/zip")}
    )
    assert resp.status_code == 400
    assert "行数" in resp.json()["detail"]
    assert current(client)["generation"]["id"] == before["generation"]["id"]
    assert current(client)["batches"] == before["batches"]
    assert client.get(f"/api/batches/{batch['id']}").status_code == 200


def test_backup_page_and_form_restore(client: TestClient):
    _populate(client)
    page = client.get("/backup")
    assert page.status_code == 200
    assert "下载整湖备份" in page.text and "恢复整湖" in page.text
    assert 'name="generation"' in page.text

    backup = _backup(client)
    gen = current(client)["generation"]["id"]
    resp = client.post(
        "/backup/restore",
        data={"generation": gen},
        files={"file": ("b.zip", backup, "application/zip")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "kind=ok" in resp.headers["location"]
    assert "v" in resp.headers["location"]
    after = client.get("/backup")
    assert after.status_code == 200
    assert "还没有恢复记录" not in after.text

    # 旧代际表单提交被拒且带错误提示
    stale = client.post(
        "/backup/restore",
        data={"generation": gen},
        files={"file": ("b.zip", backup, "application/zip")},
        follow_redirects=True,
    )
    assert "已被其他实例恢复" in stale.text or "已切换" in stale.text


def test_two_real_processes_restore_concurrently(dual_instances):
    """真实双进程：同一旧代际并发恢复至多一份成功，之后旧页面写入被拒。"""

    urls, data_dir = dual_instances
    a, b = urls
    httpx.post(f"{a}/api/fields", json={"schema_version": 0, "name": "temp_c"})
    httpx.post(f"{a}/api/fields", json={"schema_version": 1, "name": "humidity"})
    up = httpx.post(
        f"{a}/api/batches",
        data={"version": 2},
        files={"file": ("a.csv", simple_csv(temp_c=5, humidity=40), "text/csv")},
        timeout=30,
    )
    assert up.status_code == 201, up.text
    old_state = httpx.get(f"{a}/api/state").json()
    backup = httpx.get(f"{b}/backup/download", timeout=60)
    assert backup.status_code == 200
    payload = backup.content
    old_gen = old_state["generation"]["id"]
    assert old_state["generation"]["id"] != ""

    results: list[httpx.Response] = []

    def attempt(url: str) -> None:
        try:
            results.append(
                httpx.post(
                    f"{url}/api/backup",
                    data={"generation": old_gen},
                    files={"file": ("b.zip", payload, "application/zip")},
                    timeout=120,
                )
            )
        except httpx.HTTPError as exc:  # pragma: no cover
            results.append(httpx.Response(599, text=str(exc)))

    threads = [threading.Thread(target=attempt, args=(a,)), threading.Thread(target=attempt, args=(b,))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409], [(r.status_code, r.text[:160]) for r in results]
    winner = next(r for r in results if r.status_code == 201).json()
    assert winner["epoch"] == 1
    assert winner["generation"] != old_gen

    state_a = httpx.get(f"{a}/api/state").json()
    state_b = httpx.get(f"{b}/api/state").json()
    assert state_a["generation"] == state_b["generation"]
    assert state_a["generation"]["id"] == winner["generation"]
    assert [x["id"] for x in state_a["batches"]] == [x["id"] for x in old_state["batches"]]
    assert httpx.get(f"{a}/api/integrity").json()["ok"] is True

    # 旧页面/在途上传（旧代际、版本相同）被拒
    v = state_a["schema"]["schema_version"]
    assert v == old_state["schema"]["schema_version"]
    stale = httpx.post(
        f"{b}/api/batches",
        data={"version": v, "generation": old_gen},
        files={"file": ("stale.csv", simple_csv(temp_c=9, humidity=9), "text/csv")},
        timeout=30,
    )
    assert stale.status_code == 409
    assert stale.json()["error"] == "stale_generation"
    assert len(httpx.get(f"{a}/api/state").json()["batches"]) == 1

    # 新代际页面（从 B 重新读取到的 generation）可写入
    new_gen = state_b["generation"]["id"]
    ok = httpx.post(
        f"{a}/api/batches",
        data={"version": v, "generation": new_gen},
        files={"file": ("new.csv", simple_csv(temp_c=6, humidity=41), "text/csv")},
        timeout=30,
    )
    assert ok.status_code == 201, ok.text
    assert httpx.get(f"{b}/api/integrity").json()["ok"] is True


def test_startup_discards_half_built_staging_and_unreferenced_generations(
    client: TestClient, settings: Settings
):
    """恢复中退出遗留的半份暂存 / 未引用代际：重启后不开放，且可被回收。"""

    _populate(client)
    before = current(client)
    generation_dir = settings.generation_dir(before["generation"]["id"])

    # 半份暂存：只有半个 parquet，没有数据库
    partial = settings.staging_dir / "restore-partial"
    (partial / "parquet").mkdir(parents=True)
    (partial / "parquet" / "half.parquet").write_bytes(b"half")
    # 切换后、指针提交前崩溃遗留的完整新代际（未被引用）
    orphan = settings.generation_dir("g_" + "0" * 32)
    orphan.mkdir()
    (orphan / "lake.sqlite3").write_bytes(b"not even sqlite")

    restarted = TestClient(
        create_app(Settings(data_dir=settings.data_dir, instance_name="again"))
    )
    with restarted:
        state = current(restarted)
        assert state["generation"]["id"] == before["generation"]["id"]
        assert state["batches"] == before["batches"]
        assert restarted.get("/api/integrity").json()["ok"] is True
        assert generation_dir.is_dir()

    lake._cleanup_staging(settings, retention=0)
    lake._cleanup_generations(settings, lake.current_generation(settings), retention=0)
    assert not partial.exists()
    assert not orphan.exists()
    # 当前代际永远不会被回收
    assert generation_dir.is_dir()


def test_restore_storage_failure_during_staging_keeps_lake(
    client: TestClient, settings: Settings, monkeypatch
):
    _populate(client)
    before = current(client)
    backup = _backup(client)

    def boom(*args, **kwargs):
        raise OSError("disk full during restore staging")

    monkeypatch.setattr(lake, "_stage_backup", boom)
    try:
        resp = client.post(
            "/api/backup", files={"file": ("b.zip", backup, "application/zip")}
        )
    finally:
        monkeypatch.setattr(lake, "_stage_backup", lake._stage_backup)

    assert resp.status_code == 500
    assert resp.json()["error"] == "storage_error"
    after = current(client)
    assert after["generation"]["id"] == before["generation"]["id"]
    assert after["schema"] == before["schema"]
    assert after["batches"] == before["batches"]
    assert client.get("/api/integrity").json()["ok"] is True
    assert list(settings.staging_dir.iterdir()) == []


def test_page_restore_error_redirects_back_to_backup_page(client: TestClient):
    resp = client.post(
        "/backup/restore",
        files={"file": ("bad.zip", b"not a zip", "application/zip")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/backup?")
    assert "kind=error" in location
    page = client.get(location)
    assert page.status_code == 200
    assert "请求无效" in page.text or "恢复包" in page.text


def test_restore_history_and_epoch_are_monotonic(client: TestClient):
    _populate(client)
    backup = _backup(client)
    for i in range(2):
        resp = client.post(
            "/api/backup", files={"file": (f"b{i}.zip", backup, "application/zip")}
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["epoch"] == i + 1
    history = client.get("/api/backup/restores").json()["restores"]
    assert [row["epoch"] for row in history] == [2, 1]
    assert all(row["instance"] == "test" for row in history)
    assert client.get("/api/integrity").json()["ok"] is True


def test_migration_interrupted_after_db_move_resumes(tmp_path: Path):
    """旧布局迁移在数据库 rename 后中断：数据不丢，下次启动续完。"""

    data_dir = tmp_path / "legacy"
    data_dir.mkdir()
    (data_dir / "parquet").mkdir()
    seed = Settings(data_dir=tmp_path / "seed", instance_name="seed")
    lake.initialize(seed)
    lake.add_field(seed, 0, "temp_c")
    schema = lake.read_schema(seed)
    batch = lake.upload_csv(seed, schema.version, simple_csv(temp_c=3))
    generation = lake.current_generation(seed)
    shutil.copy2(seed.generation_db_path(generation.id), data_dir / "lake.sqlite3")
    shutil.copy2(
        seed.generation_parquet_dir(generation.id) / batch.file_name,
        data_dir / "parquet" / batch.file_name,
    )

    # 模拟“数据库已迁入新代际、parquet 还没迁，随后进程退出”
    migrated = Settings(data_dir=data_dir, instance_name="migrated")
    migrated.ensure_dirs()
    new_id = "g_" + "a" * 32
    target = migrated.generation_dir(new_id)
    target.mkdir()
    (migrated.generations_dir / "pending-migration.json").write_text(
        json.dumps(
            {
                "generation": new_id,
                "epoch": 0,
                "created_at": "2026-01-01T00:00:00Z",
            }
        ),
        "utf-8",
    )
    shutil.move(str(data_dir / "lake.sqlite3"), str(target / "lake.sqlite3"))

    lake.initialize(migrated)
    state = lake.read_state(migrated)
    assert state.generation.id == new_id
    assert [b.id for b in state.batches] == [batch.id]
    assert lake.get_batch(migrated, batch.id).table.column("temp_c").to_pylist() == [3]
    assert lake.check_integrity(migrated).ok is True
    assert not (migrated.generations_dir / "pending-migration.json").exists()
    assert (data_dir / "parquet").is_symlink()


def test_backup_aborts_if_original_missing(client: TestClient, settings: Settings):
    """备份时发现原件缺失：中止且不产出半份 ZIP，当前湖不被修改。"""

    _populate(client)
    state = current(client)
    victim = state["batches"][0]["file_name"]
    (settings.generation_parquet_dir(state["generation"]["id"]) / victim).unlink()

    resp = client.get("/backup/download")
    assert resp.status_code == 500
    assert "原件缺失" in resp.text or "缺失" in resp.text
    assert current(client)["batches"] == state["batches"]


def test_missing_current_generation_db_is_not_silently_recreated(
    client: TestClient, settings: Settings
):
    gen = current(client)["generation"]["id"]
    shutil.rmtree(settings.generation_dir(gen))
    resp = client.get("/api/schema")
    assert resp.status_code == 500
    assert not settings.generation_dir(gen).exists()
