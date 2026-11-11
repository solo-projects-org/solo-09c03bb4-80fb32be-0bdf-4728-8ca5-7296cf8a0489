from __future__ import annotations

import io
from pathlib import Path

import pytest

from app import app

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def sample_uploads():
    files = []
    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ):
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    return manifest, files


def test_index(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Iceberg" in response.get_data(as_text=True)


def test_health(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_verify_sample_shows_skipped_row_groups_and_deleted_ticket(client):
    response = client.get("/verify/sample")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "内置样例" in html
    assert "4242" in html
    assert "跳过 4 / 5 个行组" in html
    assert "已删除" in html
    assert "delete-pos-0001.parquet" in html
    # The stale, lower-sequence delete must be called out as ignored.
    assert "delete-pos-stale-0001.parquet" in html


def test_upload_round_trip(client):
    manifest, files = sample_uploads()
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": files,
            "tickets": "4242, 1003, 4218, 8888, 9999",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "核对结果" in html
    assert "文件内行号" in html
    assert "delete-pos-0001.parquet" in html
    assert "未找到" in html


def test_missing_file_returns_error_and_no_ticket_results(client):
    manifest = (
        io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
        "manifest.csv",
    )
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "核对未完成" in html
    assert "data-0001.parquet" in html
    assert "没有返回任何票号的核对结果" in html
    assert "<h2>核对结果</h2>" not in html
    assert 'class="ticket-card' not in html


def test_invalid_ticket_is_rejected(client):
    manifest, files = sample_uploads()
    response = client.post(
        "/check",
        data={
            "manifest": manifest,
            "files": files,
            "tickets": "4242, nope",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "不是整数" in html


def test_missing_manifest_is_rejected(client):
    response = client.post(
        "/check",
        data={"tickets": "4242", "ticket_column": "ticket_no"},
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "请上传" in html


def test_bad_manifest_is_rejected(client):
    response = client.post(
        "/check",
        data={
            "manifest": (io.BytesIO(b"not,a,manifest\n1,2,3\n"), "manifest.csv"),
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "缺少必需列" in html


def test_duplicate_uploaded_name_is_rejected(client):
    manifest, files = sample_uploads()
    duplicate = list(files) + [files[0]]
    response = client.post(
        "/verify/check",
        data={
            "manifest": manifest,
            "files": duplicate,
            "tickets": "4242",
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "上传文件名重复" in html


# ---------------------------------------------------------------------------
# Voyage-day recovery


def recovery_files():
    metadata = (
        io.BytesIO((SAMPLE / "metadata.json").read_bytes()),
        "metadata.json",
        "application/json",
    )
    manifest_list = (
        io.BytesIO((SAMPLE / "snap-1001.avro").read_bytes()),
        "snap-1001.avro",
        "application/octet-stream",
    )
    manifests = [
        (
            io.BytesIO(path.read_bytes()),
            path.name,
            "application/octet-stream",
        )
        for path in sorted(SAMPLE.glob("manifest-*.avro"))
    ]
    return metadata, manifest_list, manifests


def test_recovery_sample_contains_day_and_hour_partitions(client):
    response = client.get("/sample")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "内置样例" in html
    assert "UTC 航次日 2026-11-05" in html
    assert "manifest-day-data-1001.avro" in html
    assert "manifest-hour-data-1001.avro" in html
    assert "data-0001.parquet" in html
    assert "delete-pos-0002.parquet" in html
    # DELETED entries and entries for other days must not be exported.
    assert "data-hour-other.parquet" not in html


def test_recovery_sample_csv_is_verification_page_format(client):
    response = client.get("/sample?format=csv")
    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("text/csv")
    text = response.get_data(as_text=True)
    assert text.splitlines()[0] == (
        "original_path,uploaded_filename,file_type,data_sequence_number"
    )
    assert "data-0001.parquet,data,7" in text
    assert "delete-pos-0001.parquet,position_delete,12" in text


def test_recovery_upload_round_trip(client):
    metadata, manifest_list, manifests = recovery_files()
    response = client.post(
        "/prepare",
        data={
            "metadata": metadata,
            "manifest_list": manifest_list,
            "manifests": manifests,
            "voyage_date": "2026-11-05",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "UTC 航次日 2026-11-05" in html
    assert "data-0001.parquet" in html
    assert "数据序列号" in html


def test_recovery_missing_skipped_manifest_is_fine(client):
    metadata, manifest_list, manifests = recovery_files()
    kept = [item for item in manifests if item[1] not in (
        "manifest-day-other-1001.avro",
        "manifest-hour-other-1001.avro",
    )]
    response = client.post(
        "/prepare",
        data={
            "metadata": metadata,
            "manifest_list": manifest_list,
            "manifests": kept,
            "voyage_date": "2026-11-05",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert "data-0001.parquet" in response.get_data(as_text=True)


def test_recovery_missing_required_manifest_aborts_with_path(client):
    metadata, manifest_list, manifests = recovery_files()
    kept = [item for item in manifests if item[1] != "manifest-hour-delete-1001.avro"]
    response = client.post(
        "/prepare",
        data={
            "metadata": metadata,
            "manifest_list": manifest_list,
            "manifests": kept,
            "voyage_date": "2026-11-05",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "未生成文件清单" in html
    assert "manifest-hour-delete-1001.avro" in html
    assert "original_path,uploaded_filename" not in html


def test_recovery_empty_day_shows_empty(client):
    metadata, manifest_list, manifests = recovery_files()
    response = client.post(
        "/prepare",
        data={
            "metadata": metadata,
            "manifest_list": manifest_list,
            "manifests": manifests,
            "voyage_date": "2030-01-01",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "当天没有文件" in html


def test_recovery_duplicate_uploaded_name_is_rejected(client):
    metadata, manifest_list, manifests = recovery_files()
    duplicate = [
        (
            io.BytesIO(b"x"),
            "metadata.json",
            "application/octet-stream",
        )
    ]
    response = client.post(
        "/prepare",
        data={
            "metadata": metadata,
            "manifest_list": manifest_list,
            "manifests": manifests + duplicate,
            "voyage_date": "2026-11-05",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "上传文件名重复" in html
