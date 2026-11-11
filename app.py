"""Flask front end for Iceberg v2 voyage-day manifest recovery.

Primary page: upload ``metadata.json``, the snapshot's Avro manifest list and
the Avro manifest copies (keeping their original file names), choose a UTC
voyage day, and export a CSV that the verification page can read.

The original positional-delete verification page remains available at
``/verify`` for the exported CSV.
"""

from __future__ import annotations

import os
import re
import secrets
import tempfile
import time
from pathlib import Path

from flask import (
    Flask,
    abort,
    render_template,
    request,
    send_file,
)

from checker.avro import looks_like_manifest_list
from checker.engine import CheckResult, run_check
from checker.errors import CheckError
from checker.iceberg import (
    RecoverResult,
    load_table_metadata,
    parse_voyage_date,
    recover_voyage_day,
)
from checker.manifest import parse_manifest

ROOT = Path(__file__).resolve().parent
SAMPLE_DIR = ROOT / "sample"
SAMPLE_DATE = "2026-11-05"
SAMPLE_TICKETS = "4242, 1003, 4218, 8888, 9999"
SAMPLE_TICKET_COLUMN = "ticket_no"

EXPORT_DIR = Path(
    os.environ.get(
        "EXPORT_DIR", str(Path(tempfile.gettempdir()) / "ferry-voyage-exports")
    )
)
EXPORT_TOKEN_RE = re.compile(r"[0-9a-f]{32}")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (
    int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024
)

# Form field aliases --------------------------------------------------------
_METADATA_KEYS = {
    "metadata",
    "metadata_file",
    "metadata_json",
    "metadatajson",
    "table_metadata",
    "table_metadata_json",
    "tablemetadata",
}
_LIST_KEYS = {
    "manifest_list",
    "manifest_list_file",
    "manifestlist",
    "snapshot_manifest_list",
    "snapshot_list",
    "snapshotlist",
    "list",
    "avro_list",
    "list_file",
}
_MANIFEST_KEYS = {
    "manifests",
    "manifest_files",
    "manifest_copies",
    "avro_manifests",
    "manifest",
    "files",
    "avro",
}
_DATE_KEYS = {
    "voyage_date",
    "date",
    "target_date",
    "utc_date",
    "utc_voyage_date",
    "day",
    "voyage_day",
}


def _normalise_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _clean_basename(value: str) -> str:
    name = os.path.basename(str(value).replace("\\", "/")).strip()
    return name


def _all_uploaded_parts():
    """Return every non-empty file part exactly once, as (field, storage)."""
    parts = []
    seen: set[int] = set()
    for key in request.files:
        for storage in request.files.getlist(key):
            if storage is None or not storage.filename:
                continue
            identity = id(storage)
            if identity in seen:
                continue
            seen.add(identity)
            parts.append((key, storage))
    return parts


def _save_uploads(parts, temp_dir: Path) -> tuple[dict[str, Path], list[dict]]:
    """Save uploads and enforce globally unique original file names."""
    by_name: dict[str, Path] = {}
    saved: list[dict] = []
    for index, (key, storage) in enumerate(parts, start=1):
        name = _clean_basename(storage.filename)
        if not name:
            raise CheckError("上传文件名为空。")
        if name in by_name:
            raise CheckError(
                f"上传文件名重复：{name}。所有上传文件名必须唯一"
                "（manifest 副本需保留原文件名）。",
                files=[name],
            )
        destination = temp_dir / f"upload-{index:04d}.bin"
        try:
            storage.save(destination)
        except Exception as exc:
            raise CheckError(
                f"无法保存上传文件 “{name}”：{exc}", files=[name]
            ) from None
        by_name[name] = destination
        saved.append(
            {
                "field": _normalise_key(key),
                "name": name,
                "path": destination,
            }
        )
    return by_name, saved


def _parts_for(saved: list[dict], aliases: set[str]) -> list[dict]:
    return [item for item in saved if item["field"] in aliases]


def _single_part(parts: list[dict], label: str) -> dict | None:
    if len(parts) > 1:
        raise CheckError(
            f"{label} 只能上传一个文件，收到 {len(parts)} 个："
            + "、".join(item["name"] for item in parts),
            files=[item["name"] for item in parts],
        )
    return parts[0] if parts else None


def _find_metadata_part(saved: list[dict]) -> dict | None:
    dedicated = _parts_for(saved, _METADATA_KEYS)
    if dedicated:
        return _single_part(dedicated, "metadata.json")
    for item in saved:
        if item["name"].lower() in ("metadata.json", "table-metadata.json"):
            return item
    # Last resort: a small JSON object carrying format-version.
    for item in saved:
        try:
            data = item["path"].read_bytes()[: 2 * 1024 * 1024]
            text = data.decode("utf-8-sig")
            if '"format-version"' in text:
                return item
        except (OSError, UnicodeDecodeError):
            continue
    return None


def _manifest_list_part(saved: list[dict]) -> dict | None:
    dedicated = _parts_for(saved, _LIST_KEYS)
    if dedicated:
        return _single_part(dedicated, "manifest list")
    return None


def _identify_manifest_list_by_snapshot(
    metadata, by_name: dict[str, Path]
) -> tuple[str, Path] | None:
    if metadata.current_snapshot is None:
        return None
    path = metadata.current_snapshot.manifest_list
    name = _clean_basename(path.split("?", 1)[0])
    for candidate in (name, name.replace("%20", " ")):
        if candidate in by_name:
            return candidate, by_name[candidate]
    return None


def _identify_manifest_list_by_schema(
    candidates: list[dict],
) -> dict | None:
    matches = [
        item for item in candidates if looks_like_manifest_list(item["path"])
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _selected_date_value() -> str:
    for key in request.form:
        if _normalise_key(key) in _DATE_KEYS:
            values = request.form.getlist(key)
            for value in values:
                if str(value).strip():
                    return str(value).strip()
    for key in request.args:
        if _normalise_key(key) in _DATE_KEYS:
            value = request.args.get(key, "").strip()
            if value:
                return value
    return ""


def _wants_csv() -> bool:
    path = (request.path or "").lower().rstrip("/")
    if path.endswith(".csv") or path.endswith("/download"):
        return True
    try:
        form = request.form
    except Exception:
        form = {}
    for source in (request.args, form):
        for key in ("format", "output", "download"):
            value = str(source.get(key, "")).strip().lower()
            if value in ("csv", "1", "true", "yes"):
                return True
    accept = (request.headers.get("Accept") or "").lower()
    return "text/csv" in accept and "text/html" not in accept


def _write_export(csv_text: str, voyage_date: str) -> str:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    # Best-effort cleanup of old exports.
    now = time.time()
    for stale in EXPORT_DIR.glob("*.csv"):
        try:
            if now - stale.stat().st_mtime > 24 * 60 * 60:
                stale.unlink()
        except OSError:
            pass
    token = secrets.token_hex(16)
    (EXPORT_DIR / f"{token}.csv").write_text(csv_text, encoding="utf-8")
    return token


def _render_recover(
    *,
    result: RecoverResult | None = None,
    error: CheckError | None = None,
    voyage_date_text: str = "",
    source: str = "",
    export_token: str = "",
    status: int = 200,
):
    return (
        render_template(
            "recover.html",
            result=result,
            error=error,
            voyage_date_text=voyage_date_text,
            source=source,
            export_token=export_token,
            sample_date=SAMPLE_DATE,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
        ),
        status,
    )


def _csv_error(error: CheckError, status: int = 400):
    lines = [error.message]
    if error.files:
        lines.append("涉及路径：")
        lines.extend(error.files)
    return "\n".join(lines) + "\n", status, {
        "Content-Type": "text/plain; charset=utf-8"
    }


def _handle_prepare():
    voyage_date_text = _selected_date_value()
    parts = _all_uploaded_parts()

    try:
        if not parts:
            raise CheckError(
                "请上传表 metadata.json、快照的 Avro manifest list 和 "
                "Avro manifest 副本。"
            )
        # Re-evaluate with saved names because _find_metadata_part needs paths.
        if voyage_date_text:
            voyage_date = parse_voyage_date(voyage_date_text)
        else:
            raise CheckError("请选择 UTC 航次日（YYYY-MM-DD）。")

        with tempfile.TemporaryDirectory(prefix="ferry-voyage-") as temp:
            by_name, saved = _save_uploads(parts, Path(temp))
            metadata_part = _find_metadata_part(saved)
            if metadata_part is None:
                raise CheckError(
                    "请上传表 metadata.json（format-version 2）。"
                )
            try:
                metadata = load_table_metadata(metadata_part["path"].read_bytes())
            except OSError as exc:
                raise CheckError(
                    f"无法读取 metadata.json：{exc}",
                    files=[metadata_part["name"]],
                ) from None

            list_part = _manifest_list_part(saved)
            list_label = ""
            if list_part is None:
                lookup_names = {
                    name: path
                    for name, path in by_name.items()
                    if name != metadata_part["name"]
                }
                found = _identify_manifest_list_by_snapshot(
                    metadata, lookup_names
                )
                if found is not None:
                    list_name, list_path = found
                    list_label = metadata.current_snapshot.manifest_list
                    list_part = {
                        "field": "manifest_list",
                        "name": list_name,
                        "path": list_path,
                    }
                else:
                    fallback = _identify_manifest_list_by_schema(
                        [
                            item
                            for item in saved
                            if item["name"] != metadata_part["name"]
                        ]
                    )
                    if fallback is None:
                        snapshot_path = (
                            metadata.current_snapshot.manifest_list
                            if metadata.current_snapshot is not None
                            else ""
                        )
                        raise CheckError(
                            "找不到快照的 Avro manifest list"
                            + (f"：{snapshot_path}" if snapshot_path else "。"),
                            files=[snapshot_path] if snapshot_path else [],
                        )
                    list_part = fallback
                    if metadata.current_snapshot is not None:
                        list_label = metadata.current_snapshot.manifest_list
            if not list_label:
                if metadata.current_snapshot is not None:
                    list_label = metadata.current_snapshot.manifest_list
                else:
                    list_label = list_part["name"]

            excluded = {metadata_part["name"], list_part["name"]}
            manifest_files: dict[str, Path] = {
                name: path
                for name, path in by_name.items()
                if name not in excluded
            }
            result = recover_voyage_day(
                metadata,
                list_part["path"],
                list_label,
                manifest_files,
                voyage_date,
            )
            csv_text = result.csv_text()
            try:
                token = (
                    _write_export(csv_text, voyage_date.isoformat())
                    if result.files
                    else ""
                )
            except OSError:
                token = ""
    except CheckError as exc:
        if _wants_csv():
            return _csv_error(exc)
        return _render_recover(
            error=exc,
            voyage_date_text=voyage_date_text,
            source="upload",
            status=400,
        )

    if _wants_csv():
        if result.is_empty:
            return csv_text, 200, {
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": (
                    "attachment; filename="
                    f"voyage-{voyage_date.isoformat()}-empty.csv"
                ),
                "X-Result": "empty",
            }
        return csv_text, 200, {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": (
                f"attachment; filename=voyage-{voyage_date.isoformat()}.csv"
            ),
        }

    return _render_recover(
        result=result,
        voyage_date_text=voyage_date.isoformat(),
        source="upload",
        export_token=token,
    )


def _sample_inputs():
    metadata_path = SAMPLE_DIR / "metadata.json"
    if not metadata_path.is_file():
        raise CheckError(
            "内置样例缺失：sample/metadata.json。请运行 tools/make_sample.py。"
        )
    metadata = load_table_metadata(metadata_path.read_bytes())
    if metadata.current_snapshot is None:
        raise CheckError("内置样例 metadata.json 没有当前快照。")
    list_path = SAMPLE_DIR / _clean_basename(
        metadata.current_snapshot.manifest_list
    )
    if not list_path.is_file():
        raise CheckError(
            "内置样例缺少 manifest list："
            f"{metadata.current_snapshot.manifest_list}",
            files=[metadata.current_snapshot.manifest_list],
        )
    manifest_files = {
        path.name: path
        for path in SAMPLE_DIR.glob("*.avro")
        if path.name != list_path.name
    }
    return metadata, list_path, manifest_files


def _sample_date() -> str:
    value = _selected_date_value()
    if not value:
        value = request.args.get("voyage_date") or request.args.get("date") or ""
    return value.strip() or SAMPLE_DATE


def _handle_sample():
    date_text = _sample_date()
    try:
        voyage_date = parse_voyage_date(date_text)
        metadata, list_path, manifest_files = _sample_inputs()
        result = recover_voyage_day(
            metadata,
            list_path,
            metadata.current_snapshot.manifest_list,
            manifest_files,
            voyage_date,
        )
        try:
            token = (
                _write_export(result.csv_text(), voyage_date.isoformat())
                if result.files
                else ""
            )
        except OSError:
            token = ""
    except CheckError as exc:
        if _wants_csv():
            return _csv_error(exc)
        return _render_recover(
            error=exc,
            voyage_date_text=date_text,
            source="sample",
            status=400,
        )

    if _wants_csv():
        if result.is_empty:
            return result.csv_text(), 200, {
                "Content-Type": "text/csv; charset=utf-8",
                "X-Result": "empty",
            }
        return result.csv_text(), 200, {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": (
                f"attachment; filename=sample-voyage-{voyage_date.isoformat()}.csv"
            ),
        }

    return _render_recover(
        result=result,
        voyage_date_text=voyage_date.isoformat(),
        source="sample",
        export_token=token,
    )


@app.get("/")
def index():
    return _render_recover(voyage_date_text=SAMPLE_DATE)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.route("/sample", methods=["GET", "POST"])
def sample():
    return _handle_sample()


@app.post("/")
@app.post("/prepare")
@app.post("/prepare.csv")
@app.post("/recover")
@app.post("/recover.csv")
@app.post("/export")
@app.post("/export.csv")
@app.post("/download")
@app.post("/download.csv")
def prepare():
    return _handle_prepare()


@app.get("/download/<token>")
def download_export(token: str):
    if not EXPORT_TOKEN_RE.fullmatch(token):
        abort(404)
    path = EXPORT_DIR / f"{token}.csv"
    if not path.is_file():
        abort(404)
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{token}.csv",
    )


# ---------------------------------------------------------------------------
# Existing positional-delete verification page (kept for the exported CSV)


def parse_tickets(text: str) -> list[int]:
    tokens = [
        token
        for token in re.split(r"[,\s，、;；]+", (text or "").strip())
        if token
    ]
    if not tokens:
        raise CheckError("请输入至少一个整数票号。")
    tickets: list[int] = []
    for token in tokens:
        if not re.fullmatch(r"[+-]?\d+", token):
            raise CheckError(f"票号 “{token}” 不是整数。")
        tickets.append(int(token))
    return tickets


def _save_parquet_uploads(file_storages, temp_dir: Path) -> dict[str, Path]:
    uploaded: dict[str, Path] = {}
    for file_storage in file_storages:
        if file_storage is None or not file_storage.filename:
            continue
        name = _clean_basename(file_storage.filename)
        if not name:
            raise CheckError("上传的 Parquet 文件名为空。")
        if name in uploaded:
            raise CheckError(
                f"上传文件名重复：{name}。清单中的 uploaded_filename 必须唯一。",
                files=[name],
            )
        destination = temp_dir / f"upload-{len(uploaded) + 1:04d}.parquet"
        try:
            file_storage.save(destination)
        except Exception as exc:
            raise CheckError(
                f"无法保存上传文件 “{name}”：{exc}", files=[name]
            ) from None
        if destination.stat().st_size == 0:
            raise CheckError(f"上传文件 “{name}” 为空。", files=[name])
        uploaded[name] = destination
    return uploaded


def _sample_uploaded_paths(manifest) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for entry in manifest.required_entries:
        candidate = SAMPLE_DIR / entry.uploaded_filename
        if candidate.is_file():
            paths[entry.uploaded_filename] = candidate
    return paths


def _render_verify(
    *,
    result: CheckResult | None = None,
    error: CheckError | None = None,
    tickets_text: str = "",
    ticket_column: str = "ticket_no",
    source: str = "",
    status: int = 200,
):
    return (
        render_template(
            "index.html",
            result=result,
            error=error,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source=source,
            sample_tickets=SAMPLE_TICKETS,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
        ),
        status,
    )


@app.get("/verify")
def verify_index():
    return _render_verify()


@app.get("/verify/sample")
def verify_sample():
    try:
        manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
        result = run_check(
            manifest,
            _sample_uploaded_paths(manifest),
            parse_tickets(SAMPLE_TICKETS),
            SAMPLE_TICKET_COLUMN,
        )
    except CheckError as exc:
        return _render_verify(
            error=exc,
            tickets_text=SAMPLE_TICKETS,
            ticket_column=SAMPLE_TICKET_COLUMN,
            source="sample",
            status=400,
        )
    return _render_verify(
        result=result,
        tickets_text=SAMPLE_TICKETS,
        ticket_column=SAMPLE_TICKET_COLUMN,
        source="sample",
    )


def _verify_check():
    tickets_text = request.form.get("tickets", "")
    ticket_column = request.form.get("ticket_column", "ticket_no").strip()
    manifest_file = request.files.get("manifest")
    parquet_files = request.files.getlist("files")

    if manifest_file is None or not manifest_file.filename:
        return _render_verify(
            error=CheckError(
                "请上传 Iceberg v2 有效文件清单（CSV 或 JSON）。"
            ),
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    try:
        tickets = parse_tickets(tickets_text)
        manifest = parse_manifest(manifest_file.read())
        with tempfile.TemporaryDirectory(prefix="iceberg-pos-delete-") as temp:
            uploaded_paths = _save_parquet_uploads(parquet_files, Path(temp))
            result = run_check(
                manifest, uploaded_paths, tickets, ticket_column
            )
    except CheckError as exc:
        return _render_verify(
            error=exc,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    return _render_verify(
        result=result,
        tickets_text=tickets_text,
        ticket_column=ticket_column,
        source="upload",
    )


@app.post("/verify/check")
def verify_check():
    return _verify_check()


def _looks_like_recovery_request() -> bool:
    for key in request.files:
        if _normalise_key(key) in (
            _METADATA_KEYS | _LIST_KEYS | {"manifest_list"}
        ):
            return True
        for storage in request.files.getlist(key):
            name = _clean_basename(storage.filename or "").lower()
            if name in ("metadata.json", "table-metadata.json"):
                return True
    for key in request.form:
        if _normalise_key(key) in (_DATE_KEYS | _LIST_KEYS):
            return True
    for key in request.args:
        if _normalise_key(key) in (_DATE_KEYS | _LIST_KEYS):
            return True
    return False


@app.post("/check")
def check():
    """Compatibility endpoint: recovery unless it is the verify form."""
    if _looks_like_recovery_request():
        return _handle_prepare()
    return _verify_check()


@app.errorhandler(413)
def payload_too_large(_error):
    message = CheckError(
        "上传内容超过大小限制 "
        f"（{app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB）。"
    )
    if _wants_csv():
        return _csv_error(message, status=413)
    return _render_recover(error=message, source="upload", status=413)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        debug=bool(os.environ.get("FLASK_DEBUG")),
    )
