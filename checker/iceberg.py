"""Iceberg v2 voyage-day manifest recovery.

Given a table ``metadata.json``, the snapshot's Avro manifest list, locally
uploaded copies of the manifests (keeping their original file names), and a
UTC voyage date, this module:

* interprets each manifest-list entry's partition bounds using that entry's
  own partition spec (single ``day`` or ``hour`` field on one departure-time
  column);
* skips a manifest only when its bounds prove it cannot intersect the day;
* if bounds are absent or cannot be decoded, reads the manifest;
* reads only manifests that are needed, filters their entries by partition
  value, and excludes ``DELETED`` entries;
* preserves the entry's data sequence number and inherits the manifest's
  sequence number only for ``ADDED`` entries with a null value.

Any missing or corrupt manifest that must be read, or any selected entry whose
sequence number cannot be determined, aborts the whole export with the exact
manifest path and no file list.
"""

from __future__ import annotations

import csv
import io
import json
import posixpath
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote

from .avro import AvroError, read_avro
from .errors import CheckError

EXPORT_COLUMNS = (
    "original_path",
    "uploaded_filename",
    "file_type",
    "data_sequence_number",
)

_STATUS_ADDED = 1
_STATUS_EXISTING = 0
_STATUS_DELETED = 2

_STATUS_NAMES = {
    "added": _STATUS_ADDED,
    "existing": _STATUS_EXISTING,
    "deleted": _STATUS_DELETED,
}

_CONTENT_DATA = 0
_CONTENT_POSITION_DELETE = 1
_CONTENT_EQUALITY_DELETE = 2


class FileType(str, Enum):
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"


@dataclass(frozen=True)
class PartitionField:
    name: str
    transform: str
    source_id: int | None
    source_name: str


@dataclass
class PartitionSpec:
    spec_id: int
    fields: list[PartitionField]

    @property
    def field(self) -> PartitionField:
        return self.fields[0]


@dataclass
class Snapshot:
    snapshot_id: int
    manifest_list: str
    timestamp_ms: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableMetadata:
    raw: dict[str, Any]
    format_version: int
    specs: dict[int, PartitionSpec]
    default_spec_id: int | None
    snapshots: list[Snapshot]
    current_snapshot: Snapshot | None
    source_column: str
    spec_problems: dict[int, str] = field(default_factory=dict)


@dataclass
class ManifestListRecord:
    index: int
    manifest_path: str
    spec_id: int
    content: int
    sequence_number: int | None
    partitions: list[dict[str, Any]] | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class ManifestReport:
    index: int
    manifest_path: str
    spec_id: int
    transform: str
    action: str  # "read" | "skip"
    reason: str
    lower: int | None = None
    upper: int | None = None
    selected_entries: int = 0


@dataclass
class SelectedFile:
    original_path: str
    suggested_upload_name: str
    file_type: FileType
    sequence_number: int
    status: int
    manifest_path: str
    partition_value: int
    partition_spec_id: int
    sequence_source: str  # "entry" | "manifest"
    file_format: str = "PARQUET"
    order: int = 0


@dataclass
class RecoverResult:
    voyage_date: date
    manifest_list_path: str
    files: list[SelectedFile]
    manifest_reports: list[ManifestReport]
    metadata: TableMetadata
    warnings: list[str] = field(default_factory=list)

    @property
    def read_manifest_count(self) -> int:
        return sum(1 for report in self.manifest_reports if report.action == "read")

    @property
    def skipped_manifest_count(self) -> int:
        return sum(
            1 for report in self.manifest_reports if report.action == "skip"
        )

    @property
    def data_files(self) -> list[SelectedFile]:
        return [f for f in self.files if f.file_type is FileType.DATA]

    @property
    def delete_files(self) -> list[SelectedFile]:
        return [f for f in self.files if f.file_type is not FileType.DATA]

    @property
    def is_empty(self) -> bool:
        return not self.files

    def csv_text(self) -> str:
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(EXPORT_COLUMNS)
        for item in self.files:
            writer.writerow(
                [
                    item.original_path,
                    item.suggested_upload_name,
                    item.file_type.value,
                    item.sequence_number,
                ]
            )
        return buffer.getvalue()


def parse_voyage_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.date()
    if isinstance(value, date):
        return value
    text = (value or "").strip()
    if not text:
        raise CheckError("请选择 UTC 航次日（YYYY-MM-DD）。")
    # Tolerate a timestamp string but always use its UTC date.
    if "T" in text or " " in text:
        candidate = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise CheckError(f"航次日 “{text}” 不是有效日期：{exc}") from None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.date()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise CheckError(
            f"航次日 “{text}” 不是有效日期，应为 YYYY-MM-DD。"
        ) from None


def _day_ordinal(day: date) -> int:
    return (day - date(1970, 1, 1)).days


def _normalise_transform(value: Any) -> str:
    return str(value or "").strip().lower()


def _schema_field_names(raw: dict[str, Any]) -> dict[int, str]:
    names: dict[int, str] = {}
    schemas = raw.get("schemas")
    if not isinstance(schemas, list):
        single = raw.get("schema")
        schemas = [single] if isinstance(single, dict) else []
    for schema in schemas:
        for field in schema.get("fields", []) if isinstance(schema, dict) else []:
            try:
                field_id = int(field["id"])
            except (KeyError, TypeError, ValueError):
                continue
            names.setdefault(field_id, str(field.get("name", "")))
    return names


def _parse_partition_specs(
    raw: dict[str, Any], schema_names: dict[int, str]
) -> tuple[dict[int, PartitionSpec], dict[int, str], int | None, str]:
    raw_specs = raw.get("partition-specs")
    if not isinstance(raw_specs, list):
        # Iceberg v1 metadata uses "partition-spec" (a bare field array).
        legacy = raw.get("partition-spec")
        if isinstance(legacy, list):
            raw_specs = [{"spec-id": 0, "fields": legacy}]
        else:
            raw_specs = []
    if not raw_specs:
        raise CheckError(
            "表 metadata.json 中没有分区定义；仅支持按发航时间列做 day/hour 单字段分区。"
        )

    specs: dict[int, PartitionSpec] = {}
    problems: dict[int, str] = {}
    source_names: set[str] = set()

    for spec_index, spec_raw in enumerate(raw_specs):
        if not isinstance(spec_raw, dict):
            raise CheckError("表 metadata.json 的分区定义格式无效。")
        try:
            spec_id = int(spec_raw.get("spec-id", spec_index))
        except (TypeError, ValueError):
            raise CheckError("分区定义中的 spec-id 不是整数。") from None
        fields_raw = spec_raw.get("fields")
        if not isinstance(fields_raw, list) or len(fields_raw) != 1:
            problems[spec_id] = (
                "仅支持单字段分区，该定义含有 "
                f"{len(fields_raw) if isinstance(fields_raw, list) else 0} 个字段"
            )
            continue
        field_raw = fields_raw[0]
        if not isinstance(field_raw, dict):
            problems[spec_id] = "分区字段格式无效"
            continue
        transform = _normalise_transform(field_raw.get("transform"))
        if transform not in ("day", "hour"):
            problems[spec_id] = (
                "仅支持 day 或 hour 分区，该定义的 transform 为 "
                f"“{field_raw.get('transform')}”"
            )
            continue
        try:
            source_id = int(field_raw.get("source-id"))
        except (TypeError, ValueError):
            source_id = None
        source_name = schema_names.get(source_id, "") if source_id is not None else ""
        if not source_name:
            source_name = str(field_raw.get("source-name", "")).strip()
        if not source_name:
            # A minimal metadata fixture may omit the schema; the source-id
            # still lets us enforce "one departure-time column".
            source_name = (
                f"source-id={source_id}"
                if source_id is not None
                else "未命名发航时间列"
            )
        name = str(field_raw.get("name") or "").strip()
        if not name:
            problems[spec_id] = "缺少分区字段名"
            continue
        source_names.add(source_name)
        specs[spec_id] = PartitionSpec(
            spec_id=spec_id,
            fields=[
                PartitionField(
                    name=name,
                    transform=transform,
                    source_id=source_id,
                    source_name=source_name,
                )
            ],
        )

    if not specs:
        details = "；".join(
            f"spec-id={spec_id}: {reason}"
            for spec_id, reason in sorted(problems.items())
        )
        raise CheckError(
            "表的分区定义不受支持；仅支持同一发航时间列按 day 或 hour "
            "单字段分区。"
            + (f"（{details}）" if details else "")
        )

    if len(source_names) > 1:
        raise CheckError(
            "仅支持同一发航时间列的分区；表的分区定义使用了不同源列："
            + "、".join(sorted(source_names))
        )

    try:
        default_spec_id = int(raw["default-spec-id"])
    except (KeyError, TypeError, ValueError):
        default_spec_id = None
    return specs, problems, default_spec_id, next(iter(source_names))


def _parse_snapshots(raw: dict[str, Any]) -> list[Snapshot]:
    snapshots: list[Snapshot] = []
    raw_snapshots = raw.get("snapshots")
    if not isinstance(raw_snapshots, list):
        return snapshots
    for item in raw_snapshots:
        if not isinstance(item, dict):
            continue
        try:
            snapshot_id = int(item.get("snapshot-id"))
        except (TypeError, ValueError):
            continue
        path = item.get("manifest-list") or item.get("manifest_list") or ""
        try:
            timestamp = int(item["timestamp-ms"])
        except (KeyError, TypeError, ValueError):
            timestamp = None
        snapshots.append(
            Snapshot(
                snapshot_id=snapshot_id,
                manifest_list=str(path),
                timestamp_ms=timestamp,
                raw=item,
            )
        )
    return snapshots


def _current_snapshot(raw: dict[str, Any], snapshots: list[Snapshot]) -> Snapshot | None:
    current_id = raw.get("current-snapshot-id")
    if current_id is None:
        runtime_refs = raw.get("refs")
        if isinstance(runtime_refs, dict):
            main = runtime_refs.get("main")
            if isinstance(main, dict):
                current_id = main.get("snapshot-id")
    if current_id is None:
        # A backup with a single snapshot is unambiguous.
        return snapshots[0] if len(snapshots) == 1 else None
    try:
        wanted = int(current_id)
    except (TypeError, ValueError):
        return None
    if wanted < 0:
        return None
    for snapshot in snapshots:
        if snapshot.snapshot_id == wanted:
            return snapshot
    return None


def load_table_metadata(data: bytes | str) -> TableMetadata:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CheckError(f"metadata.json 不是有效的 UTF-8 文本：{exc}") from None
    else:
        text = data
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CheckError(f"metadata.json 解析失败：{exc}") from None
    if not isinstance(raw, dict):
        raise CheckError("metadata.json 顶层必须是 JSON 对象。")
    try:
        format_version = int(raw.get("format-version"))
    except (TypeError, ValueError):
        raise CheckError("metadata.json 缺少有效的 format-version。") from None
    if format_version != 2:
        raise CheckError(
            f"仅支持 Iceberg v2；metadata.json 的 format-version 为 {format_version}。"
        )

    schema_names = _schema_field_names(raw)
    specs, spec_problems, default_spec_id, source_column = _parse_partition_specs(
        raw, schema_names
    )
    snapshots = _parse_snapshots(raw)
    current = _current_snapshot(raw, snapshots)
    if current is not None and not current.manifest_list:
        raise CheckError(
            f"快照 {current.snapshot_id} 没有 manifest-list 路径。"
        )
    return TableMetadata(
        raw=raw,
        format_version=format_version,
        specs=specs,
        default_spec_id=default_spec_id,
        snapshots=snapshots,
        current_snapshot=current,
        source_column=source_column,
        spec_problems=spec_problems,
    )


def _manifest_path_from_record(
    record: dict[str, Any], index: int, manifest_list_path: str
) -> str:
    value = record.get("manifest_path")
    if value is None or str(value).strip() == "":
        raise CheckError(
            f"manifest list 第 {index} 条缺少 manifest_path（{manifest_list_path}）。",
            files=[manifest_list_path],
        )
    return str(value)


def load_manifest_list(path: str | Path, label: str) -> list[ManifestListRecord]:
    try:
        avro = read_avro(path)
    except AvroError as exc:
        raise CheckError(
            f"无法读取 manifest list（{label}）：{exc}", files=[label]
        ) from None

    records: list[ManifestListRecord] = []
    for index, raw in enumerate(avro.records, start=1):
        manifest_path = _manifest_path_from_record(raw, index, label)
        spec_value = raw.get("partition_spec_id")
        if spec_value is None:
            spec_value = raw.get("partition-spec-id")
        try:
            spec_id = int(spec_value) if spec_value is not None else -1
        except (TypeError, ValueError):
            raise CheckError(
                f"manifest list 第 {index} 条的 partition_spec_id 不是整数"
                f"（{label}）。",
                files=[label],
            ) from None
        try:
            content = int(raw.get("content", 0))
        except (TypeError, ValueError):
            content = 0
        sequence_value = raw.get("sequence_number")
        if sequence_value is None:
            sequence_value = raw.get("data_sequence_number")
        try:
            sequence = (
                int(sequence_value) if sequence_value is not None else None
            )
        except (TypeError, ValueError):
            raise CheckError(
                f"manifest list 第 {index} 条的 sequence_number 不是整数"
                f"（{label}）。",
                files=[label],
            ) from None
        partitions = raw.get("partitions")
        if isinstance(partitions, dict):
            partitions = [partitions]
        elif partitions is not None and not isinstance(partitions, list):
            partitions = None
        records.append(
            ManifestListRecord(
                index=index,
                manifest_path=manifest_path,
                spec_id=spec_id,
                content=content,
                sequence_number=sequence,
                partitions=partitions,
                raw=raw,
            )
        )
    return records


def _decode_bound(value: Any, *, manifest_path: str) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean bound")
    if isinstance(value, int):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) == 0:
            raise ValueError("empty bound")
        if len(raw) in (4, 8):
            return int.from_bytes(raw, "little", signed=True)
        raise ValueError(f"unsupported bound length {len(raw)}")
    raise ValueError(f"unsupported bound value {type(value).__name__}")


def _spec_for(
    metadata: TableMetadata, record: ManifestListRecord
) -> PartitionSpec:
    spec_id = record.spec_id
    if spec_id < 0 and metadata.default_spec_id is not None:
        spec_id = metadata.default_spec_id
    spec = metadata.specs.get(spec_id)
    if spec is None:
        problem = metadata.spec_problems.get(spec_id)
        reason = f"：{problem}" if problem else ""
        raise CheckError(
            "manifest list 使用了不支持的分区定义 spec-id="
            f"{record.spec_id}{reason}，无法解释分区"
            f"（{record.manifest_path}）。",
            files=[record.manifest_path],
        )
    return spec


def _manifest_intersects(
    record: ManifestListRecord, spec: PartitionSpec, voyage_date: date
) -> tuple[bool, str, int | None, int | None]:
    """Return (must_read, reason, lower, upper) for a manifest-list entry."""
    transform = spec.field.transform
    summaries = record.partitions
    if not summaries:
        return True, "缺少分区上下界，读取 manifest", None, None

    summary = summaries[0]
    if not isinstance(summary, dict):
        return True, "分区统计格式无效，读取 manifest", None, None
    lower_raw = summary.get("lower_bound")
    upper_raw = summary.get("upper_bound")
    if lower_raw is None:
        lower_raw = summary.get("lower")
    if upper_raw is None:
        upper_raw = summary.get("upper")
    if lower_raw is None or upper_raw is None:
        return True, "分区上下界缺失，读取 manifest", None, None
    if isinstance(lower_raw, (bytes, bytearray, memoryview)) and len(lower_raw) == 0:
        return True, "分区下界为空，读取 manifest", None, None
    if isinstance(upper_raw, (bytes, bytearray, memoryview)) and len(upper_raw) == 0:
        return True, "分区上界为空，读取 manifest", None, None

    try:
        lower = _decode_bound(lower_raw, manifest_path=record.manifest_path)
        upper = _decode_bound(upper_raw, manifest_path=record.manifest_path)
    except ValueError as exc:
        return True, f"分区上下界无法解释（{exc}），读取 manifest", None, None

    if lower > upper:
        return True, "分区上下界倒置，读取 manifest", lower, upper

    day = _day_ordinal(voyage_date)
    if transform == "day":
        intersects = not (lower > day or upper < day)
        relation = (
            f"day 分区 [{lower}, {upper}] "
            + ("与航次日相交" if intersects else "不含航次日")
        )
        return intersects, relation, lower, upper

    start_hour = day * 24
    end_hour = start_hour + 23
    intersects = not (lower > end_hour or upper < start_hour)
    relation = (
        f"hour 分区 [{lower}, {upper}] "
        + ("与航次日相交" if intersects else "不含航次日")
    )
    return intersects, relation, lower, upper


def _basename(path: str) -> str:
    text = str(path or "").strip().split("?", 1)[0]
    name = posixpath.basename(text.replace("\\", "/"))
    return name


def _candidate_names(path: str) -> list[str]:
    name = _basename(path)
    if not name:
        return []
    candidates = [name]
    try:
        decoded = unquote(name)
    except Exception:
        decoded = name
    if decoded != name:
        candidates.append(decoded)
    return candidates


def _find_manifest_file(
    manifest_path: str, manifest_files: Mapping[str, Path]
) -> tuple[str, Path] | None:
    for candidate in _candidate_names(manifest_path):
        if candidate in manifest_files:
            return candidate, manifest_files[candidate]
    # As a last resort allow a full-path key (some callers keep paths).
    if manifest_path in manifest_files:
        return manifest_path, manifest_files[manifest_path]
    return None


def _content_to_type(value: Any) -> FileType:
    if isinstance(value, str):
        text = re.sub(r"[^a-z0-9]+", "", value.strip().lower())
        if text in ("data", "0"):
            return FileType.DATA
        if text in ("positiondelete", "positionaldeletes", "posdelete", "1"):
            return FileType.POSITION_DELETE
        if text in ("equalitydelete", "eqdelete", "2"):
            return FileType.EQUALITY_DELETE
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise CheckError(f"manifest 条目的 data_file.content 无法识别：{value!r}。")
    if number == _CONTENT_DATA:
        return FileType.DATA
    if number == _CONTENT_POSITION_DELETE:
        return FileType.POSITION_DELETE
    if number == _CONTENT_EQUALITY_DELETE:
        return FileType.EQUALITY_DELETE
    raise CheckError(f"manifest 条目的 data_file.content 无效：{number}。")


def _status_value(value: Any, manifest_path: str) -> int:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _STATUS_NAMES:
            return _STATUS_NAMES[text]
        if text.lstrip("+-").isdigit():
            value = int(text)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CheckError(
            f"manifest 条目的 status 无效（{manifest_path}）。",
            files=[manifest_path],
        )
    if value not in (_STATUS_EXISTING, _STATUS_ADDED, _STATUS_DELETED):
        raise CheckError(
            f"manifest 条目的 status={value} 无效（{manifest_path}）。",
            files=[manifest_path],
        )
    return value


def _entry_sequence(
    entry: dict[str, Any],
    status: int,
    manifest: ManifestListRecord,
) -> tuple[int, str]:
    for key in ("sequence_number", "data_sequence_number", "data_seq"):
        value = entry.get(key)
        if value is not None:
            try:
                return int(value), "entry"
            except (TypeError, ValueError):
                raise CheckError(
                    "manifest 条目的数据序列号不是整数"
                    f"（{manifest.manifest_path}）。",
                    files=[manifest.manifest_path],
                ) from None
    if status == _STATUS_ADDED and manifest.sequence_number is not None:
        return manifest.sequence_number, "manifest"
    raise CheckError(
        "无法确定条目的数据序列号：条目既没有 sequence_number，"
        "manifest list 也没有可继承的 sequence_number"
        f"（{manifest.manifest_path}）。",
        files=[manifest.manifest_path],
    )


def _entry_partition_value(
    entry: dict[str, Any],
    spec: PartitionSpec,
    manifest_path: str,
) -> int | None:
    data_file = entry.get("data_file")
    if not isinstance(data_file, dict):
        raise CheckError(
            f"manifest 条目缺少 data_file（{manifest_path}）。",
            files=[manifest_path],
        )
    partition = data_file.get("partition")
    if not isinstance(partition, dict):
        raise CheckError(
            "manifest 条目的 data_file.partition 缺失或格式无效"
            f"（{manifest_path}）。",
            files=[manifest_path],
        )
    field = spec.field
    if field.name in partition:
        value = partition[field.name]
    elif len(partition) == 1:
        value = next(iter(partition.values()))
    else:
        raise CheckError(
            f"manifest 条目缺少分区字段 “{field.name}” 的值"
            f"（{manifest_path}）。",
            files=[manifest_path],
        )
    if value is None:
        # A null partition value cannot be part of any day/hour partition;
        # such an entry simply does not match the selected voyage day.
        return None
    if isinstance(value, bool):
        raise CheckError(
            f"manifest 条目的分区值不是整数（{manifest_path}）。",
            files=[manifest_path],
        )
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        return int(value.strip(), 10)
    raise CheckError(
        f"manifest 条目的分区值不是整数（{manifest_path}）。",
        files=[manifest_path],
    )


def _partition_matches(value: int, transform: str, voyage_date: date) -> bool:
    day = _day_ordinal(voyage_date)
    if transform == "day":
        return value == day
    start_hour = day * 24
    return start_hour <= value <= start_hour + 23


def _unique_upload_names(files: list[SelectedFile]) -> None:
    used: dict[str, int] = {}
    for item in files:
        name = _basename(item.original_path)
        if not name:
            raise CheckError(
                f"无法从原始路径取得文件名：{item.original_path}",
                files=[item.manifest_path],
            )
        base = name
        count = used.get(base.lower(), 0) + 1
        used[base.lower()] = count
        if count > 1:
            stem, dot, suffix = base.rpartition(".")
            if dot and stem:
                name = f"{stem}-{count}.{suffix}"
            else:
                name = f"{base}-{count}"
        item.suggested_upload_name = name


def recover_voyage_day(
    metadata: TableMetadata,
    manifest_list_path: Path,
    manifest_list_label: str,
    manifest_files: Mapping[str, Path],
    voyage_date: date,
) -> RecoverResult:
    """Build the selected-day file list.  Raises ``CheckError`` on any gap."""
    manifest_records = load_manifest_list(manifest_list_path, manifest_list_label)
    reports: list[ManifestReport] = []
    selected: list[SelectedFile] = []
    warnings: list[str] = []
    order = 0

    for record in manifest_records:
        spec = _spec_for(metadata, record)
        must_read, reason, lower, upper = _manifest_intersects(
            record, spec, voyage_date
        )
        report = ManifestReport(
            index=record.index,
            manifest_path=record.manifest_path,
            spec_id=spec.spec_id,
            transform=spec.field.transform,
            action="read" if must_read else "skip",
            reason=reason,
            lower=lower,
            upper=upper,
        )
        reports.append(report)
        if not must_read:
            continue

        found = _find_manifest_file(record.manifest_path, manifest_files)
        if found is None:
            raise CheckError(
                "需要读取的 manifest 副本缺失："
                f"{record.manifest_path}（上传文件名应为 "
                f"{_basename(record.manifest_path) or '未知'}）。",
                files=[record.manifest_path],
            )
        uploaded_name, local_path = found
        try:
            avro = read_avro(local_path)
        except AvroError as exc:
            raise CheckError(
                f"需要读取的 manifest 损坏：{record.manifest_path}"
                f"（上传副本 {uploaded_name}）：{exc}",
                files=[record.manifest_path],
            ) from None

        for entry_index, entry in enumerate(avro.records, start=1):
            if not isinstance(entry, dict):
                raise CheckError(
                    f"manifest 第 {entry_index} 条不是对象"
                    f"（{record.manifest_path}）。",
                    files=[record.manifest_path],
                )
            status_raw = entry.get("status")
            if status_raw is None:
                raise CheckError(
                    f"manifest 第 {entry_index} 条缺少 status"
                    f"（{record.manifest_path}）。",
                    files=[record.manifest_path],
                )
            status = _status_value(status_raw, record.manifest_path)
            if status == _STATUS_DELETED:
                continue

            data_file = entry.get("data_file")
            if not isinstance(data_file, dict):
                raise CheckError(
                    f"manifest 第 {entry_index} 条缺少 data_file"
                    f"（{record.manifest_path}）。",
                    files=[record.manifest_path],
                )
            file_path = data_file.get("file_path")
            if file_path is None or str(file_path).strip() == "":
                raise CheckError(
                    f"manifest 第 {entry_index} 条的 file_path 为空"
                    f"（{record.manifest_path}）。",
                    files=[record.manifest_path],
                )
            file_path = str(file_path)

            raw_format = data_file.get("file_format")
            file_format = str(raw_format).strip().upper() if raw_format else ""
            if file_format and file_format != "PARQUET":
                warnings.append(
                    f"已忽略非 Parquet 文件 {file_path}（格式 "
                    f"{file_format}，manifest {record.manifest_path}）。"
                )
                continue

            raw_content = data_file.get("content")
            if raw_content is None:
                # Old/partial manifests may omit the per-file content; the
                # manifest list's data/delete content is the best fallback.
                raw_content = (
                    _CONTENT_DATA if record.content == 0 else _CONTENT_POSITION_DELETE
                )
            try:
                file_type = _content_to_type(raw_content)
            except CheckError as exc:
                raise CheckError(
                    f"{exc.message}（{record.manifest_path}）",
                    files=[record.manifest_path],
                ) from None

            partition_value = _entry_partition_value(
                entry, spec, record.manifest_path
            )
            if partition_value is None:
                continue
            if not _partition_matches(
                partition_value, spec.field.transform, voyage_date
            ):
                continue

            sequence, sequence_source = _entry_sequence(
                entry, status, record
            )
            order += 1
            selected.append(
                SelectedFile(
                    original_path=file_path,
                    suggested_upload_name="",
                    file_type=file_type,
                    sequence_number=sequence,
                    status=status,
                    manifest_path=record.manifest_path,
                    partition_value=partition_value,
                    partition_spec_id=spec.spec_id,
                    sequence_source=sequence_source,
                    file_format=file_format or "PARQUET",
                    order=order,
                )
            )
            report.selected_entries += 1

    # A snapshot should reference each live file once.  Identical duplicates
    # are collapsed; conflicting duplicates are treated as a corrupt input.
    deduped: list[SelectedFile] = []
    by_path: dict[str, SelectedFile] = {}
    for item in selected:
        prior = by_path.get(item.original_path)
        if prior is None:
            by_path[item.original_path] = item
            deduped.append(item)
            continue
        if (
            prior.file_type is not item.file_type
            or prior.sequence_number != item.sequence_number
        ):
            raise CheckError(
                f"同一原始路径在需要读取的 manifest 中出现冲突条目："
                f"{item.original_path}（{prior.manifest_path} 与 "
                f"{item.manifest_path}）。",
                files=[prior.manifest_path, item.manifest_path],
            )
    _unique_upload_names(deduped)

    return RecoverResult(
        voyage_date=voyage_date,
        manifest_list_path=manifest_list_label,
        files=deduped,
        manifest_reports=reports,
        metadata=metadata,
        warnings=warnings,
    )
