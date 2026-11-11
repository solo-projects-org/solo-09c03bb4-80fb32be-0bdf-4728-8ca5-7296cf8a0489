"""Generate the bundled Iceberg v2 recovery sample.

The sample intentionally contains both a ``day`` partition spec and an
``hour`` partition spec on the same ``voyage_time`` column, so the web page's
"载入样例" button demonstrates both interpretations side by side.

Layout:
  metadata.json .............. two snapshots? no, one current snapshot
  snap-1001.avro ............. manifest list
  manifest-day-data-1001.avro      day spec, days 2026-11-04..06
  manifest-day-delete-1001.avro    day spec, 2026-11-05
  manifest-hour-data-1001.avro     hour spec, hours 6..8 on 2026-11-05
  manifest-hour-delete-1001.avro   hour spec, hour 10 on 2026-11-05
  manifest-day-other-1001.avro     day spec, another date (must be skipped)
  manifest-hour-other-1001.avro    hour spec, another date (must be skipped)

The selected day is 2026-11-05 (UTC).  The generated CSV can be fed to the
verification page; the referenced file basenames match the bundled Parquet
files where available.
"""

from __future__ import annotations

import json
import struct
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from checker.avro import write_avro  # noqa: E402

SAMPLE = ROOT / "sample"

SNAPSHOT_ID = 1001
TABLE_LOCATION = "s3://ferry-lake/warehouse/settlement"
BASE = f"{TABLE_LOCATION}/voyage_date=2026-11-05"

DATA1_PATH = f"{BASE}/data/data-0001.parquet"
DATA2_PATH = f"{BASE}/data/data-0002.parquet"
DELETE1_PATH = f"{BASE}/delete-pos/delete-pos-0001.parquet"
DELETE2_PATH = f"{BASE}/delete-pos/delete-pos-0002.parquet"
STALE_DELETE_PATH = f"{BASE}/delete-pos/delete-pos-stale-0001.parquet"
EQ_DELETE_PATH = f"{BASE}/delete-eq/delete-eq-0001.parquet"

OTHER_DATA_PATH = (
    f"{TABLE_LOCATION}/voyage_date=2026-11-08/data/data-other-0001.parquet"
)
OTHER_DELETE_PATH = (
    f"{TABLE_LOCATION}/voyage_date=2026-11-08/delete-pos/"
    "delete-other-0001.parquet"
)

# Iceberg date and hour transforms both produce int values, encoded as
# little-endian bound bytes in the manifest list.
EPOCH = date(1970, 1, 1)


def day_value(value: date) -> int:
    return (value - EPOCH).days


def hour_value(value: date, hour: int) -> int:
    return day_value(value) * 24 + hour


def bound_int(value: int) -> bytes:
    return struct.pack("<i", value)


MANIFEST_LIST_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "namespace": "org.apache.iceberg",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "content", "type": "int"},
        {"name": "sequence_number", "type": "long"},
        {"name": "min_sequence_number", "type": "long"},
        {"name": "added_snapshot_id", "type": "long"},
        {"name": "added_files_count", "type": ["null", "int"], "default": None},
        {"name": "existing_files_count", "type": ["null", "int"], "default": None},
        {"name": "deleted_files_count", "type": ["null", "int"], "default": None},
        {"name": "added_rows_count", "type": ["null", "long"], "default": None},
        {"name": "existing_rows_count", "type": ["null", "long"], "default": None},
        {"name": "deleted_rows_count", "type": ["null", "long"], "default": None},
        {
            "name": "partitions",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "field_summary",
                        "fields": [
                            {"name": "contains_null", "type": "boolean"},
                            {
                                "name": "contains_nan",
                                "type": ["null", "boolean"],
                                "default": None,
                            },
                            {
                                "name": "lower_bound",
                                "type": ["null", "bytes"],
                                "default": None,
                            },
                            {
                                "name": "upper_bound",
                                "type": ["null", "bytes"],
                                "default": None,
                            },
                        ],
                    },
                },
            ],
            "default": None,
        },
        {"name": "key_metadata", "type": ["null", "bytes"], "default": None},
    ],
}


def manifest_schema(partition_field: str) -> dict[str, Any]:
    return {
        "type": "record",
        "name": "manifest_entry",
        "namespace": "org.apache.iceberg",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": ["null", "long"], "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "default": None},
            {
                "name": "file_sequence_number",
                "type": ["null", "long"],
                "default": None,
            },
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "partition",
                                "fields": [
                                    {
                                        "name": partition_field,
                                        "type": ["null", "int"],
                                        "default": None,
                                    }
                                ],
                            },
                        },
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {
                            "name": "column_sizes",
                            "type": [
                                "null",
                                {"type": "map", "values": "long"},
                            ],
                            "default": None,
                        },
                        {
                            "name": "value_counts",
                            "type": [
                                "null",
                                {"type": "map", "values": "long"},
                            ],
                            "default": None,
                        },
                        {
                            "name": "null_value_counts",
                            "type": [
                                "null",
                                {"type": "map", "values": "long"},
                            ],
                            "default": None,
                        },
                        {
                            "name": "lower_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "map",
                                    "values": "bytes",
                                },
                            ],
                            "default": None,
                        },
                        {
                            "name": "upper_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "map",
                                    "values": "bytes",
                                },
                            ],
                            "default": None,
                        },
                    ],
                },
            },
        ],
    }


def data_entry(
    status: int,
    path: str,
    partition_field: str,
    partition_value: int,
    *,
    content: int = 0,
    sequence_number: int | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "snapshot_id": SNAPSHOT_ID,
        "sequence_number": sequence_number,
        "file_sequence_number": sequence_number,
        "data_file": {
            "content": content,
            "file_path": path,
            "file_format": "PARQUET",
            "partition": {partition_field: partition_value},
            "record_count": 10,
            "file_size_in_bytes": 1024,
            "column_sizes": None,
            "value_counts": None,
            "null_value_counts": None,
            "lower_bounds": None,
            "upper_bounds": None,
        },
    }


def write_manifest(path: Path, partition_field: str, entries: list[dict[str, Any]]) -> None:
    write_avro(path, manifest_schema(partition_field), entries)


def manifest_record(
    *,
    path: str,
    spec_id: int,
    content: int,
    sequence_number: int,
    lower: int,
    upper: int,
) -> dict[str, Any]:
    return {
        "manifest_path": path,
        "manifest_length": 2048,
        "partition_spec_id": spec_id,
        "content": content,
        "sequence_number": sequence_number,
        "min_sequence_number": sequence_number,
        "added_snapshot_id": SNAPSHOT_ID,
        "added_files_count": 1,
        "existing_files_count": 0,
        "deleted_files_count": 0,
        "added_rows_count": 10,
        "existing_rows_count": 0,
        "deleted_rows_count": 0,
        "partitions": [
            {
                "contains_null": False,
                "contains_nan": None,
                "lower_bound": bound_int(lower),
                "upper_bound": bound_int(upper),
            }
        ],
        "key_metadata": None,
    }


def metadata_json(manifest_list_path: str) -> dict[str, Any]:
    return {
        "format-version": 2,
        "table-uuid": "f0c3a8a9-6b16-4c7a-9a32-ferry000001",
        "location": TABLE_LOCATION,
        "last-updated-ms": 1762300000000,
        "last-column-id": 2,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "voyage_time",
                        "required": True,
                        "type": "timestamptz",
                    },
                    {
                        "id": 2,
                        "name": "ticket_no",
                        "required": False,
                        "type": "long",
                    },
                ],
            }
        ],
        "current-schema-id": 0,
        "partition-specs": [
            {
                "spec-id": 0,
                "fields": [
                    {
                        "name": "voyage_day",
                        "transform": "day",
                        "source-id": 1,
                        "field-id": 1000,
                    }
                ],
            },
            {
                "spec-id": 1,
                "fields": [
                    {
                        "name": "voyage_hour",
                        "transform": "hour",
                        "source-id": 1,
                        "field-id": 1001,
                    }
                ],
            },
        ],
        "default-spec-id": 1,
        "last-partition-id": 1001,
        "properties": {"write.format.default": "parquet"},
        "current-snapshot-id": SNAPSHOT_ID,
        "snapshots": [
            {
                "snapshot-id": SNAPSHOT_ID,
                "timestamp-ms": 1762300000000,
                "manifest-list": manifest_list_path,
                "summary": {
                    "operation": "append",
                    "added-data-files": "4",
                    "added-delete-files": "2",
                },
                "schema-id": 0,
            }
        ],
        "snapshot-log": [
            {"timestamp-ms": 1762300000000, "snapshot-id": SNAPSHOT_ID}
        ],
        "metadata-log": [],
    }


def main() -> None:
    SAMPLE.mkdir(parents=True, exist_ok=True)
    selected_day = date(2026, 11, 5)
    other_day = date(2026, 11, 8)

    day_data_path = f"{TABLE_LOCATION}/metadata/manifest-day-data-1001.avro"
    day_delete_path = f"{TABLE_LOCATION}/metadata/manifest-day-delete-1001.avro"
    hour_data_path = f"{TABLE_LOCATION}/metadata/manifest-hour-data-1001.avro"
    hour_delete_path = f"{TABLE_LOCATION}/metadata/manifest-hour-delete-1001.avro"
    day_other_path = f"{TABLE_LOCATION}/metadata/manifest-day-other-1001.avro"
    hour_other_path = f"{TABLE_LOCATION}/metadata/manifest-hour-other-1001.avro"
    list_path = f"{TABLE_LOCATION}/metadata/snap-1001.avro"

    # Day spec: bounds cover 2026-11-04..06 so the manifest must be read for
    # 11-05, while individual entries are filtered by their partition value.
    day_value_04 = day_value(date(2026, 11, 4))
    day_value_05 = day_value(selected_day)
    day_value_06 = day_value(date(2026, 11, 6))
    write_manifest(
        SAMPLE / "manifest-day-data-1001.avro",
        "voyage_day",
        [
            # ADDED without an entry sequence number -> inherits manifest seq 7.
            data_entry(
                1, DATA1_PATH, "voyage_day", day_value_05, sequence_number=None
            ),
            # A DELETED entry for the selected day must never be exported.
            data_entry(
                2, OTHER_DELETE_PATH, "voyage_day", day_value_05, sequence_number=5
            ),
            # Entries for other days in an intersecting manifest are filtered.
            data_entry(
                1, OTHER_DATA_PATH, "voyage_day", day_value_04, sequence_number=9
            ),
        ],
    )
    write_manifest(
        SAMPLE / "manifest-day-delete-1001.avro",
        "voyage_day",
        [
            data_entry(
                1,
                DELETE1_PATH,
                "voyage_day",
                day_value_05,
                content=1,
                sequence_number=None,
            ),
            data_entry(
                1,
                STALE_DELETE_PATH,
                "voyage_day",
                day_value_05,
                content=1,
                sequence_number=6,
            ),
            data_entry(
                1,
                EQ_DELETE_PATH,
                "voyage_day",
                day_value_05,
                content=2,
                sequence_number=12,
            ),
        ],
    )

    # Hour spec: a narrow hour range on the selected day.
    first_hour = hour_value(selected_day, 6)
    last_hour = hour_value(selected_day, 8)
    write_manifest(
        SAMPLE / "manifest-hour-data-1001.avro",
        "voyage_hour",
        [
            # EXISTING keeps its own data sequence number (7).
            data_entry(
                0,
                DATA2_PATH,
                "voyage_hour",
                hour_value(selected_day, 7),
                sequence_number=7,
            ),
            data_entry(
                1,
                f"{TABLE_LOCATION}/voyage_date=2026-11-06/data/data-hour-other.parquet",
                "voyage_hour",
                hour_value(date(2026, 11, 6), 7),
                sequence_number=9,
            ),
        ],
    )
    write_manifest(
        SAMPLE / "manifest-hour-delete-1001.avro",
        "voyage_hour",
        [
            # ADDED without a sequence -> inherits the manifest-list seq (8).
            data_entry(
                1,
                DELETE2_PATH,
                "voyage_hour",
                hour_value(selected_day, 10),
                content=1,
                sequence_number=None,
            )
        ],
    )

    # Manifests that are provably disjoint from 2026-11-05 must be skipped.
    write_manifest(
        SAMPLE / "manifest-day-other-1001.avro",
        "voyage_day",
        [
            data_entry(
                1,
                OTHER_DATA_PATH,
                "voyage_day",
                day_value(other_day),
                sequence_number=9,
            )
        ],
    )
    write_manifest(
        SAMPLE / "manifest-hour-other-1001.avro",
        "voyage_hour",
        [
            data_entry(
                1,
                f"{TABLE_LOCATION}/voyage_date=2026-11-08/data/"
                "data-hour-other-0001.parquet",
                "voyage_hour",
                hour_value(other_day, 4),
                sequence_number=9,
            )
        ],
    )

    list_records = [
        manifest_record(
            path=day_data_path,
            spec_id=0,
            content=0,
            sequence_number=7,
            lower=day_value_04,
            upper=day_value_06,
        ),
        manifest_record(
            path=day_delete_path,
            spec_id=0,
            content=1,
            sequence_number=12,
            lower=day_value_05,
            upper=day_value_05,
        ),
        manifest_record(
            path=hour_data_path,
            spec_id=1,
            content=0,
            sequence_number=8,
            lower=first_hour,
            upper=last_hour,
        ),
        manifest_record(
            path=hour_delete_path,
            spec_id=1,
            content=1,
            sequence_number=8,
            lower=hour_value(selected_day, 10),
            upper=hour_value(selected_day, 10),
        ),
        manifest_record(
            path=day_other_path,
            spec_id=0,
            content=0,
            sequence_number=9,
            lower=day_value(other_day),
            upper=day_value(other_day),
        ),
        manifest_record(
            path=hour_other_path,
            spec_id=1,
            content=0,
            sequence_number=9,
            lower=hour_value(other_day, 0),
            upper=hour_value(other_day, 23),
        ),
    ]
    write_avro(SAMPLE / "snap-1001.avro", MANIFEST_LIST_SCHEMA, list_records)
    (SAMPLE / "metadata.json").write_text(
        json.dumps(metadata_json(list_path), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"wrote sample metadata and Avro files to {SAMPLE}")


if __name__ == "__main__":
    main()
