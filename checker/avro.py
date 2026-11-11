"""Small, dependency-free Avro object-container-file reader/writer.

Only the parts needed by Iceberg metadata are implemented: records, arrays,
maps, unions, enums, fixed values and the usual primitive types.  The reader
returns *raw* Avro values (logical types are deliberately not converted) so
that Iceberg partition values stay ordinary ``int`` values.

Supported codecs: ``null``, ``deflate``, ``snappy``, ``bzip2`` and ``xz``.
The writer writes ``null`` or ``deflate`` blocks.
"""

from __future__ import annotations

import bz2
import io
import json
import lzma
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

MAGIC = b"Obj\x01"

_PRIMITIVES = {
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "string",
}


class AvroError(Exception):
    """Raised when an Avro file cannot be decoded."""


@dataclass
class AvroFile:
    schema: Any
    records: list[dict[str, Any]]
    metadata: dict[str, bytes]
    codec: str


class _Decoder:
    def __init__(self, data: bytes, registry: dict[str, Any]):
        self.data = data
        self.pos = 0
        self.registry = registry

    # -- primitive helpers -------------------------------------------------
    def _read(self, size: int) -> bytes:
        end = self.pos + size
        if end > len(self.data):
            raise AvroError("Avro 数据被截断。")
        chunk = self.data[self.pos:end]
        self.pos = end
        return chunk

    def _read_long(self) -> int:
        raw = 0
        shift = 0
        while True:
            if self.pos >= len(self.data):
                raise AvroError("Avro varint 被截断。")
            byte = self.data[self.pos]
            self.pos += 1
            raw |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
            if shift > 70:
                raise AvroError("Avro varint 过长。")
        # Zig-zag decoding.
        return (raw >> 1) ^ -(raw & 1)

    # -- schema resolution ------------------------------------------------
    def _full_name(self, schema: dict[str, Any], namespace: str) -> str:
        name = str(schema.get("name", ""))
        own_ns = schema.get("namespace", namespace)
        if name and own_ns and "." not in name:
            return f"{own_ns}.{name}"
        return name

    def _resolve(self, name: str, namespace: str) -> Any:
        for candidate in (
            name,
            f"{namespace}.{name}" if namespace else "",
        ):
            if candidate and candidate in self.registry:
                return self.registry[candidate]
        # Fall back to a unique short-name match.
        matches = [
            schema
            for fullname, schema in self.registry.items()
            if fullname.rsplit(".", 1)[-1] == name
        ]
        if len(matches) == 1:
            return matches[0]
        raise AvroError(f"Avro 类型引用无法解析：{name}")

    def decode(self, schema: Any, namespace: str = "") -> Any:
        if isinstance(schema, str):
            if schema in _PRIMITIVES:
                return self._decode_primitive(schema)
            return self.decode(self._resolve(schema, namespace), namespace)

        if isinstance(schema, list):
            index = self._read_long()
            if index < 0 or index >= len(schema):
                raise AvroError(f"Avro union 索引越界：{index}")
            return self.decode(schema[index], namespace)

        if isinstance(schema, dict):
            kind = schema.get("type")
            if kind in _PRIMITIVES:
                return self._decode_primitive(kind)
            if kind == "array":
                return self._decode_array(schema["items"], namespace)
            if kind == "map":
                return self._decode_map(schema["values"], namespace)
            if kind == "enum":
                index = self._read_long()
                symbols = schema.get("symbols", [])
                if index < 0 or index >= len(symbols):
                    raise AvroError(f"Avro enum 索引越界：{index}")
                return symbols[index]
            if kind == "fixed":
                size = int(schema.get("size", 0))
                return self._read(size)
            if kind == "record":
                fullname = self._full_name(schema, namespace)
                own_ns = schema.get("namespace", namespace)
                if "." in fullname and not schema.get("namespace"):
                    own_ns = fullname.rsplit(".", 1)[0]
                return self._decode_record(schema, own_ns)
            # A dict whose "type" is itself a schema (rare but legal).
            return self.decode(kind, namespace)

        raise AvroError(f"无法识别的 Avro schema：{schema!r}")

    def _decode_primitive(self, kind: str) -> Any:
        if kind == "null":
            return None
        if kind == "boolean":
            value = self._read(1)[0]
            if value not in (0, 1):
                raise AvroError(f"Avro boolean 值无效：{value}")
            return bool(value)
        if kind in ("int", "long"):
            return self._read_long()
        if kind == "float":
            return struct.unpack("<f", self._read(4))[0]
        if kind == "double":
            return struct.unpack("<d", self._read(8))[0]
        if kind == "bytes":
            length = self._read_long()
            if length < 0:
                raise AvroError("Avro bytes 长度为负数。")
            return self._read(length)
        if kind == "string":
            length = self._read_long()
            if length < 0:
                raise AvroError("Avro string 长度为负数。")
            try:
                return self._read(length).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise AvroError(f"Avro string 不是 UTF-8：{exc}") from None
        raise AvroError(f"未知 Avro 原始类型：{kind}")

    def _decode_array(self, item_schema: Any, namespace: str) -> list[Any]:
        values: list[Any] = []
        while True:
            count = self._read_long()
            if count == 0:
                return values
            if count < 0:
                count = -count
                # Block byte size is informational; skipping it is safe.
                self._read_long()
            for _ in range(count):
                values.append(self.decode(item_schema, namespace))

    def _decode_map(self, value_schema: Any, namespace: str) -> dict[str, Any]:
        values: dict[str, Any] = {}
        while True:
            count = self._read_long()
            if count == 0:
                return values
            if count < 0:
                count = -count
                self._read_long()
            for _ in range(count):
                key = self.decode("string", namespace)
                values[key] = self.decode(value_schema, namespace)

    def _decode_record(self, schema: dict[str, Any], namespace: str) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for field in schema.get("fields", []):
            record[field["name"]] = self.decode(field["type"], namespace)
        return record


def _collect_named(schema: Any, registry: dict[str, Any], namespace: str = "") -> None:
    if isinstance(schema, list):
        for branch in schema:
            _collect_named(branch, registry, namespace)
        return
    if not isinstance(schema, dict):
        return

    kind = schema.get("type")
    if kind in ("record", "enum", "fixed"):
        name = str(schema.get("name", ""))
        own_ns = schema.get("namespace", namespace)
        fullname = f"{own_ns}.{name}" if own_ns and "." not in name else name
        if fullname:
            registry[fullname] = schema
            if name and name not in registry:
                registry[name] = schema
        child_ns = own_ns
        if kind == "record":
            if "." in fullname and not schema.get("namespace"):
                child_ns = fullname.rsplit(".", 1)[0]
            for field in schema.get("fields", []):
                _collect_named(field["type"], registry, child_ns)
        return

    if kind == "array":
        _collect_named(schema.get("items"), registry, namespace)
    elif kind == "map":
        _collect_named(schema.get("values"), registry, namespace)
    elif isinstance(kind, (dict, list)):
        _collect_named(kind, registry, namespace)


def _decode_avro_container(data: bytes) -> AvroFile:
    if not data.startswith(MAGIC):
        raise AvroError("不是 Avro object container 文件（缺少 Obj\\x01 魔数）。")
    decoder = _Decoder(data, {})
    decoder.pos = len(MAGIC)
    metadata = decoder.decode({"type": "map", "values": "bytes"})
    schema_raw = metadata.get("avro.schema")
    if schema_raw is None:
        raise AvroError("Avro 文件缺少 avro.schema。")
    try:
        schema = json.loads(schema_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AvroError(f"Avro schema 不是有效 JSON：{exc}") from None

    codec = metadata.get("avro.codec", b"null").decode("utf-8", "replace")
    sync = decoder._read(16)

    registry: dict[str, Any] = {}
    _collect_named(schema, registry)
    records: list[dict[str, Any]] = []

    while decoder.pos < len(data):
        # A trailing byte here means a truncated/foreign file.
        if len(data) - decoder.pos < 2:
            raise AvroError("Avro 文件在数据块之前被截断。")
        count = decoder._read_long()
        if count < 0:
            raise AvroError("Avro 数据块记录数为负数。")
        block_size = decoder._read_long()
        if block_size < 0:
            raise AvroError("Avro 数据块大小为负数。")
        block = decoder._read(block_size)
        block_sync = decoder._read(16)
        if block_sync != sync:
            raise AvroError("Avro 数据块同步标记不匹配，文件已损坏。")

        plain = _decompress_block(block, codec)
        block_decoder = _Decoder(plain, registry)
        for _ in range(count):
            value = block_decoder.decode(schema)
            if not isinstance(value, dict):
                raise AvroError("Avro 记录的顶层类型不是 record。")
            records.append(value)
        if block_decoder.pos != len(plain):
            raise AvroError(
                "Avro 数据块解码后仍有剩余字节，文件可能已损坏。"
            )

    return AvroFile(
        schema=schema,
        records=records,
        metadata=metadata,
        codec=codec,
    )


def _snappy_decompress(data: bytes) -> bytes:
    """Pure-Python decoder for the raw Snappy block format."""
    pos = 0
    length = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise AvroError("Snappy 数据被截断。")
        byte = data[pos]
        pos += 1
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
        if shift > 35:
            raise AvroError("Snappy 长度前缀过长。")
    output = bytearray()
    while pos < len(data):
        tag = data[pos]
        pos += 1
        kind = tag & 0x03
        if kind == 0:
            literal_len = tag >> 2
            if literal_len < 60:
                literal_len += 1
            else:
                extra = literal_len - 59
                if extra > 4 or pos + extra > len(data):
                    raise AvroError("Snappy literal 长度无效。")
                literal_len = int.from_bytes(
                    data[pos:pos + extra], "little"
                ) + 1
                pos += extra
            if pos + literal_len > len(data):
                raise AvroError("Snappy literal 被截断。")
            output.extend(data[pos:pos + literal_len])
            pos += literal_len
            continue

        if kind == 1:
            if pos >= len(data):
                raise AvroError("Snappy copy 被截断。")
            copy_len = ((tag >> 2) & 0x07) + 4
            offset = ((tag >> 5) << 8) | data[pos]
            pos += 1
        elif kind == 2:
            if pos + 2 > len(data):
                raise AvroError("Snappy copy 被截断。")
            copy_len = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + 2], "little")
            pos += 2
        else:
            if pos + 4 > len(data):
                raise AvroError("Snappy copy 被截断。")
            copy_len = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + 4], "little")
            pos += 4

        if offset <= 0 or offset > len(output):
            raise AvroError("Snappy copy 偏移量无效。")
        for _ in range(copy_len):
            output.append(output[-offset])

    if len(output) != length:
        raise AvroError(
            f"Snappy 解压长度不符：期望 {length}，得到 {len(output)}。"
        )
    return bytes(output)


def _decompress_block(block: bytes, codec: str) -> bytes:
    codec = (codec or "null").lower()
    if codec in ("null", "", "none"):
        return block
    if codec == "deflate":
        try:
            return zlib.decompress(block, -15)
        except zlib.error as exc:
            raise AvroError(f"Avro deflate 解压失败：{exc}") from None
    if codec == "snappy":
        if len(block) < 4:
            raise AvroError("Avro snappy 数据块过短。")
        body = block[:-4]
        try:
            plain = _snappy_decompress(body)
        except AvroError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            raise AvroError(f"Avro snappy 解压失败：{exc}") from None
        expected = int.from_bytes(block[-4:], "big")
        actual = zlib.crc32(plain) & 0xFFFFFFFF
        if expected != actual:
            raise AvroError("Avro snappy CRC32 校验失败。")
        return plain
    if codec == "bzip2":
        try:
            return bz2.decompress(block)
        except OSError as exc:
            raise AvroError(f"Avro bzip2 解压失败：{exc}") from None
    if codec == "xz":
        try:
            return lzma.decompress(block)
        except lzma.LZMAError as exc:
            raise AvroError(f"Avro xz 解压失败：{exc}") from None
    if codec in ("zstandard", "zstd"):
        try:
            import zstandard  # type: ignore
        except ImportError:
            raise AvroError(
                "Avro 使用 zstandard 压缩，但当前环境没有 zstandard 库。"
            ) from None
        try:
            return zstandard.ZstdDecompressor().decompress(block)
        except Exception as exc:
            raise AvroError(f"Avro zstandard 解压失败：{exc}") from None
    raise AvroError(f"不支持的 Avro 压缩 codec：{codec}")


def read_avro(path: str | Path) -> AvroFile:
    try:
        data = Path(path).read_bytes()
    except OSError as exc:
        raise AvroError(f"无法读取 Avro 文件：{exc}") from None
    if not data:
        raise AvroError("Avro 文件为空。")
    try:
        return _decode_avro_container(data)
    except AvroError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise AvroError(f"Avro 文件解码失败：{exc}") from None


def looks_like_manifest_list(path: str | Path) -> bool:
    """Return True when the Avro schema looks like an Iceberg manifest list."""
    try:
        avro = read_avro(path)
    except AvroError:
        return False
    schema = avro.schema
    if not isinstance(schema, dict) or schema.get("type") != "record":
        return False
    fields = {field.get("name") for field in schema.get("fields", [])}
    return "manifest_path" in fields and "manifest_length" in fields


# ---------------------------------------------------------------------------
# Writing (used by tools/make_sample.py; intentionally minimal)


def _encode_long(value: int, out: io.BytesIO) -> None:
    if value < -(1 << 63) or value >= (1 << 63):
        raise AvroError(f"Avro long 越界：{value}")
    raw = (value << 1) ^ (value >> 63)
    while True:
        byte = raw & 0x7F
        raw >>= 7
        if raw:
            out.write(bytes([byte | 0x80]))
        else:
            out.write(bytes([byte]))
            return


def _matches_schema(schema: Any, value: Any, namespace: str, registry: dict[str, Any]) -> bool:
    if isinstance(schema, str):
        if schema in _PRIMITIVES:
            return _matches_primitive(schema, value)
        resolved = _WriterRegistry.resolve(schema, namespace, registry)
        return _matches_schema(resolved, value, namespace, registry)
    if isinstance(schema, list):
        if value is None:
            return any(branch == "null" for branch in schema)
        return any(_matches_schema(branch, value, namespace, registry) for branch in schema)
    if isinstance(schema, dict):
        kind = schema.get("type")
        if kind in _PRIMITIVES:
            return _matches_primitive(str(kind), value)
        if kind == "record":
            return isinstance(value, dict)
        if kind == "array":
            return isinstance(value, (list, tuple))
        if kind == "map":
            return isinstance(value, dict)
        if kind == "enum":
            return isinstance(value, str)
        if kind == "fixed":
            return isinstance(value, (bytes, bytearray))
        if isinstance(kind, (dict, list)):
            return _matches_schema(kind, value, namespace, registry)
    return False


def _matches_primitive(kind: str, value: Any) -> bool:
    if kind == "null":
        return value is None
    if kind == "boolean":
        return isinstance(value, bool)
    if kind in ("int", "long"):
        return isinstance(value, int) and not isinstance(value, bool)
    if kind in ("float", "double"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind in ("bytes", "string"):
        return isinstance(value, (bytes, bytearray, str))
    return False


class _WriterRegistry:
    @staticmethod
    def resolve(name: str, namespace: str, registry: dict[str, Any]) -> Any:
        for candidate in (name, f"{namespace}.{name}" if namespace else ""):
            if candidate and candidate in registry:
                return registry[candidate]
        matches = [
            schema
            for fullname, schema in registry.items()
            if fullname.rsplit(".", 1)[-1] == name
        ]
        if len(matches) == 1:
            return matches[0]
        raise AvroError(f"Avro 类型引用无法解析：{name}")


def _encode(schema: Any, value: Any, out: io.BytesIO, namespace: str, registry: dict[str, Any]) -> None:
    if isinstance(schema, str):
        if schema in _PRIMITIVES:
            _encode_primitive(schema, value, out)
            return
        _encode(_WriterRegistry.resolve(schema, namespace, registry), value, out, namespace, registry)
        return

    if isinstance(schema, list):
        chosen = None
        if value is None:
            for index, branch in enumerate(schema):
                if branch == "null" or (
                    isinstance(branch, dict) and branch.get("type") == "null"
                ):
                    chosen = index
                    break
        else:
            for index, branch in enumerate(schema):
                if branch == "null":
                    continue
                if _matches_schema(branch, value, namespace, registry):
                    chosen = index
                    break
        if chosen is None:
            raise AvroError(f"无法为值 {value!r} 选择 Avro union 分支。")
        _encode_long(chosen, out)
        _encode(schema[chosen], value, out, namespace, registry)
        return

    if isinstance(schema, dict):
        kind = schema.get("type")
        if kind in _PRIMITIVES:
            _encode_primitive(str(kind), value, out)
            return
        if kind == "record":
            own_ns = schema.get("namespace", namespace)
            name = str(schema.get("name", ""))
            fullname = f"{own_ns}.{name}" if own_ns and "." not in name else name
            if "." in fullname and not schema.get("namespace"):
                own_ns = fullname.rsplit(".", 1)[0]
            for field in schema.get("fields", []):
                _encode(field["type"], value.get(field["name"]), out, own_ns, registry)
            return
        if kind == "array":
            items = list(value or [])
            if items:
                _encode_long(len(items), out)
                for item in items:
                    _encode(schema["items"], item, out, namespace, registry)
            _encode_long(0, out)
            return
        if kind == "map":
            if value:
                _encode_long(len(value), out)
                for key, item in value.items():
                    _encode("string", str(key), out, namespace, registry)
                    _encode(schema["values"], item, out, namespace, registry)
            _encode_long(0, out)
            return
        if kind == "enum":
            symbols = schema.get("symbols", [])
            try:
                index = symbols.index(value)
            except ValueError:
                raise AvroError(f"Avro enum 值无效：{value!r}") from None
            _encode_long(index, out)
            return
        if kind == "fixed":
            raw = value if isinstance(value, bytes) else bytes(value)
            if len(raw) != int(schema.get("size", 0)):
                raise AvroError("Avro fixed 长度不符。")
            out.write(raw)
            return
        _encode(kind, value, out, namespace, registry)
        return

    raise AvroError(f"无法写入的 Avro schema：{schema!r}")


def _encode_primitive(kind: str, value: Any, out: io.BytesIO) -> None:
    if kind == "null":
        return
    if kind == "boolean":
        out.write(b"\x01" if value else b"\x00")
        return
    if kind in ("int", "long"):
        _encode_long(int(value), out)
        return
    if kind == "float":
        out.write(struct.pack("<f", float(value)))
        return
    if kind == "double":
        out.write(struct.pack("<d", float(value)))
        return
    if kind in ("bytes", "string"):
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        _encode_long(len(raw), out)
        out.write(raw)
        return
    raise AvroError(f"未知 Avro 原始类型：{kind}")


def write_avro(
    path: str | Path,
    schema: Any,
    records: Iterable[dict[str, Any]],
    *,
    codec: str = "null",
    extra_metadata: dict[str, bytes] | None = None,
) -> None:
    codec = (codec or "null").lower()
    if codec not in ("null", "deflate"):
        raise AvroError("样例生成器仅支持 null/deflate codec。")
    schema_json = json.dumps(
        schema, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    metadata: dict[str, bytes] = {
        "avro.schema": schema_json,
        "avro.codec": codec.encode("utf-8"),
    }
    if extra_metadata:
        metadata.update(extra_metadata)

    out = io.BytesIO()
    out.write(MAGIC)
    _encode({"type": "map", "values": "bytes"}, metadata, out, "", {})
    sync = b"ferry-iceberg-01"
    out.write(sync)

    registry: dict[str, Any] = {}
    _collect_named(schema, registry)
    values = list(records)
    if values:
        block = io.BytesIO()
        for record in values:
            _encode(schema, record, block, "", registry)
        payload = block.getvalue()
        if codec == "deflate":
            compressor = zlib.compressobj(wbits=-15)
            payload = compressor.compress(payload) + compressor.flush()
        _encode_long(len(values), out)
        _encode_long(len(payload), out)
        out.write(payload)
        out.write(sync)

    Path(path).write_bytes(out.getvalue())
