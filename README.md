# 渡轮结算 · Iceberg v2 航次日文件补取

湖仓值班员做渡轮结算迁移时，需要从 Iceberg v2 备份里补取某个 **UTC 航次日**
的 Parquet 数据文件和删除文件。备份可能缺少其他日期的 manifest，因此网页
按 manifest list 的分区上下界只读取与所选航次日可能相交的 manifest，并导出
一份核查页（`/verify`）可以直接读取的 CSV。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。样例同时含
`day(voyage_time)` 和 `hour(voyage_time)` 两个分区定义。健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **表 `metadata.json`**（`format-version` 必须为 2）。
2. 上传 **快照的 Avro manifest list**。
3. 上传 **Avro manifest 副本**，可多选；副本必须保留 manifest list 中的原文件名。
4. 选择 **UTC 航次日**，提交。

没有专门选择 manifest list 时，网页会按 `metadata.json` 当前快照的
`manifest-list` 路径中的文件名匹配上传副本。

上传的 manifest 缺失/损坏只有在它 **需要被读取** 时才报错；被分区上下界确定
为其他日期的 manifest 不必上传。所有上传文件名必须唯一，否则直接拒绝。

### 分区支持

只支持 **同一个发航时间列** 上的单字段分区：

| 分区 transform | 上下界解释 | 条目匹配 |
| --- | --- | --- |
| `day` | 自 1970-01-01 起的 UTC 天数 | 分区值等于所选航次日 |
| `hour` | 自 1970-01-01 00:00 UTC 起的小时数 | 分区值落在所选航次日的 24 小时内 |

manifest list 中每条 manifest 使用自己的 `partition_spec_id` 解释上下界。
只要下界或上界缺失、无法解码，就读取该 manifest；只有上下界 **确定** 与当天
不相交时才跳过。条目再按自己的分区值过滤，`DELETED` 条目一律排除。

### 数据序列号

- `EXISTING`、`ADDED` 条目保留条目自身的 `sequence_number`；
- 只有 `ADDED` 且条目序列号为 null 时，才继承 manifest list 中该 manifest 的
  `sequence_number`；
- 需要读取的 manifest 缺失、损坏，或选中条目的序列号无法确定时，整次补取
  停止，页面明确指出 manifest 原始路径，且不生成清单/CSV。

### 导出 CSV

CSV 列为：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
```

`file_type` 为 `data`、`position_delete` 或 `equality_delete`；该格式可直接上传
到核查页 `/verify`。当天没有任何文件时页面显示“当天没有文件”，CSV 只含表头。
内置样例导出的 CSV 可直接与 `sample/` 下的 Parquet 副本一起交给核查页。

## 核查页

原有票号位置删除核对页仍在 `/verify`（样例 `/verify/sample`）。它接收导出的
CSV 和对应 Parquet 副本，逐票核对位置删除。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow（核查页使用）。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q
```

重新生成样例（无需 PyArrow）：

```bash
python3 tools/make_sample.py
```

`checker/avro.py` 是纯 Python 的 Avro OCF 读写实现，支持 `null`、`deflate`、
`snappy`、`bzip2`、`xz` 解码，不依赖额外的 Avro 库。

## 目录

```
app.py                     Flask 路由、上传处理、CSV 导出
checker/iceberg.py         metadata/manifest list/manifest 解析与航次日筛选
checker/avro.py            纯 Python Avro OCF 读写
checker/engine.py          核查页的 Parquet 行组跳读与位置删除匹配
checker/manifest.py        核查页 CSV/JSON 清单解析
templates/recover.html     航次日补取页面
templates/index.html       核查页
sample/                    内置样例（metadata.json、Avro、Parquet）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile / docker-compose.yml
```
