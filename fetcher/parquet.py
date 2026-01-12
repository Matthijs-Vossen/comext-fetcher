from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

try:
    import pyarrow as pa
    import pyarrow.csv as csv
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - handled at runtime
    pa = None
    csv = None
    pq = None

try:
    import py7zr
except ImportError:  # pragma: no cover - handled at runtime
    py7zr = None

from .models import DownloadTarget

logger = logging.getLogger(__name__)

PRODUCT_OUTPUT_COLUMNS = [
    "REPORTER",
    "PARTNER",
    "TRADE_TYPE",
    "PRODUCT_NC",
    "FLOW",
    "STAT_PROCEDURE",
    "PERIOD",
    "VALUE_EUR",
    "QUANTITY_KG",
]

HISTORICAL_COLUMN_MAP = [
    ("DECLARANT_ISO", "REPORTER"),
    ("PARTNER_ISO", "PARTNER"),
    ("TRADE_TYPE", "TRADE_TYPE"),
    ("PRODUCT_NC", "PRODUCT_NC"),
    ("FLOW", "FLOW"),
    ("STAT_REGIME", "STAT_PROCEDURE"),
    ("PERIOD", "PERIOD"),
    ("VALUE_IN_EUROS", "VALUE_EUR"),
    ("QUANTITY_IN_KG", "QUANTITY_KG"),
]

def _product_column_types() -> dict[str, "pa.DataType"]:
    return {
        "REPORTER": pa.string(),
        "PARTNER": pa.string(),
        "TRADE_TYPE": pa.string(),
        "PRODUCT_NC": pa.string(),
        "FLOW": pa.string(),
        "STAT_PROCEDURE": pa.string(),
        "PERIOD": pa.int32(),
        "VALUE_EUR": pa.int64(),
        "QUANTITY_KG": pa.int64(),
    }


def _historical_column_types() -> dict[str, "pa.DataType"]:
    return {
        "DECLARANT_ISO": pa.string(),
        "PARTNER_ISO": pa.string(),
        "TRADE_TYPE": pa.string(),
        "PRODUCT_NC": pa.string(),
        "FLOW": pa.string(),
        "STAT_REGIME": pa.string(),
        "PERIOD": pa.int32(),
        "VALUE_IN_EUROS": pa.int64(),
        "QUANTITY_IN_KG": pa.int64(),
    }


def _output_schema() -> "pa.Schema":
    return pa.schema(
        [
            ("REPORTER", pa.string()),
            ("PARTNER", pa.string()),
            ("TRADE_TYPE", pa.string()),
            ("PRODUCT_NC", pa.string()),
            ("FLOW", pa.string()),
            ("STAT_PROCEDURE", pa.string()),
            ("PERIOD", pa.int32()),
            ("VALUE_EUR", pa.int64()),
            ("QUANTITY_KG", pa.int64()),
        ]
    )


@dataclass
class ConversionStats:
    converted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


def convert_targets_to_parquet(
    targets: Iterable[DownloadTarget],
    archive_root: Path,
    parquet_root: Path,
    *,
    max_workers: int,
    group: str,
    logger_: logging.Logger,
) -> ConversionStats:
    _ensure_dependencies()
    stats = ConversionStats()
    parquet_root.mkdir(parents=True, exist_ok=True)
    tasks: list[tuple[Path, Path]] = []
    for target in targets:
        archive_path = archive_root / target.filename
        if not archive_path.exists():
            stats.errors.append(f"{target.name}: archive not found")
            continue

        parquet_name = _parquet_name_for_target(target, group=group)
        parquet_path = parquet_root / parquet_name
        if parquet_path.exists():
            if parquet_path.stat().st_mtime >= archive_path.stat().st_mtime:
                stats.skipped += 1
                continue
            parquet_path.unlink()
            logger_.info("Parquet outdated, regenerating: %s", parquet_path)

        tasks.append((archive_path, parquet_path))

    if not tasks:
        return stats

    worker_count = max(1, max_workers)
    if worker_count == 1:
        for archive_path, parquet_path in tasks:
            try:
                convert_archive_to_parquet(
                    archive_path,
                    parquet_path,
                    group=group,
                )
            except Exception as exc:  # noqa: BLE001
                message = f"{archive_path.name}: {exc}"
                stats.errors.append(message)
                logger_.error("Parquet error: %s", message)
                continue

            stats.converted += 1
            logger_.info("Parquet written: %s", parquet_path)
        return stats

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                convert_archive_to_parquet,
                archive_path,
                parquet_path,
                group=group,
            ): (archive_path, parquet_path)
            for archive_path, parquet_path in tasks
        }
        for future in as_completed(future_map):
            archive_path, parquet_path = future_map[future]
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                message = f"{archive_path.name}: {exc}"
                stats.errors.append(message)
                logger_.error("Parquet error: %s", message)
                continue

            stats.converted += 1
            logger_.info("Parquet written: %s", parquet_path)

    return stats


def convert_archive_to_parquet(
    archive_path: Path,
    parquet_path: Path,
    *,
    group: str,
) -> None:
    _ensure_dependencies()
    with TemporaryDirectory(prefix="comext_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        dat_path = _extract_dat_file(archive_path, tmp_path)
        _write_parquet_from_dat(dat_path, parquet_path, group=group)


def _extract_dat_file(archive_path: Path, dest_dir: Path) -> Path:
    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        archive.extractall(path=dest_dir)

    dat_files = list(dest_dir.rglob("*.dat"))
    if not dat_files:
        raise ValueError("No .dat file found inside archive")
    if len(dat_files) > 1:
        logger.warning(
            "Multiple .dat files found in %s; using %s",
            archive_path.name,
            dat_files[0].name,
        )
    return dat_files[0]


def _write_parquet_from_dat(dat_path: Path, parquet_path: Path, *, group: str) -> None:
    if group == "products":
        include_columns = PRODUCT_OUTPUT_COLUMNS
        column_types = _product_column_types()
        column_map = [(name, name) for name in PRODUCT_OUTPUT_COLUMNS]
    elif group == "historical":
        include_columns = [source for source, _ in HISTORICAL_COLUMN_MAP]
        column_types = _historical_column_types()
        column_map = HISTORICAL_COLUMN_MAP
    elif group == "transport-hs":
        include_columns = None
        column_types = None
        column_map = None
    else:
        raise ValueError(f"Unsupported data group: {group}")

    read_options = csv.ReadOptions(autogenerate_column_names=False, use_threads=True)
    parse_options = csv.ParseOptions(delimiter=",")
    convert_options = csv.ConvertOptions(
        include_columns=include_columns,
        column_types=column_types,
    )

    reader = csv.open_csv(
        dat_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    writer = None
    output_schema = _output_schema() if group != "transport-hs" else None
    try:
        for batch in reader:
            if group == "transport-hs":
                table = pa.Table.from_batches([batch])
                table = _drop_total_rows(table)
            else:
                table = _build_output_table(batch, column_map)
                table = _drop_total_rows(table)
                if group == "historical":
                    table = _normalize_product_nc(table)

            if writer is None:
                schema = table.schema if group == "transport-hs" else output_schema
                writer = pq.ParquetWriter(parquet_path, schema=schema)

            if group != "transport-hs":
                table = table.cast(output_schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def _build_output_table(
    batch: pa.RecordBatch, column_map: list[tuple[str, str]]
) -> pa.Table:
    arrays = []
    names = []
    for source, target in column_map:
        arrays.append(_get_batch_column(batch, source))
        names.append(target)
    return pa.Table.from_arrays(arrays, names=names)


def _get_batch_column(batch: pa.RecordBatch, name: str) -> pa.Array:
    index = batch.schema.get_field_index(name)
    if index == -1:
        raise ValueError(f"Missing column '{name}' in input data.")
    return batch.column(index)


def _normalize_product_nc(table: pa.Table) -> pa.Table:
    values = table["PRODUCT_NC"].to_pylist()
    normalized = [_normalize_product_nc_value(value) for value in values]
    column_index = table.schema.get_field_index("PRODUCT_NC")
    columns = list(table.columns)
    columns[column_index] = pa.array(normalized, type=pa.string())
    return pa.Table.from_arrays(columns, names=table.schema.names)


def _drop_total_rows(table: pa.Table) -> pa.Table:
    if "PRODUCT_NC" not in table.schema.names:
        return table
    column = table["PRODUCT_NC"]
    upper = pc.utf8_upper(pc.cast(column, pa.string()))
    mask = pc.and_(pc.is_valid(upper), pc.not_equal(upper, "TOTAL"))
    return table.filter(mask)


def _normalize_product_nc_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    prefix = ""
    for char in text:
        if not char.isdigit():
            break
        prefix += char

    prefix = prefix[:8]
    if len(prefix) == len(text) and text.isdigit():
        return text
    return prefix + ("X" * (8 - len(prefix)))


def _ensure_dependencies() -> None:
    if pa is None or csv is None or pq is None:
        raise RuntimeError("pyarrow is required for parquet conversion.")
    if py7zr is None:
        raise RuntimeError("py7zr is required for extracting .7z archives.")


def _parquet_name_for_target(target: DownloadTarget, *, group: str) -> str:
    if group in ("products", "historical"):
        return f"comext_{target.yyyymm}.parquet"
    return Path(target.filename).with_suffix(".parquet").name
