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
ANNUAL_BATCH_SIZE = 200_000

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
ANNUAL_OUTPUT_COLUMNS = [
    "REPORTER",
    "PARTNER",
    "TRADE_TYPE",
    "PRODUCT_NC",
    "FLOW",
    "PERIOD",
    "VALUE_EUR",
    "QUANTITY_KG",
]

STAT_PROCEDURE_MAP = {
    "5": "2",
    "6": "2",
    "7": "3",
}
STAT_PROCEDURE_OLD_CODES = set(STAT_PROCEDURE_MAP)

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
        "VALUE_EUR": pa.float64(),
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
        "VALUE_IN_EUROS": pa.float64(),
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
            ("VALUE_EUR", pa.float64()),
            ("QUANTITY_KG", pa.int64()),
        ]
    )


def _annual_output_schema() -> "pa.Schema":
    return pa.schema(
        [
            ("REPORTER", pa.string()),
            ("PARTNER", pa.string()),
            ("TRADE_TYPE", pa.string()),
            ("PRODUCT_NC", pa.string()),
            ("FLOW", pa.string()),
            ("PERIOD", pa.int32()),
            ("VALUE_EUR", pa.float64()),
            ("QUANTITY_KG", pa.int64()),
        ]
    )


@dataclass
class ConversionStats:
    converted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class AnnualAggregationStats:
    aggregated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


def convert_targets_to_parquet(
    targets: Iterable[DownloadTarget],
    archive_root: Path,
    parquet_root: Path,
    *,
    drop_confidential: bool = False,
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
                    drop_confidential=drop_confidential,
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
                drop_confidential=drop_confidential,
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


def aggregate_targets_to_annual(
    targets: Iterable[DownloadTarget],
    parquet_root: Path,
    annual_root: Path,
    *,
    max_workers: int,
    group: str,
    logger_: logging.Logger,
) -> AnnualAggregationStats:
    _ensure_dependencies()
    stats = AnnualAggregationStats()
    annual_root.mkdir(parents=True, exist_ok=True)

    targets_by_year: dict[int, list[DownloadTarget]] = {}
    for target in targets:
        year = int(target.yyyymm[:4])
        targets_by_year.setdefault(year, []).append(target)

    tasks: list[tuple[int, list[DownloadTarget]]] = sorted(targets_by_year.items())
    if not tasks:
        return stats

    worker_count = max(1, max_workers)
    if worker_count == 1:
        for year, year_targets in tasks:
            _aggregate_year(
                year,
                year_targets,
                parquet_root,
                annual_root,
                group=group,
                stats=stats,
                logger_=logger_,
            )
        return stats

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _aggregate_year,
                year,
                year_targets,
                parquet_root,
                annual_root,
                group=group,
                stats=stats,
                logger_=logger_,
            ): year
            for year, year_targets in tasks
        }
        for future in as_completed(future_map):
            future.result()

    return stats


def _aggregate_year(
    year: int,
    year_targets: list[DownloadTarget],
    parquet_root: Path,
    annual_root: Path,
    *,
    group: str,
    stats: AnnualAggregationStats,
    logger_: logging.Logger,
) -> None:
    annual_path = annual_root / f"comext_{year}.parquet"
    monthly_paths: list[Path] = []
    for target in year_targets:
        monthly_path = parquet_root / _parquet_name_for_target(target, group=group)
        if monthly_path.exists():
            monthly_paths.append(monthly_path)

    if not monthly_paths:
        stats.errors.append(f"{year}: no monthly parquet files found")
        return

    latest_mtime = max(path.stat().st_mtime for path in monthly_paths)
    if annual_path.exists() and annual_path.stat().st_mtime >= latest_mtime:
        stats.skipped += 1
        return
    if annual_path.exists():
        annual_path.unlink()

    try:
        _write_annual_parquet(monthly_paths, annual_path)
    except Exception as exc:  # noqa: BLE001
        message = f"{year}: {exc}"
        stats.errors.append(message)
        logger_.error("Annual parquet error: %s", message)
        return

    stats.aggregated += 1
    logger_.info("Annual parquet written: %s", annual_path)


def convert_archive_to_parquet(
    archive_path: Path,
    parquet_path: Path,
    *,
    group: str,
    drop_confidential: bool = False,
) -> None:
    _ensure_dependencies()
    with TemporaryDirectory(prefix="comext_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        dat_path = _extract_dat_file(archive_path, tmp_path)
        _write_parquet_from_dat(
            dat_path,
            parquet_path,
            group=group,
            drop_confidential=drop_confidential,
        )


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


def _write_parquet_from_dat(
    dat_path: Path,
    parquet_path: Path,
    *,
    group: str,
    drop_confidential: bool = False,
) -> None:
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

    output_schema = _output_schema() if group != "transport-hs" else None
    writer = None
    try:
        if group in ("products", "historical"):
            to_aggregate_tables: list[pa.Table] = []
            untouched_tables: list[pa.Table] = []
            saw_old_codes = False
            for batch in reader:
                table = _build_output_table(batch, column_map)
                table = _drop_total_rows(table)
                if table.num_rows == 0:
                    continue
                if group == "historical":
                    table = _normalize_product_nc(table)
                if drop_confidential:
                    table = _drop_confidential_rows(table)
                    if table.num_rows == 0:
                        continue
                had_old_codes = _has_old_stat_procedure(table)
                saw_old_codes = saw_old_codes or had_old_codes
                table = _normalize_stat_procedure(table)
                to_aggregate, untouched = _split_stat_aggregate_candidates(table)
                if to_aggregate is not None and to_aggregate.num_rows:
                    to_aggregate_tables.append(to_aggregate)
                if untouched is not None and untouched.num_rows:
                    untouched_tables.append(untouched)

            aggregated = None
            if saw_old_codes:
                merged = _concat_tables(to_aggregate_tables)
                if merged is not None:
                    aggregated = _aggregate_products_like_table(merged)
            combined = _concat_tables(
                untouched_tables + ([aggregated] if aggregated is not None else [])
                if saw_old_codes
                else untouched_tables + to_aggregate_tables
            )
            if combined is None:
                combined = _empty_table(output_schema)
            combined = combined.cast(output_schema)
            with pq.ParquetWriter(parquet_path, schema=output_schema) as writer:
                writer.write_table(combined)
        else:
            writer = None
            for batch in reader:
                table = pa.Table.from_batches([batch])
                table = _drop_total_rows(table)
                if drop_confidential:
                    table = _drop_confidential_rows(table)
                    if table.num_rows == 0:
                        continue
                if writer is None:
                    writer = pq.ParquetWriter(parquet_path, schema=table.schema)
                writer.write_table(table)
    finally:
        if group == "transport-hs" and writer is not None:
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


def _normalize_stat_procedure(table: pa.Table) -> pa.Table:
    if "STAT_PROCEDURE" not in table.schema.names:
        return table
    values = table["STAT_PROCEDURE"].to_pylist()
    normalized = [_normalize_stat_procedure_value(value) for value in values]
    column_index = table.schema.get_field_index("STAT_PROCEDURE")
    columns = list(table.columns)
    columns[column_index] = pa.array(normalized, type=pa.string())
    return pa.Table.from_arrays(columns, names=table.schema.names)


def _aggregate_products_like_table(table: pa.Table) -> pa.Table:
    group_keys = [
        "REPORTER",
        "PARTNER",
        "TRADE_TYPE",
        "PRODUCT_NC",
        "FLOW",
        "STAT_PROCEDURE",
        "PERIOD",
    ]
    aggregated = table.group_by(group_keys).aggregate(
        [("VALUE_EUR", "sum"), ("QUANTITY_KG", "sum")]
    )
    aggregated = aggregated.rename_columns(
        group_keys + ["VALUE_EUR", "QUANTITY_KG"]
    )
    return aggregated.select(PRODUCT_OUTPUT_COLUMNS)


def _aggregate_annual_table(table: pa.Table) -> pa.Table:
    group_keys = [
        "REPORTER",
        "PARTNER",
        "TRADE_TYPE",
        "PRODUCT_NC",
        "FLOW",
        "PERIOD",
    ]
    aggregated = table.group_by(group_keys).aggregate(
        [("VALUE_EUR", "sum"), ("QUANTITY_KG", "sum")]
    )
    aggregated = aggregated.rename_columns(
        group_keys + ["VALUE_EUR", "QUANTITY_KG"]
    )
    return aggregated.select(ANNUAL_OUTPUT_COLUMNS)


def _write_annual_parquet(monthly_paths: list[Path], annual_path: Path) -> None:
    annual_schema = _annual_output_schema()
    columns = [
        "REPORTER",
        "PARTNER",
        "TRADE_TYPE",
        "PRODUCT_NC",
        "FLOW",
        "PERIOD",
        "VALUE_EUR",
        "QUANTITY_KG",
    ]
    tables: list[pa.Table] = []
    for monthly_path in monthly_paths:
        parquet_file = pq.ParquetFile(monthly_path)
        for batch in parquet_file.iter_batches(
            columns=columns,
            batch_size=ANNUAL_BATCH_SIZE,
        ):
            table = pa.Table.from_batches([batch])
            period_index = table.schema.get_field_index("PERIOD")
            year_values = pc.divide(table.column(period_index), 100)
            year_values = pc.floor(year_values)
            year_values = pc.cast(year_values, pa.int32())
            table = table.set_column(period_index, "PERIOD", year_values)
            tables.append(_aggregate_annual_table(table))

    merged = _concat_tables(tables)
    if merged is None:
        annual = _empty_table(annual_schema)
    else:
        annual = _aggregate_annual_table(merged).cast(annual_schema)
    with pq.ParquetWriter(annual_path, schema=annual_schema) as writer:
        writer.write_table(annual)


def _split_stat_aggregate_candidates(
    table: pa.Table,
) -> tuple[pa.Table | None, pa.Table | None]:
    if "STAT_PROCEDURE" not in table.schema.names:
        return None, table
    mask = pc.is_in(table["STAT_PROCEDURE"], value_set=pa.array(["2", "3"]))
    if not pc.any(mask).as_py():
        return None, table
    return table.filter(mask), table.filter(pc.invert(mask))


def _has_old_stat_procedure(table: pa.Table) -> bool:
    if "STAT_PROCEDURE" not in table.schema.names:
        return False
    mask = pc.is_in(
        table["STAT_PROCEDURE"],
        value_set=pa.array(sorted(STAT_PROCEDURE_OLD_CODES)),
    )
    return bool(pc.any(mask).as_py())


def _drop_total_rows(table: pa.Table) -> pa.Table:
    if "PRODUCT_NC" not in table.schema.names:
        return table
    column = table["PRODUCT_NC"]
    upper = pc.utf8_upper(pc.cast(column, pa.string()))
    mask = pc.and_(pc.is_valid(upper), pc.not_equal(upper, "TOTAL"))
    return table.filter(mask)


def _drop_confidential_rows(table: pa.Table) -> pa.Table:
    if "PRODUCT_NC" not in table.schema.names:
        return table
    column = pc.cast(table["PRODUCT_NC"], pa.string())
    upper = pc.utf8_upper(column)
    has_x = pc.match_substring(upper, "X")
    has_x = pc.fill_null(has_x, False)
    mask = pc.invert(has_x)
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


def _normalize_stat_procedure_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return STAT_PROCEDURE_MAP.get(text, text)


def _empty_table(schema: pa.Schema) -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, names=schema.names)


def _concat_tables(tables: list[pa.Table | None]) -> pa.Table | None:
    non_empty = [table for table in tables if table is not None and table.num_rows]
    if not non_empty:
        return None
    if len(non_empty) == 1:
        return non_empty[0]
    return pa.concat_tables(non_empty, promote_options="default")


def _ensure_dependencies() -> None:
    if pa is None or csv is None or pq is None:
        raise RuntimeError("pyarrow is required for parquet conversion.")
    if py7zr is None:
        raise RuntimeError("py7zr is required for extracting .7z archives.")


def _parquet_name_for_target(target: DownloadTarget, *, group: str) -> str:
    if group in ("products", "historical"):
        return f"comext_{target.yyyymm}.parquet"
    return Path(target.filename).with_suffix(".parquet").name
