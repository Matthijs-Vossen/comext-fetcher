from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

try:
    import tomllib
except ImportError:  # pragma: no cover - only used on Python < 3.11
    tomllib = None

from .client import DEFAULT_DATA_GROUP, DATA_GROUPS, EurostatClient
from .coverage import CoverageError, assert_monthly_coverage, build_expected_months
from .downloader import download_all
from .parquet import aggregate_targets_to_annual, convert_targets_to_parquet


def find_project_root(start: Path) -> Path:
    """Walk upwards from ``start`` to locate the repo root by common markers."""
    markers = ("pyproject.toml", ".git")
    for path in (start, *start.parents):
        if any((path / marker).exists() for marker in markers):
            return path
    return Path.cwd()


PROJECT_ROOT = find_project_root(Path(__file__).resolve().parent)
DEFAULT_DATA_ROOT = (PROJECT_ROOT / "data").resolve()
DEFAULT_COMPRESSED_ROOT = DEFAULT_DATA_ROOT / "compressed"
DEFAULT_EXTRACTED_ROOT = DEFAULT_DATA_ROOT / "extracted"
DEFAULT_EXTRACTED_NO_CONFIDENTIAL_ROOT = DEFAULT_DATA_ROOT / "extracted_no_confidential"
DEFAULT_EXTRACTED_ANNUAL_ROOT = DEFAULT_DATA_ROOT / "extracted_annual"
DEFAULT_EXTRACTED_ANNUAL_NO_CONFIDENTIAL_ROOT = (
    DEFAULT_DATA_ROOT / "extracted_annual_no_confidential"
)
DEFAULT_DEST_PRODUCTS = DEFAULT_COMPRESSED_ROOT / "products"
DEFAULT_DEST_TRANSPORT = DEFAULT_COMPRESSED_ROOT / "transport_hs"
DEFAULT_DEST_HISTORICAL = DEFAULT_COMPRESSED_ROOT / "historical"
DEFAULT_EXTRACTED_PRODUCTS_LIKE = DEFAULT_EXTRACTED_ROOT / "products_like"
DEFAULT_EXTRACTED_TRANSPORT = DEFAULT_EXTRACTED_ROOT / "transport_hs"
DEFAULT_EXTRACTED_NO_CONFIDENTIAL_PRODUCTS_LIKE = (
    DEFAULT_EXTRACTED_NO_CONFIDENTIAL_ROOT / "products_like"
)
DEFAULT_EXTRACTED_NO_CONFIDENTIAL_TRANSPORT = (
    DEFAULT_EXTRACTED_NO_CONFIDENTIAL_ROOT / "transport_hs"
)
DEFAULT_EXTRACTED_ANNUAL_PRODUCTS_LIKE = (
    DEFAULT_EXTRACTED_ANNUAL_ROOT / "products_like"
)
DEFAULT_EXTRACTED_ANNUAL_NO_CONFIDENTIAL_PRODUCTS_LIKE = (
    DEFAULT_EXTRACTED_ANNUAL_NO_CONFIDENTIAL_ROOT / "products_like"
)
DEFAULT_MAX_WORKERS = 6

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """Raised when the config file is missing or malformed."""


@dataclass
class FetcherConfig:
    dests: dict[str, Path]
    extracted_products_like: Path
    extracted_transport_hs: Path
    extracted_no_confidential_products_like: Path
    extracted_no_confidential_transport_hs: Path
    extracted_annual_products_like: Path
    extracted_annual_no_confidential_products_like: Path
    from_year: int
    to_year: Optional[int]
    data_groups: Sequence[str]
    max_workers: int
    drop_confidential: bool
    output_mode: str
    dry_run: bool
    verbose: bool


def entrypoint(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config).expanduser().resolve()
    try:
        config = load_config(config_path)
    except ConfigError as err:
        print(f"Config error: {err}", file=sys.stderr)
        raise SystemExit(2) from err

    if args.dry_run:
        config.dry_run = True
    if args.verbose:
        config.verbose = True

    setup_logging(config.verbose)

    try:
        run(config)
    except CoverageError as err:
        logger.error(str(err))
        raise SystemExit(1) from err
    except KeyboardInterrupt as err:  # pragma: no cover - user interrupt
        logger.error("Aborted by user")
        raise SystemExit(130) from err


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Eurostat Comext fetcher using a config file.",
    )
    parser.add_argument(
        "config",
        help="Path to a JSON/TOML config file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching files without downloading them.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser


def load_config(path: Path) -> FetcherConfig:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    raw = _read_config_file(path)
    if not isinstance(raw, dict):
        raise ConfigError("Config must be a JSON/TOML object at the top level.")

    return _build_config(raw)


def _read_config_file(path: Path) -> Mapping[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    if suffix == ".toml":
        if tomllib is None:
            raise ConfigError("TOML config requires Python 3.11+.")
        with path.open("rb") as handle:
            return tomllib.load(handle)
    raise ConfigError("Config must be .json or .toml.")


def _build_config(raw: Mapping[str, Any]) -> FetcherConfig:
    base_dest = _get_path(raw, "dest")
    dest_products = _get_path(raw, "dest_products", DEFAULT_DEST_PRODUCTS)
    dest_transport = _get_path(raw, "dest_transport_hs", DEFAULT_DEST_TRANSPORT)
    dest_historical = _get_path(raw, "dest_historical", DEFAULT_DEST_HISTORICAL)
    extracted_products_like = _get_path(
        raw,
        "extracted_products_like",
        DEFAULT_EXTRACTED_PRODUCTS_LIKE,
    )
    extracted_transport_hs = _get_path(
        raw,
        "extracted_transport_hs",
        DEFAULT_EXTRACTED_TRANSPORT,
    )
    extracted_no_confidential_products_like = _get_path(
        raw,
        "extracted_no_confidential_products_like",
        DEFAULT_EXTRACTED_NO_CONFIDENTIAL_PRODUCTS_LIKE,
    )
    extracted_no_confidential_transport_hs = _get_path(
        raw,
        "extracted_no_confidential_transport_hs",
        DEFAULT_EXTRACTED_NO_CONFIDENTIAL_TRANSPORT,
    )
    extracted_annual_products_like = _get_path(
        raw,
        "extracted_annual_products_like",
        DEFAULT_EXTRACTED_ANNUAL_PRODUCTS_LIKE,
    )
    extracted_annual_no_confidential_products_like = _get_path(
        raw,
        "extracted_annual_no_confidential_products_like",
        DEFAULT_EXTRACTED_ANNUAL_NO_CONFIDENTIAL_PRODUCTS_LIKE,
    )

    if base_dest is not None:
        dest_products = base_dest
        dest_historical = base_dest

    data_groups = _resolve_data_groups(raw)

    return FetcherConfig(
        dests={
            "products": dest_products,
            "transport-hs": dest_transport,
            "historical": dest_historical,
        },
        extracted_products_like=extracted_products_like,
        extracted_transport_hs=extracted_transport_hs,
        extracted_no_confidential_products_like=extracted_no_confidential_products_like,
        extracted_no_confidential_transport_hs=extracted_no_confidential_transport_hs,
        extracted_annual_products_like=extracted_annual_products_like,
        extracted_annual_no_confidential_products_like=extracted_annual_no_confidential_products_like,
        from_year=_get_int(raw, "from_year", 2002),
        to_year=_get_int(raw, "to_year", None, allow_none=True),
        data_groups=data_groups,
        max_workers=_get_int(raw, "max_workers", DEFAULT_MAX_WORKERS),
        drop_confidential=_get_bool(raw, "drop_confidential", False),
        output_mode=_get_output_mode(raw),
        dry_run=_get_bool(raw, "dry_run", False),
        verbose=_get_bool(raw, "verbose", False),
    )


def _get_int(
    raw: Mapping[str, Any],
    key: str,
    default: Optional[int],
    *,
    allow_none: bool = False,
) -> Optional[int]:
    value = raw.get(key, default)
    if value is None:
        if allow_none:
            return None
        raise ConfigError(f"Missing required integer field: {key}")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Field '{key}' must be an integer.")
    return value


def _get_bool(raw: Mapping[str, Any], key: str, default: bool) -> bool:
    value = raw.get(key, default)
    if isinstance(value, bool):
        return value
    raise ConfigError(f"Field '{key}' must be a boolean.")


def _get_path(
    raw: Mapping[str, Any],
    key: str,
    default: Optional[Path] = None,
) -> Optional[Path]:
    value = raw.get(key, None)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"Field '{key}' must be a path string.")
    return Path(value).expanduser().resolve()


def _get_output_mode(raw: Mapping[str, Any]) -> str:
    value = raw.get("output_mode", "both")
    if not isinstance(value, str):
        raise ConfigError("Field 'output_mode' must be a string.")
    value = value.strip().lower()
    if value not in {"monthly", "both"}:
        raise ConfigError("Field 'output_mode' must be one of: monthly, both.")
    return value


def _resolve_data_groups(raw: Mapping[str, Any]) -> Sequence[str]:
    if "data_groups" in raw:
        return _parse_data_groups(raw["data_groups"])

    return (DEFAULT_DATA_GROUP,)


def _parse_data_groups(value: Any) -> Sequence[str]:
    if value is None:
        return (DEFAULT_DATA_GROUP,)
    if isinstance(value, dict):
        groups = [key for key, enabled in value.items() if bool(enabled)]
    else:
        raise ConfigError(
            "Field 'data_groups' must be a mapping of group names to booleans."
        )

    if not groups:
        raise ConfigError("Field 'data_groups' must not be empty.")

    seen = set()
    unique: list[str] = []
    for group in groups:
        if group not in DATA_GROUPS:
            raise ConfigError(f"Unknown data group: {group}")
        if group in seen:
            continue
        seen.add(group)
        unique.append(group)

    return tuple(unique)


def run(config: FetcherConfig) -> None:
    client = EurostatClient()

    logger.info("Listing available files from Eurostat...")
    targets = client.collect_targets(
        data_groups=config.data_groups,
        from_year=config.from_year,
        to_year=config.to_year,
    )

    if not targets:
        logger.info("No files matched your criteria.")
        return

    by_group: dict[str, list] = {}
    for target in targets:
        by_group.setdefault(target.group, []).append(target)

    total_bytes = sum(target.size for target in targets if target.size is not None)
    size_str = f"{total_bytes / 1e9:.2f} GB" if total_bytes else "size N/A"
    logger.info(
        "Found %s files across %s group(s) (%s)",
        len(targets),
        len(by_group),
        size_str,
    )

    if config.dry_run:
        for group, items in by_group.items():
            logger.info("\nGroup: %s  -> %s", group, config.dests[group])
            for target in items:
                logger.info(
                    "%s: %s  [%s bytes]  <- %s",
                    target.yyyymm,
                    target.name,
                    target.size if target.size is not None else "unknown",
                    target.dir_path,
                )
        return

    aggregate_downloaded = 0
    aggregate_skipped = 0
    aggregate_errors: list[str] = []
    aggregate_converted = 0
    aggregate_conversion_skipped = 0
    aggregate_conversion_errors: list[str] = []
    aggregate_annual = 0
    aggregate_annual_skipped = 0
    aggregate_annual_errors: list[str] = []
    write_monthly = True
    write_annual = config.output_mode == "both"
    for group, items in by_group.items():
        compressed_dir = config.dests[group]
        if config.drop_confidential:
            if group in ("products", "historical"):
                extracted_dir = config.extracted_no_confidential_products_like
            else:
                extracted_dir = config.extracted_no_confidential_transport_hs
        else:
            if group in ("products", "historical"):
                extracted_dir = config.extracted_products_like
            else:
                extracted_dir = config.extracted_transport_hs
        extracted_dir.mkdir(parents=True, exist_ok=True)
        compressed_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            "\nDownloading %s files to %s (group=%s)...",
            len(items),
            compressed_dir,
            group,
        )
        stats = download_all(
            client,
            items,
            compressed_dir,
            max_workers=config.max_workers,
            logger_=logger,
        )
        aggregate_downloaded += stats.downloaded
        aggregate_skipped += stats.skipped
        aggregate_errors.extend(stats.errors)

        conversion_stats = convert_targets_to_parquet(
            items,
            compressed_dir,
            extracted_dir,
            drop_confidential=config.drop_confidential,
            max_workers=config.max_workers,
            group=group,
            logger_=logger,
        )
        aggregate_converted += conversion_stats.converted
        aggregate_conversion_skipped += conversion_stats.skipped
        aggregate_conversion_errors.extend(conversion_stats.errors)

        if write_annual and group in ("products", "historical"):
            if config.drop_confidential:
                annual_dir = config.extracted_annual_no_confidential_products_like
            else:
                annual_dir = config.extracted_annual_products_like
            annual_dir.mkdir(parents=True, exist_ok=True)
            annual_stats = aggregate_targets_to_annual(
                items,
                extracted_dir,
                annual_dir,
                max_workers=config.max_workers,
                group=group,
                logger_=logger,
            )
            aggregate_annual += annual_stats.aggregated
            aggregate_annual_skipped += annual_stats.skipped
            aggregate_annual_errors.extend(annual_stats.errors)

    logger.info("")
    logger.info(
        "Done. New: %s, Skipped: %s, Errors: %s",
        aggregate_downloaded,
        aggregate_skipped,
        len(aggregate_errors),
    )
    logger.info(
        "Parquet. New: %s, Skipped: %s, Errors: %s",
        aggregate_converted,
        aggregate_conversion_skipped,
        len(aggregate_conversion_errors),
    )
    if write_annual:
        logger.info(
            "Annual parquet. New: %s, Skipped: %s, Errors: %s",
            aggregate_annual,
            aggregate_annual_skipped,
            len(aggregate_annual_errors),
        )
    if aggregate_errors:
        logger.warning(
            "Some downloads failed. You can re-run the script; it will skip completed "
            "files and retry the rest."
        )
    if aggregate_conversion_errors:
        logger.warning(
            "Some parquet conversions failed. You can re-run the script to retry missing parquet files."
        )
    if aggregate_annual_errors:
        logger.warning(
            "Some annual parquet aggregations failed. You can re-run the script to retry missing annual files."
        )

    dest_expected: dict[Path, dict] = {}
    for group, items in by_group.items():
        dest = config.dests[group]
        expected = build_expected_months(items)
        combined = dest_expected.get(dest) or {}
        for year, months in expected.items():
            combined.setdefault(year, set()).update(months)
        dest_expected[dest] = combined

    for dest, expected in dest_expected.items():
        assert_monthly_coverage(dest, expected)
        logger.info("\nMonthly coverage check passed for %s.", dest)


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(message)s", stream=sys.stdout)
