# Eurostat Comext Fetcher

`fetcher` is a focused downloader for the Eurostat Comext bulk dataset. It queries the public dissemination API, fetches monthly archives for PRODUCTS, the 1988-2001 historical backfill, and TRANSPORT_HS, then verifies local monthly coverage.

## Usage
```bash
python -m fetcher path/to/config.json [--dry-run] [--verbose]
```

The config file describes the run settings. JSON and TOML are supported.

Full example config: `config/config.json`.

## Parquet Output
After downloading, the fetcher extracts each `.7z` archive to a temporary `.dat`, writes a `.parquet` file, and deletes the temporary `.dat`. The `.7z` archives are kept.

Default layout:
- `data/compressed/products/` for PRODUCTS `.7z`
- `data/compressed/historical/` for HISTORICAL `.7z`
- `data/compressed/transport_hs/` for TRANSPORT_HS `.7z`
- `data/extracted/products_like/` for PRODUCTS + HISTORICAL parquet
- `data/extracted/transport_hs/` for TRANSPORT_HS parquet
- `data/extracted_no_confidential/products_like/` for PRODUCTS + HISTORICAL parquet without confidential rows (when enabled)
- `data/extracted_no_confidential/transport_hs/` for TRANSPORT_HS parquet without confidential rows (when enabled)
- `data/extracted_annual/products_like/` for annual PRODUCTS + HISTORICAL parquet (when enabled)
- `data/extracted_annual_no_confidential/products_like/` for annual PRODUCTS + HISTORICAL parquet without confidential rows (when enabled)

Parquet naming:
- Products + historical: `comext_YYYYMM.parquet`
- Transport HS: same base name as the archive.
- Annual products + historical: `comext_YYYY.parquet`

Dependencies:
- `pyarrow`
- `py7zr`

## Config Settings
| Key | Description |
| --- | --- |
| `dest` | Base destination. Overrides `dest_products` and `dest_historical`. |
| `dest_products` | Group root for PRODUCTS archives (default: `data/compressed/products`). |
| `dest_historical` | Group root for HISTORICAL archives (default: `data/compressed/historical`). |
| `dest_transport_hs` | Group root for TRANSPORT_HS archives (default: `data/compressed/transport_hs`). |
| `extracted_products_like` | Output folder for PRODUCTS + HISTORICAL parquet (default: `data/extracted/products_like`). |
| `extracted_transport_hs` | Output folder for TRANSPORT_HS parquet (default: `data/extracted/transport_hs`). |
| `extracted_no_confidential_products_like` | Output folder for PRODUCTS + HISTORICAL parquet without confidential rows (default: `data/extracted_no_confidential/products_like`). |
| `extracted_no_confidential_transport_hs` | Output folder for TRANSPORT_HS parquet without confidential rows (default: `data/extracted_no_confidential/transport_hs`). |
| `extracted_annual_products_like` | Output folder for annual PRODUCTS + HISTORICAL parquet (default: `data/extracted_annual/products_like`). |
| `extracted_annual_no_confidential_products_like` | Output folder for annual PRODUCTS + HISTORICAL parquet without confidential rows (default: `data/extracted_annual_no_confidential/products_like`). |
| `from_year` | Earliest year to include (default: `2002`). |
| `to_year` | Latest year to include (default: all). |
| `max_workers` | Parallelism for CPU-heavy steps; integer or `auto` (uses `max(1, cpu_count - 2)` capped at `14`). Downloads are additionally capped at `10`. |
| `data_groups` | Map of data groups to booleans (`products`, `historical`, `transport-hs`). |
| `drop_confidential` | Drop rows with `PRODUCT_NC` containing `X` and write into the no-confidential output paths (default: `false`). |
| `output_mode` | Choose outputs: `monthly` or `both` (default: `both`). |
| `dry_run` | List matching files without downloading them (can be overridden by `--dry-run`). |
| `verbose` | Enable debug-level logging (can be overridden by `--verbose`). |

## Processing Choices (Data Shape)
### Schema alignment & typing
- Products outputs keep only: `REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `STAT_PROCEDURE`, `PERIOD`, `VALUE_EUR`, `QUANTITY_KG`.
- Historical inputs are mapped into that same schema: `DECLARANT_ISO -> REPORTER`, `PARTNER_ISO -> PARTNER`, `STAT_REGIME -> STAT_PROCEDURE`, `VALUE_IN_EUROS -> VALUE_EUR`, `QUANTITY_IN_KG -> QUANTITY_KG`, plus shared columns (`TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `PERIOD`).
- Products + historical outputs are cast to fixed dtypes (`PERIOD` int32, `VALUE_EUR` float64, `QUANTITY_KG` int64, others string) for stable downstream typing.
- Transport parquet output preserves all columns from the source file (no normalization).

### Normalization rules
- `STAT_PROCEDURE` harmonization applies only when old codes exist: `5`/`6` -> `2`, `7` -> `3`.
- Historical `PRODUCT_NC` values with non-numeric suffixes are normalized by keeping the numeric prefix and padding to 8 chars with `X` (e.g. `99RRR100` -> `99XXXXXX`), to match the non-historical CN8 format.

### Filtering rules
- Rows with `PRODUCT_NC == TOTAL` (case-insensitive) are dropped everywhere, because they are aggregate totals rather than CN8 product records.
- When `drop_confidential=true`, any row with `PRODUCT_NC` containing `X` is removed and written to the no-confidential output paths.

### Aggregation rules
- If `STAT_PROCEDURE` harmonization creates duplicate keys, only those affected rows are merged by summing `VALUE_EUR` and `QUANTITY_KG` (preserving post-2009 procedure totals).
- Historical outputs always collapse duplicate ISO-level keys (`REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `STAT_PROCEDURE`, `PERIOD`) by summing measures, since the historical files contain (a practically negligible amount of) duplicates at ISO level.
- When `drop_confidential=false`, masked-code duplicates in non-historical products are merged by the output key (`REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `STAT_PROCEDURE`, `PERIOD`) so multiple raw rows that collapse to the same masked code (e.g., `48XXXXXX`) are summed.
- Annual outputs sum monthly data across the year, aggregating across `STAT_PROCEDURE` and using keys `REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, and year (`PERIOD`); controlled by `output_mode`.

## Typical Workflows
- Discover matching files before downloading:
  ```bash
  python -m fetcher config/config.json --dry-run
  ```

## Download Lifecycle
1. The fetcher enumerates monthly archives from the Eurostat API and filters them by year range.
2. Existing local archives with matching file sizes are skipped, making reruns idempotent.
3. Remaining files download in parallel. Temporary `.partial` files guard against incomplete transfers.
4. Completion stats (downloaded/skipped/errors) are printed, followed by a monthly coverage verification. Missing months trigger a non-zero exit status so you can rerun or investigate upstream gaps.

## Troubleshooting
- Missing months reported: rerun the command; only absent months are retried. The error message distinguishes between local gaps and months missing from the API listing itself.
- HTTP failures or throttling: transient errors are retried with exponential backoff. Persistent issues surface in the log; consider lowering `max-workers` or trying later.
- Disk capacity: the destination directory is created on demand. Ensure it resides on storage with sufficient space; the log shows a size estimate before downloads begin.
