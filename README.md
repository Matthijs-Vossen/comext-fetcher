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
| `max_workers` | Parallel downloads and parquet conversions (default: `6`). |
| `data_groups` | Map of data groups to booleans (`products`, `historical`, `transport-hs`). |
| `drop_confidential` | Drop rows with `PRODUCT_NC` containing `X` and write into the no-confidential output paths (default: `false`). |
| `output_mode` | Choose outputs: `monthly` or `both` (default: `both`). |
| `dry_run` | List matching files without downloading them (can be overridden by `--dry-run`). |
| `verbose` | Enable debug-level logging (can be overridden by `--verbose`). |

## Column Selection and Historical Alignment
- Products parquet output keeps only: `REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `STAT_PROCEDURE`, `PERIOD`, `VALUE_EUR`, `QUANTITY_KG`.
- Historical parquet output is mapped into the same schema using: `DECLARANT_ISO -> REPORTER`, `PARTNER_ISO -> PARTNER`, `STAT_REGIME -> STAT_PROCEDURE`, `VALUE_IN_EUROS -> VALUE_EUR`, `QUANTITY_IN_KG -> QUANTITY_KG`, and shared columns (`TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `PERIOD`).
- `STAT_PROCEDURE` codes are harmonized by mapping `5`/`6` -> `2` and `7` -> `3` to match the post-2009 scheme; rows that collapse to identical keys are aggregated by summing `VALUE_EUR` and `QUANTITY_KG`.
- Historical `PRODUCT_NC` values with non-numeric suffixes are normalized by keeping the numeric prefix and padding the remainder to 8 chars with `X` (e.g. `99RRR100` -> `99XXXXXX`).
- Rows with `PRODUCT_NC` equal to `TOTAL` (case-insensitive) are dropped in all groups before writing parquet.
- Historical outputs aggregate rows that share the same ISO-level key (including `STAT_PROCEDURE`) by summing `VALUE_EUR` and `QUANTITY_KG`.
- Products + historical output is cast to a fixed schema (`PERIOD` int32, `VALUE_EUR` float64, `QUANTITY_KG` int64, others string).
- Transport parquet output preserves all columns from the source file.
- When `drop_confidential` is enabled, any row with `PRODUCT_NC` containing `X` is dropped and output is written to the no-confidential paths.
- Annual outputs aggregate across `STAT_PROCEDURE` and months, grouping by `REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, and year (`PERIOD`).
- `output_mode` controls whether annual aggregates are produced alongside monthly parquet outputs.

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
