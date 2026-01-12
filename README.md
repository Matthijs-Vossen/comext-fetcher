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

Parquet naming:
- Products + historical: `comext_YYYYMM.parquet`
- Transport HS: same base name as the archive.

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
| `from_year` | Earliest year to include (default: `2002`). |
| `to_year` | Latest year to include (default: all). |
| `max_workers` | Parallel downloads and parquet conversions (default: `6`). |
| `data_groups` | Map of data groups to booleans (`products`, `historical`, `transport-hs`). |
| `dry_run` | List matching files without downloading them (can be overridden by `--dry-run`). |
| `verbose` | Enable debug-level logging (can be overridden by `--verbose`). |

## Column Selection and Historical Alignment
- Products parquet output keeps only: `REPORTER`, `PARTNER`, `TRADE_TYPE`, `PRODUCT_NC`, `FLOW`, `STAT_PROCEDURE`, `PERIOD`, `VALUE_EUR`, `QUANTITY_KG`.
- Historical parquet output is mapped into the same schema using the specified field mapping.
- Historical `PRODUCT_NC` values with non-numeric suffixes are normalized by keeping the numeric prefix and padding the remainder to 8 chars with `X` (e.g. `99RRR100` -> `99XXXXXX`).
- Transport parquet output preserves all columns from the source file.

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
