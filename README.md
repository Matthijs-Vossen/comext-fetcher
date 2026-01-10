# Eurostat Comext Fetcher

`fetcher` is a focused downloader for the Eurostat Comext bulk dataset. It queries the public dissemination API, fetches monthly archives for PRODUCTS, the 1988-2001 historical backfill, and TRANSPORT_HS, then verifies local monthly coverage.

## Usage
```bash
python -m fetcher path/to/config.json [--dry-run] [--verbose]
```

The config file describes the run settings. JSON and TOML are supported.

Full example config: `config/main.json`.

## Config Settings
| Key | Description |
| --- | --- |
| `dest` | Base destination. Overrides `dest_products` and `dest_historical`. |
| `dest_products` | Destination for PRODUCTS files (default: `data/comext/products`). |
| `dest_historical` | Destination for HISTORICAL files (default: `data/comext/historical`). |
| `dest_transport_hs` | Destination for TRANSPORT_HS files (default: `data/comext/transport_hs`). |
| `from_year` | Earliest year to include (default: `2002`). |
| `to_year` | Latest year to include (default: all). |
| `max_workers` | Parallel downloads (default: `6`). |
| `data_groups` | Map of data groups to booleans (`products`, `historical`, `transport-hs`). |
| `dry_run` | List matching files without downloading them (can be overridden by `--dry-run`). |
| `verbose` | Enable debug-level logging (can be overridden by `--verbose`). |

## Typical Workflows
- Discover matching files before downloading:
  ```bash
  python -m fetcher config/main.json --dry-run
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
