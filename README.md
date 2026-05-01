# Eurostat Comext Fetcher

CLI utility for fetching, validating, and converting Eurostat Comext bulk data
to Parquet.

The tool queries Eurostat's public dissemination file API, downloads selected
Comext `.7z` archives, converts `.dat` contents to Parquet, and verifies local
monthly coverage. It was originally developed as upstream data preparation for
Comext CN harmonisation workflows such as
[`comext-harmonisation`](https://github.com/Matthijs-Vossen/comext-harmonisation).

## Install

```bash
python -m pip install -e '.[dev]'
```

Python 3.10 or newer is required.

## Quickstart

Inspect matching files without downloading:

```bash
comext-fetch configs/example.json --dry-run
```

Run the full products/historical fetch/conversion workflow:

```bash
comext-fetch configs/config.json
```

The `config.json` file is a large workflow. Inspect it with `--dry-run` first
and make sure the destination storage is appropriate before running it without
`--dry-run`. The `transport.json` config is a separate dry-run-oriented example
for the `TRANSPORT_HS` data group:

```bash
comext-fetch configs/transport.json --dry-run
```

## What It Downloads

The fetcher supports three Eurostat Comext bulk groups:

- `products`: monthly `COMEXT_DATA/PRODUCTS` archives;
- `historical`: `COMEXT_HISTORICAL_DATA/PRODUCTS_1988_2001` backfill archives;
- `transport-hs`: monthly `COMEXT_DATA/TRANSPORT_HS` archives.

Configured year ranges and enabled groups determine which files are listed and
downloaded. Existing archives with matching file sizes are skipped, so reruns
are idempotent.

## Output Layout

Default compressed archive locations:

```text
data/compressed/products/
data/compressed/historical/
data/compressed/transport_hs/
```

Default Parquet locations:

```text
data/confidential/extracted/products_like/
data/confidential/extracted/transport_hs/
data/non_confidential/extracted/products_like/
data/non_confidential/extracted/transport_hs/
data/confidential/extracted_annual/products_like/
data/non_confidential/extracted_annual/products_like/
```

Product and historical outputs use names such as:

```text
comext_YYYYMM.parquet
comext_YYYY.parquet
```

Transport-HS Parquet files keep the archive base name.

## Repository Layout

```text
.
├── src/comext_fetcher/ # CLI, API client, downloader, coverage, Parquet conversion
├── configs/            # Example fetch/conversion configs
├── tests/              # Network-free tests
├── pyproject.toml      # Package metadata and tool configuration
└── README.md
```

## Processing Semantics

For PRODUCTS and HISTORICAL inputs, outputs are normalised to:

```text
REPORTER, PARTNER, TRADE_TYPE, PRODUCT_NC, FLOW, STAT_PROCEDURE,
PERIOD, VALUE_EUR, QUANTITY_KG
```

Important processing choices:

- historical columns are mapped to the same products-like schema;
- `STAT_PROCEDURE` codes `5` and `6` are mapped to `2`, and `7` is mapped to
  `3`;
- rows with `PRODUCT_NC == TOTAL` are dropped;
- historical product codes with non-numeric suffixes are normalised to masked
  CN8-like codes;
- duplicate rows introduced by procedure harmonisation or masked-code collapse
  are aggregated by key;
- optional `drop_confidential=true` removes product codes containing `X`;
- annual outputs aggregate monthly products-like Parquet across
  `STAT_PROCEDURE`.

Transport-HS output preserves the source columns, apart from optional
confidential-code filtering and `TOTAL` row removal when applicable.

## Downstream Use

For `comext-harmonisation`, use the products/historical workflow with
`drop_confidential=true` and `output_mode=both`. The relevant fetcher outputs
are:

```text
data/non_confidential/extracted_annual/products_like/
data/non_confidential/extracted/products_like/
```

Copy or symlink those directories into the corresponding
`comext-harmonisation` input paths, or point the harmonisation pipeline config
directly at them.

## Configuration

Example configs live in `configs/`. `example.json` is intentionally small and
dry-run-oriented; `config.json` reflects a full products/historical workflow.

Common settings:

- `from_year`, `to_year`: inclusive year range;
- `data_groups`: enabled data groups;
- `max_workers`: integer worker count or `"auto"`;
- `drop_confidential`: write no-confidential outputs by dropping masked codes;
- `output_mode`: `monthly` or `both`;
- `dry_run`, `verbose`: default CLI behaviour, overridable by flags;
- destination and extracted-output path settings.

Large real-data runs can require substantial disk space and time. Use `--dry-run`
first to inspect the selected files before downloading.

## Development

```bash
python -m pip install -e '.[dev]'
ruff check .
ruff format --check .
python -m pytest -q
```

The test suite is network-free. GitHub Actions runs the same checks on Python
3.10 and 3.12.

## Limitations

- Eurostat endpoint availability and file naming conventions can change.
- The tool prepares Comext data; it does not harmonise product classifications.
- Confidential-code filtering is based on `PRODUCT_NC` containing `X`.
- Full products/historical runs are large-data operations and should be planned
  with local storage constraints in mind.

## License

Code in this repository is released under the MIT License. See `LICENSE`.
