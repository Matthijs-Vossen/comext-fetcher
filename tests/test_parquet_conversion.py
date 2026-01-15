from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fetcher import parquet as parquet_module
from fetcher.models import DownloadTarget


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_normalize_product_nc_value() -> None:
    normalize = parquet_module._normalize_product_nc_value

    assert normalize("01011990") == "01011990"
    assert normalize("99RRR100") == "99XXXXXX"
    assert normalize("12AB") == "12XXXXXX"
    assert normalize(None) is None


def test_historical_conversion_maps_columns(tmp_path: Path) -> None:
    dat_path = tmp_path / "historical.dat"
    parquet_path = tmp_path / "historical.parquet"

    content = (
        "DECLARANT,DECLARANT_ISO,PARTNER,PARTNER_ISO,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,"
        "PRODUCT_cpa2002,PRODUCT_cpa2008,PRODUCT_CPA2_1,PRODUCT_BEC,PRODUCT_SECTION,FLOW,"
        "STAT_REGIME,SUPP_UNIT,PERIOD,VALUE_IN_EUROS,QUANTITY_IN_KG,SUP_QUANTITY\n"
        "001,FR,0003,NL,I,99RRR100,00151,0122,0143,.....,111,01,1,1,A,200101,2580,600,1\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="historical")
    table = pq.read_table(parquet_path)

    assert table.schema.names == parquet_module.PRODUCT_OUTPUT_COLUMNS
    assert table["REPORTER"].to_pylist() == ["FR"]
    assert table["PARTNER"].to_pylist() == ["NL"]
    assert table["TRADE_TYPE"].to_pylist() == ["I"]
    assert table["PRODUCT_NC"].to_pylist() == ["99XXXXXX"]
    assert table["FLOW"].to_pylist() == ["1"]
    assert table["STAT_PROCEDURE"].to_pylist() == ["1"]
    assert table["PERIOD"].to_pylist() == [200101]
    assert table["VALUE_EUR"].to_pylist() == [2580]
    assert table["QUANTITY_KG"].to_pylist() == [600]

    assert table.schema.field("PERIOD").type == pa.int32()
    assert table.schema.field("VALUE_EUR").type == pa.float64()
    assert table.schema.field("QUANTITY_KG").type == pa.int64()


def test_products_conversion_filters_columns(tmp_path: Path) -> None:
    dat_path = tmp_path / "products.dat"
    parquet_path = tmp_path / "products.parquet"

    content = (
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,4739,4739,2000,0\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="products")
    table = pq.read_table(parquet_path)

    assert table.schema.names == parquet_module.PRODUCT_OUTPUT_COLUMNS
    assert table["REPORTER"].to_pylist() == ["AT"]
    assert table["PARTNER"].to_pylist() == ["AD"]
    assert table["TRADE_TYPE"].to_pylist() == ["E"]
    assert table["PRODUCT_NC"].to_pylist() == ["18063100"]
    assert table["FLOW"].to_pylist() == ["2"]
    assert table["STAT_PROCEDURE"].to_pylist() == ["1"]
    assert table["PERIOD"].to_pylist() == [200201]
    assert table["VALUE_EUR"].to_pylist() == [4739]
    assert table["QUANTITY_KG"].to_pylist() == [2000]


def test_stat_procedure_normalization(tmp_path: Path) -> None:
    dat_path = tmp_path / "products_stat.dat"
    parquet_path = tmp_path / "products_stat.parquet"

    content = (
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,5,NO_SU,200201,4739,4739,2000,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,6,NO_SU,200201,4739,4739,2000,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,7,NO_SU,200201,4739,4739,2000,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,4739,4739,2000,0\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="products")
    table = pq.read_table(parquet_path)

    data = table.to_pydict()
    totals = {
        stat: (value, qty)
        for stat, value, qty in zip(
            data["STAT_PROCEDURE"], data["VALUE_EUR"], data["QUANTITY_KG"]
        )
    }

    assert totals == {
        "1": (4739.0, 2000),
        "2": (9478.0, 4000),
        "3": (4739.0, 2000),
    }


def test_stat_procedure_aggregates_across_batches(tmp_path: Path) -> None:
    dat_path = tmp_path / "products_stat_batches.dat"
    parquet_path = tmp_path / "products_stat_batches.parquet"

    rows = []
    rows.append(
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT"
    )
    rows.extend(
        [
            "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,5,NO_SU,200201,1,1,10,0",
            "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,6,NO_SU,200201,2,2,20,0",
        ]
    )
    rows.extend(
        [
            "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,5,NO_SU,200201,3,3,30,0",
            "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,7,NO_SU,200201,4,4,40,0",
        ]
    )
    _write_text(dat_path, "\n".join(rows) + "\n")

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="products")
    table = pq.read_table(parquet_path)

    data = table.to_pydict()
    totals = {
        stat: (value, qty)
        for stat, value, qty in zip(
            data["STAT_PROCEDURE"], data["VALUE_EUR"], data["QUANTITY_KG"]
        )
    }

    assert totals == {
        "2": (6.0, 60),
        "3": (4.0, 40),
    }


def test_stat_procedure_mixed_rows_aggregate_and_keep(tmp_path: Path) -> None:
    dat_path = tmp_path / "products_stat_mixed.dat"
    parquet_path = tmp_path / "products_stat_mixed.parquet"

    content = (
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,5,NO_SU,200201,100,100,10,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,200,200,20,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,2,NO_SU,200201,300,300,30,0\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="products")
    table = pq.read_table(parquet_path)

    data = table.to_pydict()
    totals = {
        stat: (value, qty)
        for stat, value, qty in zip(
            data["STAT_PROCEDURE"], data["VALUE_EUR"], data["QUANTITY_KG"]
        )
    }

    assert totals == {
        "1": (200.0, 20),
        "2": (400.0, 40),
    }


def test_historical_duplicate_keys_are_aggregated(tmp_path: Path) -> None:
    dat_path = tmp_path / "historical_dups.dat"
    parquet_path = tmp_path / "historical_dups.parquet"

    content = (
        "DECLARANT,DECLARANT_ISO,PARTNER,PARTNER_ISO,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,"
        "PRODUCT_cpa2002,PRODUCT_cpa2008,PRODUCT_CPA2_1,PRODUCT_BEC,PRODUCT_SECTION,FLOW,"
        "STAT_REGIME,SUPP_UNIT,PERIOD,VALUE_IN_EUROS,QUANTITY_IN_KG,SUP_QUANTITY\n"
        "001,FR,0003,NL,I,08112059,00151,0122,0143,.....,111,01,1,1,A,199205,100,10,1\n"
        "001,FR,0090,NL,I,08112059,00151,0122,0143,.....,111,01,1,1,A,199205,200,20,1\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(dat_path, parquet_path, group="historical")
    table = pq.read_table(parquet_path)

    assert table.num_rows == 1
    assert table["VALUE_EUR"].to_pylist() == [300.0]
    assert table["QUANTITY_KG"].to_pylist() == [30]


def test_drop_confidential_rows(tmp_path: Path) -> None:
    dat_path = tmp_path / "products_confidential.dat"
    parquet_path = tmp_path / "products_confidential.parquet"

    content = (
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT\n"
        "AT,AD,E,11XXXXXX,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,10,10,1,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,20,20,2,0\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(
        dat_path,
        parquet_path,
        group="products",
        drop_confidential=True,
    )
    table = pq.read_table(parquet_path)

    assert table["PRODUCT_NC"].to_pylist() == ["18063100"]


def test_aggregate_confidential_duplicates_when_kept(tmp_path: Path) -> None:
    dat_path = tmp_path / "products_confidential_dupes.dat"
    parquet_path = tmp_path / "products_confidential_dupes.parquet"

    content = (
        "REPORTER,PARTNER,TRADE_TYPE,PRODUCT_NC,PRODUCT_SITC,PRODUCT_CPA21,PRODUCT_CPA22,"
        "PRODUCT_BEC,PRODUCT_BEC5,PRODUCT_SECTION,FLOW,STAT_PROCEDURE,SUPPL_UNIT,PERIOD,"
        "VALUE_EUR,VALUE_NAC,QUANTITY_KG,QUANTITY_SUPPL_UNIT\n"
        "AT,AD,E,11XXXXXX,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,10,10,1,0\n"
        "AT,AD,E,11XXXXXX,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,20,20,2,0\n"
        "AT,AD,E,18063100,07330,1082,1082,122,.....,04,2,1,NO_SU,200201,40,40,4,0\n"
    )
    _write_text(dat_path, content)

    parquet_module._write_parquet_from_dat(
        dat_path,
        parquet_path,
        group="products",
        drop_confidential=False,
    )
    table = pq.read_table(parquet_path)

    data = table.to_pydict()
    keyed = {
        (r, p, t, prod, f, stat, period): (value, qty)
        for r, p, t, prod, f, stat, period, value, qty in zip(
            data["REPORTER"],
            data["PARTNER"],
            data["TRADE_TYPE"],
            data["PRODUCT_NC"],
            data["FLOW"],
            data["STAT_PROCEDURE"],
            data["PERIOD"],
            data["VALUE_EUR"],
            data["QUANTITY_KG"],
        )
    }

    assert keyed == {
        ("AT", "AD", "E", "11XXXXXX", "2", "1", 200201): (30.0, 3),
        ("AT", "AD", "E", "18063100", "2", "1", 200201): (40.0, 4),
    }


def test_annual_aggregation_products_like(tmp_path: Path) -> None:
    monthly_root = tmp_path / "monthly"
    annual_root = tmp_path / "annual"
    monthly_root.mkdir()
    annual_root.mkdir()

    table_202001 = pa.table(
        {
            "REPORTER": ["AT", "AT"],
            "PARTNER": ["AD", "AD"],
            "TRADE_TYPE": ["E", "E"],
            "PRODUCT_NC": ["18063100", "18063100"],
            "FLOW": ["2", "2"],
            "STAT_PROCEDURE": ["1", "2"],
            "PERIOD": [202001, 202001],
            "VALUE_EUR": [10.0, 20.0],
            "QUANTITY_KG": [1, 2],
        }
    )
    table_202002 = pa.table(
        {
            "REPORTER": ["AT"],
            "PARTNER": ["AD"],
            "TRADE_TYPE": ["E"],
            "PRODUCT_NC": ["18063100"],
            "FLOW": ["2"],
            "STAT_PROCEDURE": ["1"],
            "PERIOD": [202002],
            "VALUE_EUR": [30.0],
            "QUANTITY_KG": [3],
        }
    )
    table_202101 = pa.table(
        {
            "REPORTER": ["AT"],
            "PARTNER": ["AD"],
            "TRADE_TYPE": ["E"],
            "PRODUCT_NC": ["18063100"],
            "FLOW": ["2"],
            "STAT_PROCEDURE": ["1"],
            "PERIOD": [202101],
            "VALUE_EUR": [40.0],
            "QUANTITY_KG": [4],
        }
    )

    pq.write_table(table_202001, monthly_root / "comext_202001.parquet")
    pq.write_table(table_202002, monthly_root / "comext_202002.parquet")
    pq.write_table(table_202101, monthly_root / "comext_202101.parquet")

    targets = [
        DownloadTarget(
            group="products",
            dir_path="",
            name="full_v2_202001.7z",
            size=None,
            yyyymm="202001",
        ),
        DownloadTarget(
            group="products",
            dir_path="",
            name="full_v2_202002.7z",
            size=None,
            yyyymm="202002",
        ),
        DownloadTarget(
            group="products",
            dir_path="",
            name="full_v2_202101.7z",
            size=None,
            yyyymm="202101",
        ),
    ]

    stats = parquet_module.aggregate_targets_to_annual(
        targets,
        monthly_root,
        annual_root,
        max_workers=2,
        group="products",
        logger_=parquet_module.logger,
    )

    assert stats.aggregated == 2

    annual_2020 = pq.read_table(annual_root / "comext_2020.parquet")
    annual_2021 = pq.read_table(annual_root / "comext_2021.parquet")

    data_2020 = annual_2020.to_pydict()
    assert data_2020["PERIOD"] == [2020]
    assert data_2020["VALUE_EUR"] == [60.0]
    assert data_2020["QUANTITY_KG"] == [6]

    data_2021 = annual_2021.to_pydict()
    assert data_2021["PERIOD"] == [2021]
    assert data_2021["VALUE_EUR"] == [40.0]
    assert data_2021["QUANTITY_KG"] == [4]


def test_annual_aggregation_multiple_keys_and_confidential(tmp_path: Path) -> None:
    monthly_root = tmp_path / "monthly"
    monthly_no_conf_root = tmp_path / "monthly_no_conf"
    annual_root = tmp_path / "annual"
    annual_no_conf_root = tmp_path / "annual_no_conf"
    monthly_root.mkdir()
    monthly_no_conf_root.mkdir()
    annual_root.mkdir()
    annual_no_conf_root.mkdir()

    table_201901 = pa.table(
        {
            "REPORTER": ["AT", "AT"],
            "PARTNER": ["AD", "AD"],
            "TRADE_TYPE": ["E", "E"],
            "PRODUCT_NC": ["18063100", "11XXXXXX"],
            "FLOW": ["2", "2"],
            "STAT_PROCEDURE": ["1", "2"],
            "PERIOD": [201901, 201901],
            "VALUE_EUR": [10.0, 20.0],
            "QUANTITY_KG": [1, 2],
        }
    )
    table_201902 = pa.table(
        {
            "REPORTER": ["AT", "BE"],
            "PARTNER": ["AD", "FR"],
            "TRADE_TYPE": ["E", "I"],
            "PRODUCT_NC": ["18063100", "18063100"],
            "FLOW": ["2", "1"],
            "STAT_PROCEDURE": ["1", "1"],
            "PERIOD": [201902, 201902],
            "VALUE_EUR": [30.0, 40.0],
            "QUANTITY_KG": [3, 4],
        }
    )

    pq.write_table(table_201901, monthly_root / "comext_201901.parquet")
    pq.write_table(table_201902, monthly_root / "comext_201902.parquet")
    mask_conf = pc.match_substring(table_201901["PRODUCT_NC"], "X")
    pq.write_table(
        table_201901.filter(pc.invert(mask_conf)),
        monthly_no_conf_root / "comext_201901.parquet",
    )
    pq.write_table(table_201902, monthly_no_conf_root / "comext_201902.parquet")

    targets = [
        DownloadTarget(
            group="products",
            dir_path="",
            name="full_v2_201901.7z",
            size=None,
            yyyymm="201901",
        ),
        DownloadTarget(
            group="products",
            dir_path="",
            name="full_v2_201902.7z",
            size=None,
            yyyymm="201902",
        ),
    ]

    parquet_module.aggregate_targets_to_annual(
        targets,
        monthly_root,
        annual_root,
        max_workers=2,
        group="products",
        logger_=parquet_module.logger,
    )
    parquet_module.aggregate_targets_to_annual(
        targets,
        monthly_no_conf_root,
        annual_no_conf_root,
        max_workers=2,
        group="products",
        logger_=parquet_module.logger,
    )

    annual_all = pq.read_table(annual_root / "comext_2019.parquet")
    annual_no_conf = pq.read_table(annual_no_conf_root / "comext_2019.parquet")

    all_data = annual_all.to_pydict()
    all_totals = {
        (r, p, t, prod, f): (value, qty)
        for r, p, t, prod, f, value, qty in zip(
            all_data["REPORTER"],
            all_data["PARTNER"],
            all_data["TRADE_TYPE"],
            all_data["PRODUCT_NC"],
            all_data["FLOW"],
            all_data["VALUE_EUR"],
            all_data["QUANTITY_KG"],
        )
    }
    assert all_totals == {
        ("AT", "AD", "E", "18063100", "2"): (40.0, 4),
        ("AT", "AD", "E", "11XXXXXX", "2"): (20.0, 2),
        ("BE", "FR", "I", "18063100", "1"): (40.0, 4),
    }

    no_conf_data = annual_no_conf.to_pydict()
    no_conf_totals = {
        (r, p, t, prod, f): (value, qty)
        for r, p, t, prod, f, value, qty in zip(
            no_conf_data["REPORTER"],
            no_conf_data["PARTNER"],
            no_conf_data["TRADE_TYPE"],
            no_conf_data["PRODUCT_NC"],
            no_conf_data["FLOW"],
            no_conf_data["VALUE_EUR"],
            no_conf_data["QUANTITY_KG"],
        )
    }
    assert no_conf_totals == {
        ("AT", "AD", "E", "18063100", "2"): (40.0, 4),
        ("BE", "FR", "I", "18063100", "1"): (40.0, 4),
    }
