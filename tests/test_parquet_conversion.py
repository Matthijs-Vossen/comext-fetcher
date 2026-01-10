from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fetcher import parquet as parquet_module


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
    assert table.schema.field("VALUE_EUR").type == pa.int64()
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
