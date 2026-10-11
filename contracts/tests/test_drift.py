# Run from contracts/:  python -m pytest
"""The drift comparison (no AWS: Glue's answer is given as data)."""

from pathlib import Path

import yaml

from drift import contract_table, current_columns, drift

CONTRACTS = Path(__file__).resolve().parent.parent


def col(name, current=None):
    c = {"Name": name, "Type": "string"}
    if current is not None:
        c["Parameters"] = {"iceberg.field.current": current}
    return c


def test_no_drift():
    assert drift({"id", "name"}, {"id", "name"}) == (set(), set())


def test_a_new_column_in_the_table_is_drift():
    # What datacontract-cli does not see (P7 step 0).
    assert drift({"id"}, {"id", "gift_wrap"}) == ({"gift_wrap"}, set())


def test_a_field_missing_from_the_table_is_drift():
    assert drift({"id", "gone"}, {"id"}) == (set(), {"gone"})


def test_columns_iceberg_marks_dropped_are_ignored():
    columns = [col("id", "true"), col("old", "false"), col("plain")]
    assert current_columns(columns) == {"id", "plain"}


def test_every_contract_names_its_glue_table():
    # One table per contract, in the Glue database of its layer.
    for path in CONTRACTS.glob("*/*.odcs.yaml"):
        database, table, fields = contract_table(yaml.safe_load(path.read_text()))
        assert database == f"thelook_{path.parent.name}"
        assert table == path.name.removesuffix(".odcs.yaml")
        assert fields
