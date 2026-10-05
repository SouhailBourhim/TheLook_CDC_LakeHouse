"""Tests for the gold dimensions (ADR 015), above all dim_user (SCD2)."""

import datetime

from pyspark.sql import Row

from lakehouse.gold import OPEN_END, USER_COLUMNS, dim_date, dim_user

USER = (
    "struct<id:string,first_name:string,last_name:string,email:string,age:int,"
    "gender:string,street_address:string,postal_code:string,city:string,state:string,"
    "country:string,latitude:double,longitude:double,traffic_source:string,"
    "created_at:bigint,updated_at:bigint>"
)
BRONZE_USERS = f"op string, lsn bigint, kafka_offset bigint, source_ts timestamp, before {USER}, after {USER}"
CREATED = datetime.datetime(2026, 9, 30, 8, 0)
CREATED_US = int(CREATED.replace(tzinfo=datetime.timezone.utc).timestamp() * 1_000_000)


def at(hour, minute=0):
    return datetime.datetime(2026, 10, 4, hour, minute)


def user(city="Casablanca", street="1 Rue A"):
    return Row(
        id="u1", first_name="Amina", last_name="B", email="a@example.com", age=30,
        gender="F", street_address=street, postal_code="20000", city=city, state="CS",
        country="Morocco", latitude=33.5, longitude=-7.6, traffic_source="Search",
        created_at=CREATED_US, updated_at=CREATED_US,
    )  # fmt: skip


def event(op, lsn, ts, city="Casablanca", offset=None):
    image = user(city)
    key_only = Row(**{k: ("u1" if k == "id" else None) for k in image.asDict()})
    return (
        op, lsn, offset if offset is not None else lsn, ts,
        key_only if op == "d" else None, None if op == "d" else image,
    )  # fmt: skip


def versions(spark, events):
    """The real versions (the unknown member, version 0, is tested apart)."""
    df = spark.createDataFrame(events, BRONZE_USERS)
    return sorted(
        (r for r in dim_user(df).collect() if r.version > 0), key=lambda r: r.version
    )


# --- dim_user ------------------------------------------------------------------


def test_first_version_starts_at_created_at(spark):
    # History before bronze began is lost (Kafka retention): an order placed
    # before the first bronze event must still find a version (ADR 015).
    (v,) = versions(spark, [event("r", 100, at(10))])
    assert v.valid_from == CREATED
    assert v.valid_to == OPEN_END and v.is_current


def test_an_address_change_closes_the_old_version(spark):
    v1, v2 = versions(
        spark, [event("r", 100, at(10)), event("u", 200, at(12), "Rabat")]
    )
    assert (v1.city, v1.valid_to, v1.is_current) == ("Casablanca", at(12), False)
    assert (v2.city, v2.valid_from, v2.valid_to, v2.is_current) == (
        "Rabat",
        at(12),
        OPEN_END,
        True,
    )


def test_no_op_updates_and_repeated_snapshot_rows_make_no_version(spark):
    # The generator rewrites rows without changing them, and a blocking
    # snapshot repeats the current state: neither is a new version.
    vs = versions(
        spark,
        [
            event("c", 100, at(9)),
            event("u", 150, at(10)),  # same values
            event("r", 300, at(11)),  # snapshot of the same state
            event("u", 400, at(12), "Rabat"),
            event("u", 500, at(13), "Rabat"),  # same values again
        ],
    )
    assert [(v.city, v.valid_from) for v in vs] == [
        ("Casablanca", CREATED),
        ("Rabat", at(12)),
    ]


def test_events_arriving_out_of_order_are_sorted_by_lsn(spark):
    v1, v2 = versions(spark, [event("u", 200, at(12), "Rabat"), event("c", 100, at(9))])
    assert (v1.city, v2.city) == ("Casablanca", "Rabat")


def test_a_delete_closes_the_last_version_and_leaves_no_current_row(spark):
    v1, v2 = versions(
        spark,
        [
            event("c", 100, at(9)),
            event("u", 200, at(12), "Rabat"),
            event("d", 300, at(15)),
        ],
    )
    assert v2.valid_to == at(15) and not v2.is_current
    assert not v1.is_current


def test_versions_cover_time_without_gaps_or_overlaps(spark):
    vs = versions(
        spark,
        [
            event("c", 100, at(9)),
            event("u", 200, at(12), "Rabat"),
            event("u", 300, at(14), "Fes"),
            event("u", 400, at(16), "Tanger"),
        ],
    )
    for older, newer in zip(vs, vs[1:], strict=False):
        assert older.valid_to == newer.valid_from
    assert sum(v.is_current for v in vs) == 1


def test_surrogate_keys_are_unique_and_identical_on_every_rebuild(spark):
    events = [event("c", 100, at(9)), event("u", 200, at(12), "Rabat")]
    first = [v.user_sk for v in versions(spark, events)]
    second = [v.user_sk for v in versions(spark, list(reversed(events)))]
    assert first == second and len(set(first)) == 2


def test_dim_user_keeps_every_tracked_column(spark):
    (v,) = versions(spark, [event("r", 100, at(10))])
    assert all(getattr(v, c) is not None for c in USER_COLUMNS)


def test_one_unknown_member_row_for_unresolved_facts(spark):
    df = spark.createDataFrame([event("r", 100, at(10))], BRONZE_USERS)
    unknown = [r for r in dim_user(df).collect() if r.user_sk == -1]
    assert len(unknown) == 1
    (u,) = unknown
    assert (u.city, u.is_current, u.valid_to) == ("Unknown", False, OPEN_END)


# --- dim_date ------------------------------------------------------------------


def test_dim_date_is_a_contiguous_calendar(spark):
    days = dim_date(spark, "2026-02-27", "2026-03-02").orderBy("date").collect()
    assert [d.date_key for d in days] == [20260227, 20260228, 20260301, 20260302]
    assert [d.is_weekend for d in days] == [False, True, True, False]  # Sat 28, Sun 1
    assert days[0].day_name == "Friday" and days[0].quarter == 1
