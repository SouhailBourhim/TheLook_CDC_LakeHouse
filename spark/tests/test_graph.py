"""Tests for the co-purchase graph rows (FR16, ADR 019)."""

from chispa import assert_df_equality

from lakehouse.graph import co_purchase_pairs, product_nodes

ITEMS = "order_item_id string, order_id string, product_id bigint, status string"
PAIRS = "product_a bigint, product_b bigint, weight bigint"


def pairs(spark, rows):
    return co_purchase_pairs(spark.createDataFrame(rows, ITEMS))


def expected(spark, rows):
    return spark.createDataFrame(rows, PAIRS)


def check(actual, wanted):
    assert_df_equality(actual, wanted, ignore_row_order=True, ignore_nullable=True)


def test_one_pair_per_unordered_pair_with_the_lower_id_first(spark):
    actual = pairs(
        spark,
        [
            ("i1", "o1", 9, "Delivered"),
            ("i2", "o1", 3, "Delivered"),
            ("i3", "o1", 5, "Delivered"),
        ],
    )
    check(actual, expected(spark, [(3, 5, 1), (3, 9, 1), (5, 9, 1)]))


def test_weight_counts_orders_and_every_status(spark):
    # The same pair in three orders: delivered, cancelled, returned.
    rows = [
        (f"{o}-{p}", o, p, status)
        for o, status in [("o1", "Delivered"), ("o2", "Cancelled"), ("o3", "Returned")]
        for p in (1, 2)
    ]
    check(pairs(spark, rows), expected(spark, [(1, 2, 3)]))


def test_a_product_twice_in_an_order_counts_the_order_once(spark):
    actual = pairs(
        spark,
        [
            ("i1", "o1", 1, "Shipped"),
            ("i2", "o1", 1, "Shipped"),  # same product again: no (1, 1) pair
            ("i3", "o1", 2, "Shipped"),
        ],
    )
    check(actual, expected(spark, [(1, 2, 1)]))


def test_single_item_orders_and_null_keys_make_no_pair(spark):
    actual = pairs(
        spark,
        [
            ("i1", "o1", 1, "Processing"),
            ("i2", "o2", 2, "Processing"),
            ("i3", None, 3, "Processing"),
            ("i4", "o2", None, "Processing"),
        ],
    )
    assert actual.count() == 0


def test_an_order_missing_from_gold_leaves_the_weights(spark):
    # Erasure (FR9) deletes orders from gold; the full recompute lowers the
    # weight instead of keeping what an incremental += had added.
    both = [("i1", "o1", 1, "Delivered"), ("i2", "o1", 2, "Delivered")]
    again = [("i3", "o2", 1, "Delivered"), ("i4", "o2", 2, "Delivered")]
    check(pairs(spark, both + again), expected(spark, [(1, 2, 2)]))
    check(pairs(spark, again), expected(spark, [(1, 2, 1)]))


def test_every_product_is_a_node(spark):
    products = spark.createDataFrame(
        [
            (1, "Tee", "Tops & Tees", "Acme", "Women", "sku1", 5.0),
            (2, "Jeans", "Jeans", "Acme", "Men", "sku2", 9.0),
            (None, "Broken", "Jeans", "Acme", "Men", "sku3", 1.0),
        ],
        "product_id bigint, name string, category string, brand string, "
        "department string, sku string, cost double",
    )
    check(
        product_nodes(products),
        spark.createDataFrame(
            [
                (1, "Tee", "Tops & Tees", "Acme", "Women"),
                (2, "Jeans", "Jeans", "Acme", "Men"),
            ],
            "product_id bigint, name string, category string, brand string, "
            "department string",
        ),
    )
