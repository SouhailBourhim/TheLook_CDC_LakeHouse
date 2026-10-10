"""Gold -> co-purchase graph rows (spec FR16, ADR 019).

Two pure transformations, written to Neo4j by lakehouse.graph_writer:

  product_nodes      one row per product of dim_product (its properties)
  co_purchase_pairs  one row per unordered pair of products bought in the
                     same order, with the number of such orders (weight)

The whole graph is recomputed on every run from fct_order_items: an erased
order (FR9) must lower the weights, which adding new orders' pairs to the
old weights could never do.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NODE_COLUMNS = ["product_id", "name", "category", "brand", "department"]


def product_nodes(products: DataFrame) -> DataFrame:
    """Every product becomes a node, bought or not, so the API can tell a
    product with no co-purchase yet (200, []) from an unknown id (404)."""
    return products.select(*NODE_COLUMNS).where(F.col("product_id").isNotNull())


def co_purchase_pairs(items: DataFrame) -> DataFrame:
    """product_a < product_b, weight = number of distinct orders that contain
    both products.

    Every order counts, whatever its status: the basket is chosen when the
    order is placed; a cancellation or a return comes later, for reasons
    unrelated to which products go together. One row per unordered pair (the
    lower id first): the graph stores one relationship per pair and is read
    in both directions.
    """
    # One row per product per order: two items of the same product (or
    # quantity > 1) must not make a pair with itself or count an order twice.
    baskets = (
        items.where(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
        .select("order_id", "product_id")
        .distinct()
    )
    a, b = baskets.alias("a"), baskets.alias("b")
    return (
        a.join(
            b,
            (F.col("a.order_id") == F.col("b.order_id"))
            & (F.col("a.product_id") < F.col("b.product_id")),
        )
        .groupBy(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
        )
        # After the distinct, each joined row is one order for this pair.
        .agg(F.count(F.lit(1)).alias("weight"))
    )
