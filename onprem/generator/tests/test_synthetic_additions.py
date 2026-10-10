# Added in thelook-cdc-lakehouse: tests for our changes to the vendored
# generator (MongoDB documents, cart product/price, basket affinity,
# companions and popularity).
# Run from onprem/generator:  python -m pytest
import collections
import datetime
import hashlib
import random

import pytest
from faker import Faker

from src import models
from src.models import (
    PRODUCT_MAP,
    Event,
    Order,
    OrderItem,
    User,
    companion_products,
    pick_affinity_product,
    pick_order_products,
    popularity_ranking,
)
from src.mongo_writer import event_to_document

fake = Faker()


@pytest.fixture
def user():
    return User.new(country="*", state="*", postal_code="*", fake=fake)


@pytest.fixture
def order_item(user):
    return OrderItem.new(order=Order.new(user=user, fake=fake), fake=fake)


def test_cart_event_carries_the_session_product_and_its_price(user, order_item):
    events = Event.new(
        user=user, order_item=order_item, event_category="purchase", fake=fake
    )

    cart = [e for e in events if e.event_type == "cart"]
    assert len(cart) == 1
    assert cart[0].product_id == int(order_item.product_id)
    assert cart[0].price == float(PRODUCT_MAP[order_item.product_id]["retail_price"])
    assert isinstance(cart[0].price, float)  # not the CSV string
    assert all(
        e.product_id is None and e.price is None
        for e in events
        if e.event_type != "cart"
    )


def test_cancel_session_works_with_an_int_product_id(user, order_item):
    # Items re-read from PostgreSQL (order updates) have an int product_id,
    # while PRODUCT_MAP is keyed by strings: this raised KeyError once.
    order_item.product_id = int(order_item.product_id)
    events = Event.new(
        user=user, order_item=order_item, event_category="cancel", fake=fake
    )

    assert [e.event_type for e in events] == ["product", "cart", "cancel"]
    assert events[1].product_id == order_item.product_id


def test_ghost_sessions_have_no_cart_events():
    # The cart-value feature relies on cart events always belonging to an
    # order item, so ghost sessions must never produce one.
    for _ in range(300):
        events = Event.new(
            user=None, order_item=None, event_category="ghost", fake=fake
        )
        assert "cart" not in {e.event_type for e in events}


def test_event_document_uses_id_as_primary_key_and_omits_nulls():
    event = Event(
        id="e-1",
        user_id=None,
        sequence_number=1,
        session_id="s-1",
        ip_address="10.0.0.1",
        city="c",
        state="s",
        postal_code="p",
        browser="Chrome",
        traffic_source="Email",
        uri="/home",
        event_type="home",
        created_at=datetime.datetime(2026, 10, 3, 12, 0),
    )
    doc = event_to_document(event)

    assert doc["_id"] == "e-1"
    assert "id" not in doc
    assert "user_id" not in doc and "product_id" not in doc and "price" not in doc
    assert doc["created_at"] == datetime.datetime(2026, 10, 3, 12, 0)


def test_affinity_picks_from_the_first_items_category_and_skips_chosen_products():
    first = next(iter(PRODUCT_MAP))
    chosen = {first}
    for _ in range(50):
        pick = pick_affinity_product(first, chosen)
        assert PRODUCT_MAP[pick]["category"] == PRODUCT_MAP[first]["category"]
        assert pick not in chosen


def test_affinity_returns_none_when_the_category_is_exhausted(monkeypatch):
    monkeypatch.setattr(models, "PRODUCTS_BY_CATEGORY", {"jeans": ["1", "2"]})
    monkeypatch.setattr(
        models, "PRODUCT_MAP", {"1": {"category": "jeans"}, "2": {"category": "jeans"}}
    )

    assert pick_affinity_product(1, {"1"}) == "2"  # int id accepted
    assert pick_affinity_product("1", {"1", "2"}) is None


# --- companions and popularity (ADR 019) ---------------------------------------


def test_companions_are_fixed_and_follow_the_documented_hash_rule():
    # The P6 drill recomputes companions from this rule: pin it. Changing it
    # silently would make every past pair weight point at other products.
    assert companion_products("1", 5) == ("16717", "17020", "691", "17049", "661")
    category = PRODUCT_MAP["1"]["category"]
    by_rule = sorted(
        (p for p, v in PRODUCT_MAP.items() if v["category"] == category and p != "1"),
        key=lambda c: hashlib.sha256(f"1:{c}".encode()).digest(),
    )[:5]
    assert list(companion_products(1, 5)) == by_rule  # int id accepted


def test_companions_come_from_the_category_and_exclude_the_product_itself():
    for pid in random.Random(1).sample(sorted(PRODUCT_MAP), 200):
        companions = companion_products(pid, 5)
        assert len(set(companions)) == 5
        assert pid not in companions
        assert {PRODUCT_MAP[c]["category"] for c in companions} == {
            PRODUCT_MAP[pid]["category"]
        }


def test_affinity_with_companions_picks_an_unused_companion_or_none():
    companions = set(companion_products("1", 5))
    chosen = {"1", *sorted(companions)[:4]}
    assert (
        pick_affinity_product("1", chosen, companions=5) == (companions - chosen).pop()
    )
    assert pick_affinity_product("1", chosen | companions, companions=5) is None


def test_first_items_follow_the_zipf_shares():
    # 1/rank^0.8 over 29,120 products: top product 2.9 %, top 20 13.6 %.
    random.seed(7)
    ranked, _ = popularity_ranking(0.8)
    draws = collections.Counter(
        pick_order_products(1, affinity_prob=0.6, companions=5, skew=0.8)[0]
        for _ in range(100_000)
    )
    assert 0.026 < draws[ranked[0]] / 100_000 < 0.032
    assert 0.128 < sum(draws[p] for p in ranked[:20]) / 100_000 < 0.144


def test_later_items_are_companions_at_the_affinity_rate():
    random.seed(7)
    hits = later = 0
    for _ in range(5_000):
        first, *rest = pick_order_products(4, affinity_prob=0.6, companions=5, skew=0.8)
        later += len(rest)
        hits += sum(p in companion_products(first, 5) for p in rest)
    assert 0.58 < hits / later < 0.62  # uniform picks are almost never companions


def test_skew_zero_and_affinity_zero_give_the_upstream_behaviour():
    # Uniform first item: no product above a few draws in 20,000.
    random.seed(7)
    orders = [
        pick_order_products(2, affinity_prob=0.0, companions=5, skew=0.0)
        for _ in range(20_000)
    ]
    assert max(collections.Counter(o[0] for o in orders).values()) < 8
    # Second item uniform: a companion by chance in ~0.03 % of orders
    # (companions are computed per product, so only 2,000 are checked).
    assert sum(o[1] in companion_products(o[0], 5) for o in orders[:2_000]) < 3
