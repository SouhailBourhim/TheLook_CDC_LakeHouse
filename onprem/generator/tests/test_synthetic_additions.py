# Added in thelook-cdc-lakehouse: tests for our changes to the vendored
# generator (MongoDB documents, cart product/price, basket affinity).
# Run from onprem/generator:  python -m pytest
import datetime

import pytest
from faker import Faker

from src import models
from src.models import PRODUCT_MAP, Event, Order, OrderItem, User, pick_affinity_product
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
