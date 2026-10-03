# Run from onprem/review-simulator:  python -m pytest
import random
from collections import Counter
from datetime import UTC, datetime

from faker import Faker

from simulator import ACTIONS, RATINGS, TAGS, choose, edit_fields, new_review

fake = Faker()
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
CANDIDATE = {"order_item_id": "oi-1", "product_id": 27395, "user_id": "u-1"}


def test_new_review_points_at_the_delivered_item():
    doc = new_review(CANDIDATE, fake, random.Random(1), NOW)

    assert doc["order_item_id"] == "oi-1"
    assert doc["product_id"] == 27395 and isinstance(doc["product_id"], int)
    assert doc["user_id"] == "u-1"
    assert doc["rating"] in RATINGS
    assert set(doc["tags"]) <= set(TAGS) and len(doc["tags"]) <= 3
    assert doc["helpful_votes"] == 0
    assert doc["created_at"] == doc["updated_at"] == NOW


def test_each_review_gets_its_own_id():
    rng = random.Random(1)
    ids = {new_review(CANDIDATE, fake, rng, NOW)["_id"] for _ in range(100)}
    assert len(ids) == 100


def test_edit_moves_the_rating_by_one_star_within_bounds():
    rng = random.Random(1)
    for rating in (1, 3, 5):
        for _ in range(50):
            new = edit_fields({"rating": rating}, fake, rng, NOW)["rating"]
            assert 1 <= new <= 5
            assert abs(new - rating) <= 1
            assert new != rating or rating in (1, 5)  # clamped at the ends


def test_edit_marks_the_review_as_edited():
    fields = edit_fields({"rating": 4}, fake, random.Random(1), NOW)
    assert fields["edited_at"] == fields["updated_at"] == NOW
    assert set(fields) == {"rating", "text", "edited_at", "updated_at"}


def test_action_mix_follows_the_weights():
    rng = random.Random(42)
    n = 20_000
    counts = Counter(choose(rng, ACTIONS) for _ in range(n))
    for action, share in ACTIONS.items():
        assert abs(counts[action] / n - share) < 0.02
