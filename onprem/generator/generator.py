# Modified in thelook-cdc-lakehouse (see NOTICE): --db-password defaults to
# the DB_PASSWORD environment variable; the password is redacted in the logs.
# Clickstream events go to MongoDB (src/mongo_writer.py) instead of
# PostgreSQL; synthetic basket affinity and popularity (--basket-affinity-prob,
# --companions, --popularity-skew; spec 4.3, ADR 019). Load mode (P9):
# deadline pacing, --workers, random rows through the primary key instead of
# ORDER BY RANDOM().
import argparse
import asyncio
import copy
import os
import random
import logging
import uuid

from faker import Faker
from pymongo.errors import PyMongoError
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from src.db_writer import DataWriter
from src.mongo_writer import EventWriter
from src.models import (
    User,
    Order,
    OrderItem,
    Event,
    OrderStatus,
    EventCategory,
    pick_order_products,
)
from src.utils import generate_from_csv

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


# More than this behind schedule, the backlog is dropped instead of being
# caught up in a burst (and counted, so an overloaded generator shows).
MAX_BEHIND_S = 1.0


def next_deadline(
    next_at: float, now: float, interval: float
) -> tuple[float, float, float]:
    """Pacing on a schedule (open loop): the next iteration is due
    `interval` after the previous due time, not after the previous work
    ended, so the rate does not fall as the work slows down. Returns
    (due time, seconds to sleep, seconds of backlog dropped).

    Upstream slept before the work, so its rate was 1 / (1/target + work
    time): ~39 ms of work capped it near 25 iterations/s (P1 baseline)."""
    due = next_at + interval
    if now - due > MAX_BEHIND_S:
        return now, 0.0, now - due
    return due, max(0.0, due - now), 0.0


class TheLookECommSimulator:
    """
    Manages the state and execution of the e-commerce data generation simulation.
    """

    def __init__(self, args: argparse.Namespace, fake: Faker):
        """Initializes the simulator and the DataWriter."""
        self.args = args
        self.fake = fake
        self.writer = DataWriter(
            args.db_user,
            args.db_password,
            args.db_host,
            args.db_name,
            args.db_schema,
            args.db_batch_size,
        )
        self.events = EventWriter(
            args.mongo_user, args.mongo_password, args.mongo_host, args.mongo_db
        )
        self.consecutive_db_errors = 0
        self.max_consecutive_errors = 3

    async def initialize(self):
        """
        Performs the one-time setup for the simulation.
        This includes creating database tables, loading initial data, and creating Kafka topics.
        """
        try:
            logging.info("Setting up database schema...")
            await asyncio.to_thread(self.writer.create_tables_if_not_exists)

            logging.info("Writing initial data...")
            init_users = [
                User.new(
                    country=self.args.country,
                    state=self.args.state,
                    postal_code=self.args.postal_code,
                    fake=self.fake,
                )
                for _ in range(self.args.init_num_users)
            ]

            logging.info(f"Writing {len(init_users)} initial users...")
            await asyncio.to_thread(
                self.writer.upsert, table="users", data=init_users, conflict_keys=["id"]
            )

            logging.info("Writing initial products...")
            await asyncio.to_thread(
                self.writer.upsert,
                table="products",
                data=generate_from_csv("products.csv"),
                conflict_keys=["id"],
            )

            logging.info("Writing initial distribution centers...")
            await asyncio.to_thread(
                self.writer.upsert,
                table="dist_centers",
                data=generate_from_csv("distribution_centers.csv"),
                conflict_keys=["id"],
            )
            logging.info("Initialization successful.")
            return True
        except (SQLAlchemyError, OperationalError) as e:
            logging.critical(
                f"A fatal error occurred during initial setup. Cannot continue. Error: {e}"
            )
            return False

    def _random_row(self, table: str) -> list:
        """One row picked at random through the primary key's index: the
        first id at or after a random UUID, else the smallest id (wrap
        round). Ids are random UUIDs, so this is close to uniform (a row
        after a wider gap is a little more likely). Upstream's ORDER BY
        RANDOM() reads the whole table on every call, a cost that grows
        with the table (P1 baseline)."""
        rows = self.writer.select(
            table=table,
            where_clause="id >= :k",
            where_params={"k": str(uuid.uuid4())},
            order_by="id",
            limit=1,
        )
        return rows or self.writer.select(table=table, order_by="id", limit=1)

    async def _simulate_purchases(self):
        """Generates and writes a new purchase transaction, the primary simulation event."""
        # Select a random user
        user_list = await asyncio.to_thread(self._random_row, "users")
        if not user_list:
            logging.warning(
                "A random user is not retrieved. Skipping a purchase event generation."
            )
            return
        random_user = User.from_dict(user_list[0])

        # Potentially update the user's address
        if (
            self.args.user_update_prob > 0
            and random.random() < self.args.user_update_prob
        ):
            logging.info("Updating a user address...")
            random_user = random_user.update_address(
                country=self.args.country,
                state=self.args.state,
                postal_code=self.args.postal_code,
                fake=self.fake,
            )
            await asyncio.to_thread(
                self.writer.upsert,
                table="users",
                data=[random_user],
                conflict_keys=["id"],
            )

        # Generate the order and its associated events
        order = Order.new(user=random_user, fake=self.fake)
        order_items = []
        purchase_events = []
        # Synthetic (spec 4.3, ADR 019): a popular first item, then companions.
        product_ids = pick_order_products(
            order.num_of_items,
            affinity_prob=self.args.basket_affinity_prob,
            companions=self.args.companions,
            skew=self.args.popularity_skew,
        )
        for product_id in product_ids:
            order_item = OrderItem.new(
                order=order, fake=self.fake, product_id=product_id
            )
            purchase_events.extend(
                Event.new(
                    user=random_user,
                    order_item=order_item,
                    event_category=EventCategory.PURCHASE.value,
                    fake=self.fake,
                )
            )
            order_items.append(order_item)

        # Write all purchase-related data concurrently
        await asyncio.gather(
            asyncio.to_thread(
                self.writer.upsert, table="orders", data=[order], conflict_keys=["id"]
            ),
            asyncio.to_thread(
                self.writer.upsert,
                table="order_items",
                data=order_items,
                conflict_keys=["id"],
            ),
            asyncio.to_thread(self.events.upsert, purchase_events),
        )

    def _simulate_order_update(self):
        """Synchronous helper containing the logic for updating an order. To be run in a thread."""
        random_order_list = self._random_row("orders")
        if not random_order_list:
            logging.warning(
                "A random order is not retrieved. Skipping random order event upate."
            )
            return

        random_order = Order.from_dict(random_order_list[0])
        random_user = User.from_dict(
            self.writer.select(
                table="users",
                where_clause="id = :uid",
                where_params={"uid": random_order.user_id},
            )[0]
        )
        random_items = [
            OrderItem.from_dict(d)
            for d in self.writer.select(
                table="order_items",
                where_clause="order_id = :oid",
                where_params={"oid": random_order.id},
            )
        ]

        random_order.update_status(fake=self.fake)
        updated_items = [
            item.update_status(order=random_order) for item in random_items
        ]

        random_events = []
        for item in updated_items:
            event_category = None
            if item.status == OrderStatus.CANCELLED.value:
                event_category = EventCategory.CANCEL.value
            elif item.status == OrderStatus.RETURNED.value:
                event_category = EventCategory.RETURN.value

            if event_category:
                random_events.extend(
                    Event.new(
                        user=random_user,
                        order_item=item,
                        event_category=event_category,
                        fake=self.fake,
                    )
                )

        self.writer.upsert(table="orders", data=[random_order], conflict_keys=["id"])
        self.writer.upsert(
            table="order_items", data=updated_items, conflict_keys=["id"]
        )
        if random_events:
            self.events.upsert(random_events)

    async def _simulate_side_tasks(self):
        """Runs secondary simulation events based on their respective probabilities."""
        side_tasks = []

        if (
            self.args.user_create_prob > 0
            and random.random() < self.args.user_create_prob
        ):
            logging.info("Creating a new user...")
            new_user = User.new(
                country=self.args.country,
                state=self.args.state,
                postal_code=self.args.postal_code,
                fake=self.fake,
            )
            side_tasks.append(
                asyncio.to_thread(
                    self.writer.upsert,
                    table="users",
                    data=[new_user],
                    conflict_keys=["id"],
                )
            )

        if (
            self.args.ghost_create_prob > 0
            and random.random() < self.args.ghost_create_prob
        ):
            logging.info("Creating a ghost event...")
            ghost_events = Event.new(
                user=None,
                order_item=None,
                event_category=EventCategory.GHOST.value,
                fake=self.fake,
            )
            side_tasks.append(asyncio.to_thread(self.events.upsert, ghost_events))

        if (
            self.args.order_update_prob > 0
            and random.random() < self.args.order_update_prob
        ):
            logging.info("Update an order status...")
            side_tasks.append(asyncio.to_thread(self._simulate_order_update))

        if side_tasks:
            await asyncio.gather(*side_tasks)

    async def run(self):
        """The main simulation loop, which orchestrates primary and side tasks."""
        current_iteration = 0
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        # Once a minute: iterations done and backlog dropped, so a load test
        # can tell whether the generator kept up.
        window_start, window_done, window_dropped = loop.time(), 0, 0.0
        while True:
            if 0 < self.args.max_iter <= current_iteration:
                logging.info(
                    f"Stopping data generation after reaching {current_iteration} iterations."
                )
                break

            now = loop.time()
            if now - window_start >= 60:
                logging.info(
                    f"pace: {window_done / (now - window_start):.1f} iterations/s "
                    f"(target {self.args.avg_qps:g}), {window_dropped:.1f} s behind dropped"
                )
                window_start, window_done, window_dropped = now, 0, 0.0
            try:
                next_at, wait_time, dropped = next_deadline(
                    next_at, now, random.expovariate(self.args.avg_qps)
                )
                window_dropped += dropped
                await asyncio.sleep(wait_time)

                await self._simulate_purchases()
                await self._simulate_side_tasks()
            except (SQLAlchemyError, OperationalError, PyMongoError) as e:
                self.consecutive_db_errors += 1
                logging.warning(
                    f"A database error occurred. Consecutive error count: {self.consecutive_db_errors}/{self.max_consecutive_errors}. "
                    f"Error: {e}"
                )
                if self.consecutive_db_errors >= self.max_consecutive_errors:
                    logging.critical(
                        f"Exceeded max consecutive DB error limit ({self.max_consecutive_errors}). "
                        "The simulation will now stop."
                    )
                    break
                await asyncio.sleep(5)
            else:
                if self.consecutive_db_errors > 0:
                    logging.info(
                        "Database operation successful. Resetting consecutive error counter."
                    )
                self.consecutive_db_errors = 0
                current_iteration += 1
                window_done += 1
        logging.info("Simulation loop has finished.")

    async def close(self):
        """Gracefully closes the database connection."""
        logging.info("Closing database connection...")
        if self.writer.conn and not self.writer.conn.closed:
            await asyncio.to_thread(self.writer.close)
        await asyncio.to_thread(self.events.close)
        logging.info("Database connection closed.")


async def run_simulation(args: argparse.Namespace):
    """Orchestrates the creation, initialization, and execution of the simulator.

    --workers N runs N simulators, each with its own connections and 1/N of
    the rate (SQLAlchemy connections are not shared between them); the first
    one does the one-time setup."""
    share = copy.copy(args)
    share.avg_qps = args.avg_qps / args.workers
    simulators = [
        TheLookECommSimulator(share, Faker()) for _ in range(args.workers)
    ]
    try:
        if await simulators[0].initialize():
            await asyncio.gather(*(s.run() for s in simulators))
    except asyncio.CancelledError:
        logging.warning("Data generation task was cancelled.")
    finally:
        for simulator in simulators:
            await simulator.close()


def main():
    """
    Parses command-line arguments and starts the data generation simulation.
    """
    # fmt: off
    parser = argparse.ArgumentParser(description="Generate theLook eCommerce data")
    ## --- General Arguments ---
    parser.add_argument("--avg-qps", type=float, default=20.0, help="Average events per second.")
    parser.add_argument("--max-iter", type=int, default=-1, help="Max number of successful iterations (per worker). Default -1 for infinite.")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent simulators sharing --avg-qps, each with its own connections (load tests).")
    ## --- User Arguments ---
    parser.add_argument("--init-num-users", type=int, default=1000, help="Initial number of users to create.")
    parser.add_argument("--country", default="*", help="User country.")
    parser.add_argument("--state", default="*", help="User state.")
    parser.add_argument("--postal-code", default="*", help="User postal code.")
    parser.add_argument("--user-create-prob", type=float, default=0.05, help="Probability of generating a new user. Default is 0.05. Set to 0 to disable.")
    parser.add_argument("--user-update-prob", type=float, default=0.1, help="Probability of updating a user address. Default is 0.1. Set to 0 to disable.")
    ## --- Order Arguments ---
    parser.add_argument("--order-update-prob", type=float, default=0.4, help="Probability of updating an order status. Default is 0.4. Set to 0 to disable.")
    ## --- Ghost Event Arguments ---
    parser.add_argument("--ghost-create-prob", type=float, default=0.2, help="Probability of generating a ghost event. Default is 0.2. Set to 0 to disable.")
    ## --- Synthetic additions (thelook-cdc-lakehouse, spec 4.3) ---
    parser.add_argument("--basket-affinity-prob", type=float, default=0.6, help="Probability that each item after the first goes with the first item (one of its companions, or its category with --companions 0). Set to 0 for upstream behaviour.")
    parser.add_argument("--companions", type=int, default=5, help="Number of fixed companion products per product for basket affinity. 0 = any product of the first item's category.")
    parser.add_argument("--popularity-skew", type=float, default=0.8, help="Zipf exponent for an order's first item over a fixed product ranking. 0 = uniform (upstream).")
    ## --- Database Arguments ---
    parser.add_argument("--db-host", default="localhost", help="Database host.")
    parser.add_argument("--db-user", default="db_user", help="Database user.")
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD", "db_password"), help="Database password. Defaults to the DB_PASSWORD environment variable.")
    parser.add_argument("--db-name", default="fh_dev", help="Database name.")
    parser.add_argument("--db-schema", default="demo", help="Database schema.")
    parser.add_argument("--db-batch-size", type=int, default=1000)
    ## --- MongoDB Arguments (clickstream events) ---
    parser.add_argument("--mongo-host", default="localhost", help="MongoDB host (replica set rs0).")
    parser.add_argument("--mongo-user", default="generator", help="MongoDB user.")
    parser.add_argument("--mongo-password", default=os.environ.get("MONGO_PASSWORD", ""), help="MongoDB password. Defaults to the MONGO_PASSWORD environment variable.")
    parser.add_argument("--mongo-db", default="web", help="MongoDB database holding the events collection.")
    ## --- Kafka Arguments ---
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Bootstrap server addresses.")
    parser.add_argument("--topic-prefix", type=str, default="ecomm", help="Kafka topic prefix.")
    # fmt: on

    args = parser.parse_args()
    logging.info({**vars(args), "db_password": "***", "mongo_password": "***"})

    try:
        asyncio.run(run_simulation(args))
    except KeyboardInterrupt:
        logging.warning("Data generator stopped by user.")
    except Exception as e:
        logging.critical(f"An unexpected top-level error occurred: {e}", exc_info=True)
    finally:
        logging.info("Application shutdown complete.")


if __name__ == "__main__":
    main()
