from datetime import datetime, timedelta
import os
import random
from utils.csv import (
    split_csv_to_headers_and_data,
    write_to_csv,
)
from .ads_data_generator import get_all_ad_ids

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
JAFFLE_SHOP_SEEDS_PATH = "../../jaffle_shop_online/seeds"
TRAINING_DATA_PATH = "../../jaffle_shop_online/seeds/training"

# Configuration
SESSIONS_PER_CUSTOMER_MIN = 1
SESSIONS_PER_CUSTOMER_MAX = 8
SESSION_DURATION_MIN_MINUTES = 1
SESSION_DURATION_MAX_MINUTES = 45
CONVERSION_ATTRIBUTION_DAYS = (
    1  # Changed from 30 to 1 day - more realistic for jaffle shop
)


def generate_sessions_data(data_source="training"):
    """Generate session data for both website and app platforms"""
    generate_website_sessions_data(data_source)
    generate_app_sessions_data(data_source)


def get_customer_order_data(data_source="training"):
    """Get customer first order dates for attribution timing"""
    if data_source == "training":
        customers_path = os.path.join(
            CURRENT_DIRECTORY_PATH, TRAINING_DATA_PATH, "raw_customers_training.csv"
        )
        orders_path = os.path.join(
            CURRENT_DIRECTORY_PATH, TRAINING_DATA_PATH, "raw_orders_training.csv"
        )
    else:  # validation
        customers_path = os.path.join(
            CURRENT_DIRECTORY_PATH,
            "../../jaffle_shop_online/seeds/validation",
            "raw_customers_validation.csv",
        )
        orders_path = os.path.join(
            CURRENT_DIRECTORY_PATH,
            "../../jaffle_shop_online/seeds/validation",
            "raw_orders_validation.csv",
        )

    # Read customer data
    customers_headers, customers_data = split_csv_to_headers_and_data(customers_path)
    customer_ids = [int(row[0]) for row in customers_data]

    # Read order data
    orders_headers, orders_data = split_csv_to_headers_and_data(orders_path)

    # Build customer first order mapping
    customer_first_orders = {}
    for order in orders_data:
        customer_id = int(order[1])  # user_id column
        order_date = datetime.strptime(
            order[2], "%Y-%m-%d %H:%M:%S"
        )  # order_date column (now timestamp)

        if customer_id not in customer_first_orders:
            customer_first_orders[customer_id] = order_date
        else:
            customer_first_orders[customer_id] = min(
                customer_first_orders[customer_id], order_date
            )

    return customer_ids, customer_first_orders


def generate_website_sessions_data(data_source="training"):
    """Generate website session data"""
    sessions_data = []
    headers = [
        "session_id",
        "customer_id",
        "started_at",
        "ended_at",
        "utm_source",
        "ad_id",
    ]

    customer_ids, customer_first_orders = get_customer_order_data(data_source)
    all_ad_ids = get_all_ad_ids()

    session_counter = 1

    for customer_id in customer_ids:
        # Determine number of sessions for this customer
        num_sessions = random.randint(
            SESSIONS_PER_CUSTOMER_MIN, SESSIONS_PER_CUSTOMER_MAX
        )

        # If customer has orders, ensure at least one session before first order
        has_orders = customer_id in customer_first_orders
        if has_orders:
            first_order_date = customer_first_orders[customer_id]

            # Generate at least one session within attribution window (realistic for jaffle shop)
            # 70% same day, 30% within 24 hours
            if random.random() < 0.7:
                # Same day conversion - session 1-6 hours before order
                hours_before = random.randint(1, 6)
                attribution_session_start = first_order_date - timedelta(
                    hours=hours_before
                )
            else:
                # Previous day conversion - session 6-24 hours before order
                hours_before = random.randint(6, 24)
                attribution_session_start = first_order_date - timedelta(
                    hours=hours_before
                )

            attribution_session_end = attribution_session_start + timedelta(
                minutes=random.randint(
                    SESSION_DURATION_MIN_MINUTES, SESSION_DURATION_MAX_MINUTES
                )
            )

            # Choose ad (favor Google and Facebook for website)
            ad_id = random.choice(
                [aid for aid in all_ad_ids if aid.startswith(("g", "f"))]
            )
            utm_source = "google" if ad_id.startswith("g") else "facebook"

            sessions_data.append(
                [
                    f"w{session_counter}",
                    customer_id,
                    attribution_session_start.strftime("%Y-%m-%d %H:%M:%S"),
                    attribution_session_end.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_source,
                    ad_id,
                ]
            )
            session_counter += 1
            num_sessions -= 1  # One session already created

        # For jaffle shop: skip additional random sessions - people typically just have
        # the attribution session that leads to conversion
        pass

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        JAFFLE_SHOP_SEEDS_PATH,
        "sessions",
        "stg_website_sessions.csv",
    )
    write_to_csv(output_path, headers, sessions_data)
    print(f"Generated {len(sessions_data)} website session records")


def generate_app_sessions_data(data_source="training"):
    """Generate app session data"""
    sessions_data = []
    headers = [
        "session_id",
        "customer_id",
        "started_at",
        "ended_at",
        "utm_source",
        "ad_id",
    ]

    customer_ids, customer_first_orders = get_customer_order_data(data_source)
    all_ad_ids = get_all_ad_ids()

    session_counter = 1

    # App sessions are typically fewer than website sessions
    app_customer_sample = random.sample(
        customer_ids, int(len(customer_ids) * 0.6)
    )  # 60% of customers use app

    for customer_id in app_customer_sample:
        # Fewer sessions per customer for app
        num_sessions = random.randint(1, SESSIONS_PER_CUSTOMER_MAX // 2)

        # If customer has orders, ensure at least one session before first order
        has_orders = customer_id in customer_first_orders
        if has_orders:
            first_order_date = customer_first_orders[customer_id]

            # Generate at least one session within attribution window (realistic for jaffle shop)
            # 70% same day, 30% within 24 hours
            if random.random() < 0.7:
                # Same day conversion - session 1-6 hours before order
                hours_before = random.randint(1, 6)
                attribution_session_start = first_order_date - timedelta(
                    hours=hours_before
                )
            else:
                # Previous day conversion - session 6-24 hours before order
                hours_before = random.randint(6, 24)
                attribution_session_start = first_order_date - timedelta(
                    hours=hours_before
                )

            attribution_session_end = attribution_session_start + timedelta(
                minutes=random.randint(
                    SESSION_DURATION_MIN_MINUTES, SESSION_DURATION_MAX_MINUTES
                )
            )

            # Choose ad (favor Facebook and Instagram for app)
            ad_id = random.choice(
                [aid for aid in all_ad_ids if aid.startswith(("f", "i"))]
            )
            utm_source = "facebook" if ad_id.startswith("f") else "instagram"

            sessions_data.append(
                [
                    f"a{session_counter}",
                    customer_id,
                    attribution_session_start.strftime("%Y-%m-%d %H:%M:%S"),
                    attribution_session_end.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_source,
                    ad_id,
                ]
            )
            session_counter += 1
            num_sessions -= 1  # One session already created

        # For jaffle shop: skip additional random sessions - people typically just have
        # the attribution session that leads to conversion
        pass

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        JAFFLE_SHOP_SEEDS_PATH,
        "sessions",
        "stg_app_sessions.csv",
    )
    write_to_csv(output_path, headers, sessions_data)
    print(f"Generated {len(sessions_data)} app session records")


if __name__ == "__main__":
    generate_sessions_data()
