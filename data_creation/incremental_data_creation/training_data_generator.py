from datetime import datetime, timedelta
import os
import random
from collections import defaultdict
import hashlib
from utils.csv import (
    split_csv_to_headers_and_data,
    write_to_csv,
)

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME = "original_jaffle_shop_data"
NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH = (
    "../../jaffle_shop_online/seeds/training"
)
NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH = (
    "../../jaffle_shop_online/seeds/validation"
)

CUSTOMERS_COUNT = 2000
ORDERS_COUNT = 15000  # Increased orders for higher conversion rate
TIME_SPAN_IN_DAYS = 60
LOWEST_PAYMENT_IN_HUNDRENDS = 0
HIGHEST_PAYMENT_IN_HUNDRENDS = 28
MAX_PAYMENTS_PER_ORDER = 2


def generate_training_data():
    generate_customers_data()
    generate_orders_data()
    generate_payments_data()
    generate_signups_data()


def generate_customers_data():
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_customers.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_first_names = list(set([row[1] for row in data]))
    all_last_names = list(set([row[2] for row in data]))
    new_customers = []
    for customer_id in range(1, CUSTOMERS_COUNT + 1):
        new_customers.append(
            [
                customer_id,  # CUSTOMER ID
                all_first_names[
                    random.randint(0, len(all_first_names) - 1)
                ],  # FIRST NAME
                all_last_names[random.randint(0, len(all_last_names) - 1)],  # LAST NAME
            ]
        )
    write_to_csv(new_data_path, headers, new_customers)


def generate_orders_data():
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_orders.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_order_statuses = list(set([row[3] for row in data]))
    new_orders = []
    for order_id in range(1, ORDERS_COUNT + 1):
        # Generate random timestamp within the time span, including recent days
        # Use weighted distribution to ensure more recent orders (for better ROAS visibility)
        days_back_weights = []
        for i in range(TIME_SPAN_IN_DAYS + 1):
            if i <= 7:  # Last 7 days get higher weight
                days_back_weights.append(3)
            elif i <= 30:  # Last 30 days get medium weight
                days_back_weights.append(2)
            else:  # Older days get lower weight
                days_back_weights.append(1)

        days_back = random.choices(
            range(TIME_SPAN_IN_DAYS + 1), weights=days_back_weights
        )[0]
        base_datetime = datetime.now() - timedelta(days=days_back)

        hour = random.choices(
            range(24),
            weights=[
                1,
                1,
                1,
                1,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                8,
                7,
                6,
                5,
                4,
                3,
                2,
                2,
                1,
                1,
                1,
            ],
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        order_timestamp = base_datetime.replace(hour=hour, minute=minute, second=second)

        # Use weighted customer selection to ensure more customers get orders
        # 80% of orders go to first 80% of customers (more distributed)
        if random.random() < 0.8:
            customer_id = random.randint(1, int(CUSTOMERS_COUNT * 0.8))
        else:
            customer_id = random.randint(1, CUSTOMERS_COUNT)

        new_orders.append(
            [
                order_id,  # ORDER ID
                customer_id,  # CUSTOMER ID (now weighted)
                order_timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # ORDER DATE (now timestamp)
                all_order_statuses[
                    random.randint(0, len(all_order_statuses) - 1)
                ],  # ORDER STATUS
            ]
        )
    write_to_csv(new_data_path, headers, new_orders)


def generate_payments_data():
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_payments.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_payments_training.csv",
    )
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_payments_methods = list(set([row[2] for row in data]))
    new_payments = []
    payment_id = 1
    for order_id in range(1, ORDERS_COUNT + 1):
        amount_of_payments = random.randint(1, MAX_PAYMENTS_PER_ORDER)
        max_total_payment_in_hundrends = HIGHEST_PAYMENT_IN_HUNDRENDS
        for payment in range(amount_of_payments):
            payment_amount_in_hundrends = random.randint(
                LOWEST_PAYMENT_IN_HUNDRENDS, max_total_payment_in_hundrends
            )
            new_payments.append(
                [
                    payment_id,  # PAYMENT_ID
                    order_id,  # ORDER ID
                    all_payments_methods[
                        random.randint(0, len(all_payments_methods) - 1)
                    ],  # PAYMENT METHOD
                    (payment_amount_in_hundrends + 1) * 100,  # AMOUNT
                ]
            )
            payment_id += 1
            max_total_payment_in_hundrends -= payment_amount_in_hundrends
    write_to_csv(new_data_path, headers, new_payments)


def generate_signups_data():
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_signups_training.csv",
    )
    headers = ["id", "user_id", "user_email", "hashed_password", "signup_date"]
    customers_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    orders_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    customers_headers, customers_data = split_csv_to_headers_and_data(
        csv_path=customers_csv_path
    )
    orders_headers, orders_data = split_csv_to_headers_and_data(
        csv_path=orders_csv_path
    )

    customer_min_order_time_map = defaultdict(
        lambda: (
            datetime.now() - timedelta(random.randint(0, TIME_SPAN_IN_DAYS))
        ).strftime("%Y-%m-%d %H:%M:%S")
    )
    for order in orders_data:
        customer_min_order_time_map[order[1]] = min(
            datetime.strptime(
                customer_min_order_time_map[order[1]], "%Y-%m-%d %H:%M:%S"
            ),
            datetime.strptime(order[2], "%Y-%m-%d %H:%M:%S"),
        ).strftime("%Y-%m-%d %H:%M:%S")

    new_signups = []
    for customer in customers_data:
        new_signups.append(
            [
                customer[0],  # SIGNUP ID
                customer[0],  # CUSTOMER ID
                (
                    f"{customer[1]}{customer[2].lower()}{customer[0]}@example.com"
                    if random.randint(0, 30)
                    else ""
                ),  # USER EMAIL
                hashlib.sha256(datetime.now().isoformat().encode()).hexdigest(),
                customer_min_order_time_map[customer[0]],
            ]
        )
    write_to_csv(new_data_path, headers, new_signups)


if __name__ == "__main__":
    generate_training_data()
