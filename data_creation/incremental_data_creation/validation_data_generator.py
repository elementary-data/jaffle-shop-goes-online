"""
JAFFLE SHOP VALIDATION DATA GENERATOR

This script generates validation data for the jaffle shop to simulate:
- Extending historical training data with new validation period
- Real-time data validation scenarios
- Business growth patterns and new customer acquisition

PURPOSE:
- Creates validation data that extends training data timeline
- Simulates new customer acquisition during validation period
- Provides test data for model validation and monitoring
- Demonstrates incremental data loading patterns

BUSINESS CONTEXT:
- Extends training data with additional customers and orders
- Validation period: 1 day after training data ends
- New customers: 200 additional customers with realistic behavior
- Same pricing model: $1-$30 jaffle shop orders

DATA RELATIONSHIPS:
- Builds upon existing training data (customers 1-2000, orders 1-15000)
- Adds validation customers (2001-2200) and orders (15001-15050)
- Maintains same schema and business logic as training data
- Preserves referential integrity across datasets

GENERATED FILES:
- raw_customers_validation.csv: Training + new validation customers
- raw_orders_validation.csv: Training + new validation orders
- raw_payments_validation.csv: Training + new validation payments
- raw_signups_validation.csv: Training + new validation signups

IMPORTANT NOTES:
- Validation data INCLUDES all training data (cumulative)
- New records are appended to existing training data
- Maintains same realistic pricing ($1-$30 in cents)
- Validation orders occur on day after training period ends
"""

from datetime import datetime, timedelta
import os
import random
from collections import defaultdict
import hashlib
import logging
from utils.csv import (
    split_csv_to_headers_and_data,
    write_to_csv,
)

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION PARAMETERS
# =============================================================================

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME = "original_jaffle_shop_data"
NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH = (
    "../../jaffle_shop_online/seeds/training"
)
NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH = (
    "../../jaffle_shop_online/seeds/validation"
)

# Business Parameters (aligned with training data)
CUSTOMERS_COUNT = 2000  # Original training customers
ORDERS_COUNT = 15000  # Original training orders
TIME_SPAN_IN_DAYS = 60  # Historical training data window
VALIDATION_CUSTOMERS_COUNT = 200  # New customers in validation period
VALIDATION_ORDERS_COUNT = 50  # New orders in validation period


def log_time_range(source_name, timestamps):
    """
    Log the min and max time range for a data source

    Args:
        source_name (str): Name of the data source for logging
        timestamps (list): List of datetime objects to analyze
    """
    if timestamps:
        min_time = min(timestamps)
        max_time = max(timestamps)
        logger.info(
            f"{source_name}: Generated {len(timestamps)} records from {min_time} to {max_time}"
        )
        print(
            f"{source_name}: Time range {min_time} to {max_time} ({len(timestamps)} records)"
        )


def generate_validation_data():
    """
    Main function to generate all validation data files

    Generates cumulative data (training + validation) in dependency order:
    1. Customers (training + new validation customers)
    2. Orders (training + new validation orders)
    3. Payments (training + new validation payments)
    4. Signups (training + new validation signups)
    """
    generate_customers_data()
    generate_orders_data()
    generate_payments_data()
    generate_signups_data()


def generate_customers_data():
    """
    Generate validation customer data (training + new customers)

    Creates cumulative customer dataset by:
    - Loading existing training customers (1-2000)
    - Adding new validation customers (2001-2200)
    - Using same name generation logic as training
    - Maintaining sequential customer IDs

    Output: raw_customers_validation.csv
    Schema: customer_id, first_name, last_name
    Count: 2200 customers total (2000 training + 200 validation)
    """
    # Load existing training customers
    training_customers_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )
    validation_customers_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_validation.csv",
    )

    # Get original name data for realistic name generation
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_customers.csv"
    )

    # Load training data and extract names
    customers_headers, training_customers = split_csv_to_headers_and_data(
        csv_path=training_customers_path
    )
    origin_headers, origin_data = split_csv_to_headers_and_data(
        csv_path=origin_data_path
    )
    all_first_names = list(set([row[1] for row in origin_data]))
    all_last_names = list(set([row[2] for row in origin_data]))

    # Start with all training customers
    new_customers = [*training_customers]

    # Add new validation customers (2001-2200)
    for customer_id in range(
        CUSTOMERS_COUNT + 1, CUSTOMERS_COUNT + VALIDATION_CUSTOMERS_COUNT + 1
    ):
        new_customers.append(
            [
                customer_id,  # CUSTOMER ID (continues from training)
                all_first_names[
                    random.randint(0, len(all_first_names) - 1)
                ],  # FIRST NAME
                all_last_names[random.randint(0, len(all_last_names) - 1)],  # LAST NAME
            ]
        )

    write_to_csv(validation_customers_path, customers_headers, new_customers)


def generate_orders_data():
    """
    Generate validation order data (training + new orders)

    Creates cumulative order dataset by:
    - Loading existing training orders (1-15000)
    - Adding new validation orders (15001-15050)
    - New orders occur on day AFTER training period ends
    - Business hours weighting for realistic timing
    - Mix of existing and new customers

    Output: raw_orders_validation.csv
    Schema: order_id, customer_id, order_date, status
    Count: 15050 orders total (15000 training + 50 validation)
    """
    # Load existing training orders
    training_orders_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )
    validation_orders_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_validation.csv",
    )

    # Get order statuses from original data
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_orders.csv"
    )

    # Load training data and original statuses
    orders_headers, training_orders = split_csv_to_headers_and_data(
        csv_path=training_orders_path
    )
    origin_headers, origin_data = split_csv_to_headers_and_data(
        csv_path=origin_data_path
    )
    all_order_statuses = list(set([row[3] for row in origin_data]))

    # Start with all training orders
    new_orders = [*training_orders]

    # Load validation customers for order assignment
    validation_customers_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_validation.csv",
    )
    customers_headers, validation_customers = split_csv_to_headers_and_data(
        csv_path=validation_customers_path
    )

    # Find the latest order date from training data to determine validation start
    all_order_timestamps = [
        datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S") for row in training_orders
    ]
    last_order_date = max(all_order_timestamps)

    # Validation orders occur on the day AFTER training period ends
    validation_base_date = last_order_date + timedelta(days=1)
    validation_order_timestamps = []

    # Generate new validation orders (15001-15050)
    for order_id in range(len(training_orders) + 1, len(training_orders) + 51):
        # Generate random timestamp within the validation day (business hours weighted)
        hour = random.choices(
            range(24),
            weights=[
                1,  # 00:00
                1,  # 01:00
                1,  # 02:00
                1,  # 03:00
                1,  # 04:00
                2,  # 05:00
                3,  # 06:00
                4,  # 07:00
                5,  # 08:00
                6,  # 09:00
                7,  # 10:00
                8,  # 11:00
                9,  # 12:00 (lunch peak)
                8,  # 13:00
                7,  # 14:00
                6,  # 15:00
                5,  # 16:00
                4,  # 17:00
                3,  # 18:00
                2,  # 19:00
                2,  # 20:00
                1,  # 21:00
                1,  # 22:00
                1,  # 23:00
            ],
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        validation_order_timestamp = validation_base_date.replace(
            hour=hour, minute=minute, second=second
        )
        validation_order_timestamps.append(validation_order_timestamp)
        all_order_timestamps.append(validation_order_timestamp)

        # Random customer selection from all available customers (training + validation)
        new_orders.append(
            [
                order_id,  # ORDER ID (continues from training)
                random.randint(1, len(validation_customers)),  # CUSTOMER ID
                validation_order_timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # ORDER DATE (validation day)
                (
                    all_order_statuses[random.randint(0, len(all_order_statuses) - 1)]
                    if random.randint(0, 1)
                    else "lost"
                ),  # ORDER STATUS (with some failures)
            ]
        )

    write_to_csv(validation_orders_path, orders_headers, new_orders)
    log_time_range("Validation Orders (All)", all_order_timestamps)
    log_time_range("Validation Orders (New Only)", validation_order_timestamps)


def generate_payments_data():
    """
    Generate validation payment data (training + new payments)

    Creates cumulative payment dataset by:
    - Loading existing training payments
    - Adding new validation payments for new orders
    - Using same realistic jaffle pricing ($1-$30)
    - 1-2 payments per order (split payments allowed)

    Output: raw_payments_validation.csv
    Schema: payment_id, order_id, payment_method, amount
    Count: ~22,500+ payments total (training + validation)
    """
    # Load existing training payments
    training_payments_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_payments_training.csv",
    )
    validation_payments_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_payments_validation.csv",
    )

    # Load training data and extract payment methods
    payments_headers, training_payments = split_csv_to_headers_and_data(
        csv_path=training_payments_path
    )
    all_payments_methods = list(set([row[2] for row in training_payments]))

    # Start with all training payments
    new_payments = [*training_payments]
    payment_id = len(training_payments) + 1

    # Add payments for new validation orders (15001-15050)
    for order_id in range(ORDERS_COUNT + 1, ORDERS_COUNT + 51):
        # Generate realistic jaffle shop payment amounts (like original data)
        amount_of_payments = random.randint(1, 2)  # 1-2 payments per order
        for payment in range(amount_of_payments):
            # Random amount between $1-$30 in cents (100-3000 cents)
            # Matches training data pricing for consistency
            payment_amount_cents = random.randint(100, 3000)
            new_payments.append(
                [
                    payment_id,  # PAYMENT_ID (continues from training)
                    order_id,  # ORDER ID (new validation orders)
                    all_payments_methods[
                        random.randint(0, len(all_payments_methods) - 1)
                    ],  # PAYMENT METHOD
                    payment_amount_cents,  # AMOUNT in cents (realistic jaffle prices!)
                ]
            )
            payment_id += 1

    write_to_csv(validation_payments_path, payments_headers, new_payments)


def generate_signups_data():
    """
    Generate validation signup data (training + new signups)

    Creates cumulative signup dataset by:
    - Loading existing training signups
    - Adding signups for new validation customers
    - Signup timing based on customer's first order
    - Realistic email generation with some missing values

    Output: raw_signups_validation.csv
    Schema: id, user_id, user_email, hashed_password, signup_date
    Count: 2200 signups total (2000 training + 200 validation)
    """
    # Load existing training signups
    training_signups_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_signups_training.csv",
    )
    validation_signups_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_signups_validation.csv",
    )

    # Load customer and order data for signup timing
    customers_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_validation.csv",
    )
    orders_csv_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_VALIDATION_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_validation.csv",
    )

    # Load all datasets
    customers_headers, customers_data = split_csv_to_headers_and_data(
        csv_path=customers_csv_path
    )
    orders_headers, orders_data = split_csv_to_headers_and_data(
        csv_path=orders_csv_path
    )
    signups_headers, training_signups_data = split_csv_to_headers_and_data(
        csv_path=training_signups_path
    )

    # Find each customer's first order date for signup timing
    customer_min_order_time_map = defaultdict(
        lambda: (
            datetime.now() - timedelta(random.randint(0, TIME_SPAN_IN_DAYS))
        ).strftime("%Y-%m-%d %H:%M:%S")
    )

    # Calculate first order date for each customer
    for order in orders_data:
        customer_id = order[1]
        order_date = order[2]

        current_min = datetime.strptime(
            customer_min_order_time_map[customer_id], "%Y-%m-%d %H:%M:%S"
        )
        order_datetime = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S")

        customer_min_order_time_map[customer_id] = min(
            current_min, order_datetime
        ).strftime("%Y-%m-%d %H:%M:%S")

    # Start with all training signups
    new_signups = [*training_signups_data]

    # Track all signup timestamps (training + validation)
    all_signup_timestamps = [
        datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S") for row in training_signups_data
    ]
    validation_signup_timestamps = []

    # Add signups for new validation customers (2001-2200)
    for customer in customers_data[CUSTOMERS_COUNT + 2 :]:
        customer_id = customer[0]

        # Signup occurs before first order
        signup_date_str = customer_min_order_time_map[customer_id]
        signup_timestamp = datetime.strptime(signup_date_str, "%Y-%m-%d %H:%M:%S")
        validation_signup_timestamps.append(signup_timestamp)
        all_signup_timestamps.append(signup_timestamp)

        new_signups.append(
            [
                customer_id,  # SIGNUP ID (matches customer ID)
                customer_id,  # CUSTOMER ID
                "abcd@example.com",  # USER EMAIL (simplified for validation)
                hashlib.sha256(
                    datetime.now().isoformat().encode()
                ).hexdigest(),  # HASHED PASSWORD
                signup_date_str,  # SIGNUP DATE
            ]
        )

    # Find last signup date from training data
    last_signup_date = max(
        [
            datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")
            for row in training_signups_data
        ]
    )

    # Generate additional validation signups for the validation day
    validation_base_date = last_signup_date + timedelta(1)

    # Add 2 more signups during validation period (new user acquisition)
    for i in range(len(customers_data) + 1, len(customers_data) + 3):
        # Generate random timestamp within validation day (business hours)
        hour = random.randint(9, 17)  # Business hours for signups
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        validation_signup_timestamp = validation_base_date.replace(
            hour=hour, minute=minute, second=second
        )
        validation_signup_timestamps.append(validation_signup_timestamp)
        all_signup_timestamps.append(validation_signup_timestamp)

        new_signups.append(
            [
                i,  # SIGNUP ID
                i,  # CUSTOMER ID
                "abcd@example.com",  # USER EMAIL
                hashlib.sha256(
                    datetime.now().isoformat().encode()
                ).hexdigest(),  # HASHED PASSWORD
                validation_signup_timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # SIGNUP DATE
            ]
        )

    write_to_csv(validation_signups_path, signups_headers, new_signups)
    log_time_range("Validation Signups (All)", all_signup_timestamps)
    log_time_range("Validation Signups (New Only)", validation_signup_timestamps)


if __name__ == "__main__":
    generate_validation_data()
