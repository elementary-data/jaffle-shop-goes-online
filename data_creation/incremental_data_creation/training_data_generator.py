"""
JAFFLE SHOP TRAINING DATA GENERATOR

This script generates realistic training data for a fictional jaffle (sandwich) shop
to demonstrate e-commerce analytics, marketing attribution, and data observability.

PURPOSE:
- Creates 60 days of historical training data for dbt models
- Generates realistic customer behavior patterns for a food delivery business
- Provides foundation data for marketing attribution and conversion analysis

BUSINESS CONTEXT:
- Jaffle Shop: Online sandwich ordering platform
- Order values: $1-$30 (realistic for food delivery)
- Customer behavior: Repeat purchases, multiple payment methods
- Time patterns: Business hours weighted, uniform distribution across 60 days

DATA RELATIONSHIPS:
- Customers -> Orders (one-to-many)
- Orders -> Payments (one-to-many, up to 2 payments per order)
- Customers -> Signups (one-to-one, signup before first order)

GENERATED FILES:
- raw_customers_training.csv: Customer master data
- raw_orders_training.csv: Order transactions
- raw_payments_training.csv: Payment records
- raw_signups_training.csv: User registration data

IMPORTANT NOTES:
- All timestamps are uniform across 60 days (no weighted distribution)
- Payment amounts are in cents (100 = $1.00)
- Customer selection is weighted to ensure distribution
- Order timestamps use business hours weighting (9AM-5PM peak)
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

# Business Parameters
CUSTOMERS_COUNT = 2000  # Total customers to generate
ORDERS_COUNT = 15000  # Total orders (7.5 orders per customer avg)
TIME_SPAN_IN_DAYS = 60  # Historical data window
LOWEST_PAYMENT_IN_CENTS = 100  # $1.00 (minimum jaffle price)
HIGHEST_PAYMENT_IN_CENTS = 3000  # $30.00 (maximum realistic jaffle order)
MAX_PAYMENTS_PER_ORDER = 2  # Split payments allowed (credit card + coupon)


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


def generate_training_data():
    """
    Main function to generate all training data files

    Generates data in dependency order:
    1. Customers (foundation)
    2. Orders (references customers)
    3. Payments (references orders)
    4. Signups (references customers and orders for timing)
    """
    generate_customers_data()
    generate_orders_data()
    generate_payments_data()
    generate_signups_data()


def generate_customers_data():
    """
    Generate customer master data using original jaffle shop names

    Creates realistic customer profiles by:
    - Extracting first/last names from original sample data
    - Generating sequential customer IDs
    - Randomly combining names for variety

    Output: raw_customers_training.csv
    Schema: customer_id, first_name, last_name
    """
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_customers.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_customers_training.csv",
    )

    # Extract unique names from original data for realism
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_first_names = list(set([row[1] for row in data]))
    all_last_names = list(set([row[2] for row in data]))

    # Generate customer records
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
    """
    Generate order transaction data with realistic patterns

    Creates orders with:
    - Uniform distribution across 60 days (no recent bias)
    - Business hours weighting (9AM-5PM peak for food orders)
    - Weighted customer selection (80% of orders to 80% of customers)
    - Random order statuses from original data

    Output: raw_orders_training.csv
    Schema: order_id, customer_id, order_date, status
    """
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_orders.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_orders_training.csv",
    )

    # Get possible order statuses from original data
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_order_statuses = list(set([row[3] for row in data]))

    new_orders = []
    order_timestamps = []

    for order_id in range(1, ORDERS_COUNT + 1):
        # Generate uniform timestamp distribution across 60 days
        # No weighted distribution - even spread for realistic long-term analysis
        days_back = random.randint(0, TIME_SPAN_IN_DAYS)
        base_datetime = datetime.now() - timedelta(days=days_back)

        # Weight hours toward business hours (food delivery peak times)
        # Hours 8-14 (8AM-2PM) get highest weight for lunch orders
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

        order_timestamp = base_datetime.replace(hour=hour, minute=minute, second=second)
        order_timestamps.append(order_timestamp)

        # Weighted customer selection to create realistic distribution
        # 80% of orders go to first 80% of customers (Pareto principle)
        # Remaining 20% of orders distributed among all customers
        if random.random() < 0.8:
            customer_id = random.randint(1, int(CUSTOMERS_COUNT * 0.8))
        else:
            customer_id = random.randint(1, CUSTOMERS_COUNT)

        new_orders.append(
            [
                order_id,  # ORDER ID
                customer_id,  # CUSTOMER ID (weighted for realism)
                order_timestamp.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # ORDER DATE (full timestamp)
                all_order_statuses[
                    random.randint(0, len(all_order_statuses) - 1)
                ],  # ORDER STATUS
            ]
        )

    write_to_csv(new_data_path, headers, new_orders)
    log_time_range("Training Orders", order_timestamps)


def generate_payments_data():
    """
    Generate payment data with realistic jaffle shop pricing

    Creates payments with:
    - 1-2 payments per order (split payments allowed)
    - Realistic jaffle shop pricing ($1-$30 per payment)
    - Various payment methods from original data
    - All amounts in cents (100 = $1.00)

    Business Logic:
    - Single payment: Full order amount
    - Split payment: e.g., Credit card + coupon
    - Price range reflects jaffle shop economics

    Output: raw_payments_training.csv
    Schema: payment_id, order_id, payment_method, amount
    """
    origin_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ORIGINAL_JAFFLE_DATA_DIRECTORY_NAME, "raw_payments.csv"
    )
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_payments_training.csv",
    )

    # Get payment methods from original data
    headers, data = split_csv_to_headers_and_data(csv_path=origin_data_path)
    all_payments_methods = list(set([row[2] for row in data]))

    new_payments = []
    payment_id = 1

    for order_id in range(1, ORDERS_COUNT + 1):
        # Allow 1-2 payments per order (split payments realistic for food delivery)
        amount_of_payments = random.randint(1, MAX_PAYMENTS_PER_ORDER)

        for payment in range(amount_of_payments):
            # Each payment is independently random within jaffle price range
            # This creates realistic order totals of $1-$60 for multiple payments
            payment_amount_cents = random.randint(
                LOWEST_PAYMENT_IN_CENTS, HIGHEST_PAYMENT_IN_CENTS
            )
            new_payments.append(
                [
                    payment_id,  # PAYMENT_ID
                    order_id,  # ORDER ID
                    all_payments_methods[
                        random.randint(0, len(all_payments_methods) - 1)
                    ],  # PAYMENT METHOD
                    payment_amount_cents,  # AMOUNT in cents (realistic jaffle prices!)
                ]
            )
            payment_id += 1

    write_to_csv(new_data_path, headers, new_payments)


def generate_signups_data():
    """
    Generate user signup data tied to customer order behavior

    Creates signup records with:
    - Signup timing before customer's first order
    - Realistic email addresses based on customer names
    - Hashed passwords for security
    - Logical signup-to-order funnel

    Business Logic:
    - Customers sign up before their first order
    - Signup date = earliest order date for each customer
    - Email format: firstname.lastname.id@example.com

    Output: raw_signups_training.csv
    Schema: id, user_id, user_email, hashed_password, signup_date
    """
    new_data_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        NEW_JAFFLE_TRAINING_DATA_DIRECORTY_RELATIVE_PATH,
        "raw_signups_training.csv",
    )
    headers = ["id", "user_id", "user_email", "hashed_password", "signup_date"]

    # Read customer and order data to establish relationships
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

    # Find each customer's first order date for signup timing
    customer_min_order_time_map = defaultdict(
        lambda: (
            datetime.now() - timedelta(random.randint(0, TIME_SPAN_IN_DAYS))
        ).strftime("%Y-%m-%d %H:%M:%S")
    )

    # Calculate actual first order date for each customer
    for order in orders_data:
        customer_id = order[1]
        order_date = order[2]

        # Update to earliest order date for this customer
        current_min = datetime.strptime(
            customer_min_order_time_map[customer_id], "%Y-%m-%d %H:%M:%S"
        )
        order_datetime = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S")

        customer_min_order_time_map[customer_id] = min(
            current_min, order_datetime
        ).strftime("%Y-%m-%d %H:%M:%S")

    # Generate signup records
    new_signups = []
    signup_timestamps = []

    for customer in customers_data:
        customer_id = customer[0]
        first_name = customer[1]
        last_name = customer[2]

        # Signup occurs before first order
        signup_date_str = customer_min_order_time_map[customer_id]
        signup_timestamp = datetime.strptime(signup_date_str, "%Y-%m-%d %H:%M:%S")
        signup_timestamps.append(signup_timestamp)

        # Generate realistic email (with some missing for data quality testing)
        email = (
            f"{first_name}{last_name.lower()}{customer_id}@example.com"
            if random.randint(0, 30)  # 30/31 chance of having email
            else ""
        )

        new_signups.append(
            [
                customer_id,  # SIGNUP ID (matches customer ID)
                customer_id,  # CUSTOMER ID
                email,  # USER EMAIL
                hashlib.sha256(
                    datetime.now().isoformat().encode()
                ).hexdigest(),  # HASHED PASSWORD
                signup_date_str,  # SIGNUP DATE
            ]
        )

    write_to_csv(new_data_path, headers, new_signups)
    log_time_range("Training Signups", signup_timestamps)


if __name__ == "__main__":
    generate_training_data()
