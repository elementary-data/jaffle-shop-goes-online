"""
JAFFLE SHOP SESSIONS DATA GENERATOR

This script generates realistic user session data for marketing attribution analysis.
Sessions represent user interactions with the jaffle shop platform across different touchpoints.

PURPOSE:
- Creates session data for marketing attribution modeling
- Simulates customer journey across web and mobile platforms
- Provides touchpoint data for conversion analysis
- Supports multi-touch attribution tracking

BUSINESS CONTEXT:
- Jaffle Shop: Online sandwich ordering with web and mobile apps
- Sessions represent customer interactions before conversions
- Time distribution: Uniform across 60-day period (matches order distribution)
- Platform mix: Website (~60%) and mobile app (~40%) sessions

ATTRIBUTION LOGIC:
- Sessions are distributed uniformly across the full 60-day period
- No bias toward recent periods (matches order timing)
- Each session represents a potential touchpoint in customer journey
- Session timing designed to support various attribution models

GENERATED FILES:
- stg_website_sessions.csv: Web platform sessions
- stg_app_sessions.csv: Mobile app sessions

IMPORTANT NOTES:
- Session generation is NOT tied to individual customer orders
- Uniform distribution across time period prevents attribution bias
- Session volumes realistic for food delivery platform
- Supports both first-touch and multi-touch attribution analysis
"""

import random
import os
import csv
from datetime import datetime, timedelta
import logging
from utils.csv import write_to_csv

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION PARAMETERS
# =============================================================================

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
SESSIONS_DATA_DIRECTORY_RELATIVE_PATH = "../../generated_data/sessions"

# Business Parameters
TIME_SPAN_IN_DAYS = 60  # Training data time span
WEBSITE_SESSIONS_COUNT = 9600  # Web platform sessions (distributed evenly across days/sources)
APP_SESSIONS_COUNT = 6400  # Mobile app sessions (distributed evenly across days/sources)

# Extend coverage to include validation day (for order drop ROAS detection)
VALIDATION_EXTENSION_DAYS = 1  # Extend 1 day beyond training to cover validation period

# Session Channel Sources (marketing attribution) with weights
# Ad platforms (google/facebook/instagram) are weighted higher so enough sessions
# flow through to attribution touches for a stable ROAS baseline.
SESSION_SOURCES = [
    "google",  # Organic search
    "facebook",  # Social media
    "instagram",  # Social media
    "email",  # Email marketing
    "direct",  # Direct traffic
    "youtube",  # Video marketing
    "bing",  # Alternative search
    "linkedin",  # Professional network
    "twitter",  # Social media
    "organic",  # Organic/referral
]
SESSION_SOURCE_WEIGHTS = [
    25,  # google
    22,  # facebook
    20,  # instagram
    4,   # email
    6,   # direct
    4,   # youtube
    3,   # bing
    3,   # linkedin
    3,   # twitter
    6,   # organic
]

# Session Mediums (traffic types)
SESSION_MEDIUMS = [
    "organic",  # Organic search traffic
    "paid",  # Paid advertising
    "social",  # Social media traffic
    "email",  # Email campaigns
    "direct",  # Direct visits
    "referral",  # Referral traffic
    "cpc",  # Cost per click campaigns
    "display",  # Display advertising
    "video",  # Video advertising
]


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


def load_available_ad_ids():
    """
    Load all available ad_ids from the generated ads CSV files

    This ensures sessions reference actual ad campaigns for proper attribution.

    Returns:
        dict: Mapping of utm_source to list of ad_ids for that source
    """
    ad_ids_by_source = {"google": [], "facebook": [], "instagram": []}

    ads_directory = os.path.join(
        CURRENT_DIRECTORY_PATH,
        "../../generated_data/ads",
    )

    # Map CSV files to utm_sources
    ads_files = {
        "google": "stg_google_ads.csv",
        "facebook": "stg_facebook_ads.csv",
        "instagram": "stg_instagram_ads.csv",
    }

    for source, filename in ads_files.items():
        filepath = os.path.join(ads_directory, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", newline="") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        ad_ids_by_source[source].append(row["ad_id"])
                print(f"Loaded {len(ad_ids_by_source[source])} ad_ids for {source}")
            except Exception as e:
                print(f"Warning: Could not load ad_ids from {filename}: {e}")
                # Fallback to dummy ad_ids if file doesn't exist
                ad_ids_by_source[source] = [
                    f"{source}_fallback_ad_{i}" for i in range(100)
                ]
        else:
            print(f"Warning: {filename} not found, using fallback ad_ids")
            # Fallback to dummy ad_ids if file doesn't exist
            ad_ids_by_source[source] = [f"{source}_fallback_ad_{i}" for i in range(100)]

    return ad_ids_by_source


def get_random_ad_id(utm_source, ad_ids_by_source):
    """
    Get a random ad_id for the given utm_source

    Args:
        utm_source (str): The utm_source for the session
        ad_ids_by_source (dict): Mapping of sources to ad_ids

    Returns:
        str: A valid ad_id for attribution
    """
    # Map session sources to ad platform sources
    source_mapping = {
        "google": "google",
        "facebook": "facebook",
        "instagram": "instagram",
        "bing": "google",  # Map bing to google ads
        "youtube": "google",  # Map youtube to google ads
        "linkedin": "facebook",  # Map linkedin to facebook ads
        "twitter": "facebook",  # Map twitter to facebook ads
        "email": "google",  # Map email to google ads
        "direct": "google",  # Map direct to google ads
        "organic": "google",  # Map organic to google ads
    }

    mapped_source = source_mapping.get(utm_source, "google")
    available_ads = ad_ids_by_source.get(mapped_source, ad_ids_by_source["google"])

    if available_ads:
        return random.choice(available_ads)
    else:
        # Final fallback
        return f"{utm_source}_fallback_ad_001"


def load_ordering_customer_ids():
    """
    Load customer IDs that appear in the training orders data.

    Sessions are biased toward these customers so that enough sessions
    match an order within the 7-day attribution window, producing a
    stable ROAS baseline instead of a noisy one.
    """
    orders_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        "../../generated_data/training/raw_orders_training.csv",
    )
    customer_ids = set()
    if os.path.exists(orders_path):
        with open(orders_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                customer_ids.add(int(row["user_id"]))
        print(f"Loaded {len(customer_ids)} ordering customer IDs for session bias")
    return sorted(customer_ids) if customer_ids else list(range(1, 2001))


def make_daily_customer_pool(ordering_customer_ids, day_offset, size):
    """Build a shuffled customer pool for one day, cycling through ordering customers."""
    rng = random.Random(42 + day_offset)
    pool = []
    while len(pool) < size:
        batch = ordering_customer_ids.copy()
        rng.shuffle(batch)
        pool.extend(batch)
    return pool[:size]


def generate_sessions_data():
    """
    Main function to generate all sessions data

    Generates sessions for both platforms:
    1. Website sessions (primary platform)
    2. Mobile app sessions (secondary platform)

    Both use uniform time distribution to avoid attribution bias
    Sessions now reference actual ad_ids from ads data for proper attribution.
    """
    # Load available ad_ids from generated ads data
    print("Loading ad_ids for attribution linkage...")
    ad_ids_by_source = load_available_ad_ids()

    # Load ordering customers for session bias
    ordering_customer_ids = load_ordering_customer_ids()

    generate_website_sessions(ad_ids_by_source, ordering_customer_ids)
    generate_app_sessions(ad_ids_by_source, ordering_customer_ids)


def generate_website_sessions(ad_ids_by_source, ordering_customer_ids):
    """
    Generate website session data for marketing attribution

    Creates website sessions with:
    - Uniform distribution across 60-day period (matches orders)
    - Business hours weighting for realistic daily patterns
    - Mixed marketing channels (organic, paid, social, etc.)
    - References actual ad_ids from ads data for proper attribution

    Attribution Context:
    - Website is primary conversion platform
    - Sessions represent touchpoints in customer journey
    - Time distribution matches order data to avoid bias
    - Channel mix reflects typical food delivery traffic
    - Ad_ids now match actual advertising campaigns

    Args:
        ad_ids_by_source (dict): Mapping of utm_source to available ad_ids

    Output: stg_website_sessions.csv
    Schema: session_id, customer_id, started_at, ended_at, utm_source, ad_id
    Count: 1910 sessions
    """
    website_sessions_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        SESSIONS_DATA_DIRECTORY_RELATIVE_PATH,
        "stg_website_sessions.csv",
    )

    headers = [
        "session_id",
        "customer_id",
        "started_at",
        "ended_at",
        "utm_source",
        "ad_id",
    ]
    sessions_data = []
    session_timestamps = []

    num_days = TIME_SPAN_IN_DAYS + VALIDATION_EXTENSION_DAYS + 1
    total_weight = sum(SESSION_SOURCE_WEIGHTS)
    sessions_per_day = WEBSITE_SESSIONS_COUNT // num_days
    session_id = 0

    hour_weights = [
        1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8,
        9, 8, 7, 6, 5, 4, 3, 2, 2, 1, 1, 1,
    ]

    for day_offset in range(num_days):
        days_back = TIME_SPAN_IN_DAYS - day_offset
        base_date = datetime.now() - timedelta(days=days_back)
        day_pool = make_daily_customer_pool(ordering_customer_ids, day_offset, sessions_per_day)
        pool_idx = 0

        for source_idx, utm_source in enumerate(SESSION_SOURCES):
            source_count = max(1, round(sessions_per_day * SESSION_SOURCE_WEIGHTS[source_idx] / total_weight))
            for _ in range(source_count):
                session_id += 1
                hour = random.choices(range(24), weights=hour_weights)[0]
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                session_timestamp = base_date.replace(hour=hour, minute=minute, second=second)
                session_timestamps.append(session_timestamp)

                session_duration_minutes = random.randint(5, 120)
                end_timestamp = session_timestamp + timedelta(minutes=session_duration_minutes)
                customer_id = day_pool[pool_idx % len(day_pool)]
                pool_idx += 1
                ad_id = get_random_ad_id(utm_source, ad_ids_by_source)

                sessions_data.append([
                    session_id,
                    customer_id,
                    session_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_source,
                    ad_id,
                ])

    write_to_csv(website_sessions_path, headers, sessions_data)
    log_time_range("Website Sessions", session_timestamps)


def generate_app_sessions(ad_ids_by_source, ordering_customer_ids):
    """
    Generate mobile app session data for marketing attribution

    Creates mobile app sessions with:
    - Uniform distribution across 60-day period (matches orders)
    - Business hours weighting for realistic daily patterns
    - Mobile-optimized channel mix (different from web)
    - References actual ad_ids from ads data for proper attribution

    Attribution Context:
    - Mobile app is secondary conversion platform
    - Sessions represent mobile touchpoints in customer journey
    - Time distribution matches order data to avoid bias
    - Channel mix reflects mobile app marketing strategies
    - Ad_ids now match actual advertising campaigns

    Args:
        ad_ids_by_source (dict): Mapping of utm_source to available ad_ids

    Output: stg_app_sessions.csv
        Schema: session_id, customer_id, started_at, ended_at, utm_source, ad_id
    Count: 1143 sessions
    """
    app_sessions_path = os.path.join(
        CURRENT_DIRECTORY_PATH,
        SESSIONS_DATA_DIRECTORY_RELATIVE_PATH,
        "stg_app_sessions.csv",
    )

    headers = [
        "session_id",
        "customer_id",
        "started_at",
        "ended_at",
        "utm_source",
        "ad_id",
    ]
    sessions_data = []
    session_timestamps = []

    num_days = TIME_SPAN_IN_DAYS + VALIDATION_EXTENSION_DAYS + 1
    total_weight = sum(SESSION_SOURCE_WEIGHTS)
    sessions_per_day = APP_SESSIONS_COUNT // num_days
    session_id = 0

    hour_weights = [
        1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        10, 9, 8, 7, 6, 5, 4, 3, 2, 2, 1, 1,
    ]

    for day_offset in range(num_days):
        days_back = TIME_SPAN_IN_DAYS - day_offset
        base_date = datetime.now() - timedelta(days=days_back)
        day_pool = make_daily_customer_pool(ordering_customer_ids, 1000 + day_offset, sessions_per_day)
        pool_idx = 0

        for source_idx, utm_source in enumerate(SESSION_SOURCES):
            source_count = max(1, round(sessions_per_day * SESSION_SOURCE_WEIGHTS[source_idx] / total_weight))
            for _ in range(source_count):
                session_id += 1
                hour = random.choices(range(24), weights=hour_weights)[0]
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                session_timestamp = base_date.replace(hour=hour, minute=minute, second=second)
                session_timestamps.append(session_timestamp)

                session_duration_minutes = random.randint(3, 60)
                end_timestamp = session_timestamp + timedelta(minutes=session_duration_minutes)
                customer_id = day_pool[pool_idx % len(day_pool)]
                pool_idx += 1
                ad_id = get_random_ad_id(utm_source, ad_ids_by_source)

                sessions_data.append([
                    session_id,
                    customer_id,
                    session_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_source,
                    ad_id,
                ])

    write_to_csv(app_sessions_path, headers, sessions_data)
    log_time_range("App Sessions", session_timestamps)


if __name__ == "__main__":
    generate_sessions_data()
