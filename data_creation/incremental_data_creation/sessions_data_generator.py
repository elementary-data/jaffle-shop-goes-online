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
SESSIONS_DATA_DIRECTORY_RELATIVE_PATH = "../../jaffle_shop_online/seeds/sessions"

# Business Parameters
TIME_SPAN_IN_DAYS = 60  # Training data time span
WEBSITE_SESSIONS_COUNT = 1910  # Web platform sessions
APP_SESSIONS_COUNT = 1143  # Mobile app sessions

# Extend coverage to include validation day (for order drop ROAS detection)
VALIDATION_EXTENSION_DAYS = 1  # Extend 1 day beyond training to cover validation period

# Session Channel Sources (marketing attribution)
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
        "../../jaffle_shop_online/seeds/ads",
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

    generate_website_sessions(ad_ids_by_source)
    generate_app_sessions(ad_ids_by_source)


def generate_website_sessions(ad_ids_by_source):
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

    for session_id in range(1, WEBSITE_SESSIONS_COUNT + 1):
        # Generate uniform timestamp distribution across extended period (training + validation)
        # This prevents attribution bias toward recent periods and covers the validation day
        days_back = random.randint(-VALIDATION_EXTENSION_DAYS, TIME_SPAN_IN_DAYS)
        base_datetime = datetime.now() - timedelta(days=days_back)

        # Weight hours toward peak usage times (similar to order patterns)
        # Food delivery platforms see peak traffic around meal times
        hour = random.choices(
            range(24),
            weights=[
                1,  # 00:00 - Low traffic
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
                9,  # 12:00 - Peak lunch traffic
                8,  # 13:00
                7,  # 14:00
                6,  # 15:00
                5,  # 16:00
                4,  # 17:00 - Dinner prep time
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

        session_timestamp = base_datetime.replace(
            hour=hour, minute=minute, second=second
        )
        session_timestamps.append(session_timestamp)

        # Generate marketing attribution data
        utm_source = random.choice(SESSION_SOURCES)

        # Generate session duration (5-120 minutes)
        session_duration_minutes = random.randint(5, 120)
        end_timestamp = session_timestamp + timedelta(minutes=session_duration_minutes)

        # Generate customer ID (assuming customers 1-2000 exist from training data)
        customer_id = random.randint(1, 2000)

        # Get actual ad ID for proper attribution linkage
        ad_id = get_random_ad_id(utm_source, ad_ids_by_source)

        sessions_data.append(
            [
                session_id,  # SESSION_ID
                customer_id,  # CUSTOMER_ID
                session_timestamp.strftime("%Y-%m-%d %H:%M:%S"),  # STARTED_AT
                end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),  # ENDED_AT
                utm_source,  # UTM_SOURCE (attribution)
                ad_id,  # AD_ID (ad attribution)
            ]
        )

    write_to_csv(website_sessions_path, headers, sessions_data)
    log_time_range("Website Sessions", session_timestamps)


def generate_app_sessions(ad_ids_by_source):
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

    for session_id in range(1, APP_SESSIONS_COUNT + 1):
        # Generate uniform timestamp distribution across extended period (training + validation)
        # Matches website session timing for consistent attribution analysis
        days_back = random.randint(-VALIDATION_EXTENSION_DAYS, TIME_SPAN_IN_DAYS)
        base_datetime = datetime.now() - timedelta(days=days_back)

        # Mobile app usage patterns (slightly different from web)
        # Mobile apps often see more distributed usage throughout the day
        hour = random.choices(
            range(24),
            weights=[
                1,  # 00:00
                1,  # 01:00
                1,  # 02:00
                1,  # 03:00
                2,  # 04:00
                3,  # 05:00
                4,  # 06:00
                5,  # 07:00
                6,  # 08:00 - Morning commute
                7,  # 09:00
                8,  # 10:00
                9,  # 11:00
                10,  # 12:00 - Peak lunch (mobile convenience)
                9,  # 13:00
                8,  # 14:00
                7,  # 15:00
                6,  # 16:00
                5,  # 17:00 - Evening commute
                4,  # 18:00
                3,  # 19:00
                2,  # 20:00
                2,  # 21:00
                1,  # 22:00
                1,  # 23:00
            ],
        )[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        session_timestamp = base_datetime.replace(
            hour=hour, minute=minute, second=second
        )
        session_timestamps.append(session_timestamp)

        # Generate marketing attribution data (mobile-focused sources)
        utm_source = random.choice(SESSION_SOURCES)

        # Generate session duration (3-60 minutes for mobile)
        session_duration_minutes = random.randint(3, 60)
        end_timestamp = session_timestamp + timedelta(minutes=session_duration_minutes)

        # Generate customer ID (assuming customers 1-2000 exist from training data)
        customer_id = random.randint(1, 2000)

        # Get actual ad ID for proper attribution linkage (same pool as website)
        ad_id = get_random_ad_id(utm_source, ad_ids_by_source)

        sessions_data.append(
            [
                session_id,  # SESSION_ID
                customer_id,  # CUSTOMER_ID
                session_timestamp.strftime("%Y-%m-%d %H:%M:%S"),  # STARTED_AT
                end_timestamp.strftime("%Y-%m-%d %H:%M:%S"),  # ENDED_AT
                utm_source,  # UTM_SOURCE (attribution)
                ad_id,  # AD_ID (mobile ad attribution)
            ]
        )

    write_to_csv(app_sessions_path, headers, sessions_data)
    log_time_range("App Sessions", session_timestamps)


if __name__ == "__main__":
    generate_sessions_data()
