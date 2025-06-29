from datetime import datetime, timedelta
import os
import random
from utils.csv import write_to_csv

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
JAFFLE_SHOP_SEEDS_PATH = "../../jaffle_shop_online/seeds"

# Configuration
TIME_SPAN_IN_DAYS = 60
ADS_PER_PLATFORM = 100
MIN_DAILY_COST = 5
MAX_DAILY_COST = 500

# UTM Medium options
UTM_MEDIUMS = ["paid_search", "cpc", "social", "display", "video", "shopping"]

# Campaign names
CAMPAIGNS = [
    "spring_sale",
    "summer_promo",
    "back_to_school",
    "holiday_deals",
    "brand_awareness",
    "retargeting",
    "new_product_launch",
    "customer_acquisition",
    "loyalty_program",
    "flash_sale",
]


def generate_ads_data():
    """Generate advertising data for all three platforms"""
    generate_google_ads_data()
    generate_facebook_ads_data()
    generate_instagram_ads_data()


def generate_google_ads_data():
    """Generate Google Ads data"""
    ads_data = []
    headers = ["ad_id", "date", "utm_medium", "utm_campain", "cost"]

    # Generate ads for Google
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_id = f"g{ad_num}"

        # Random start date within the year
        start_date = datetime.now() - timedelta(
            days=random.randint(30, TIME_SPAN_IN_DAYS)
        )

        # Generate daily data from start date to now
        current_date = start_date
        daily_cost = random.randint(MIN_DAILY_COST, MAX_DAILY_COST)
        utm_medium = random.choice(UTM_MEDIUMS)
        campaign = random.choice(CAMPAIGNS)

        while current_date <= datetime.now():
            # Vary cost slightly day by day
            cost_variation = random.uniform(0.8, 1.2)
            daily_cost_adjusted = int(daily_cost * cost_variation)

            # Generate random time within the day (business hours weighted)
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

            timestamp = current_date.replace(hour=hour, minute=minute, second=second)

            ads_data.append(
                [
                    ad_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_medium,
                    campaign,
                    daily_cost_adjusted,
                ]
            )

            current_date += timedelta(days=1)

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH, JAFFLE_SHOP_SEEDS_PATH, "ads", "stg_google_ads.csv"
    )
    write_to_csv(output_path, headers, ads_data)
    print(f"Generated {len(ads_data)} Google ads records")


def generate_facebook_ads_data():
    """Generate Facebook Ads data"""
    ads_data = []
    headers = ["ad_id", "date", "utm_medium", "utm_campain", "cost"]

    # Generate ads for Facebook
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_id = f"f{ad_num}"

        # Random start date within the year
        start_date = datetime.now() - timedelta(
            days=random.randint(30, TIME_SPAN_IN_DAYS)
        )

        # Generate daily data from start date to now
        current_date = start_date
        daily_cost = random.randint(MIN_DAILY_COST, MAX_DAILY_COST)
        utm_medium = random.choice(UTM_MEDIUMS)
        campaign = random.choice(CAMPAIGNS)

        while current_date <= datetime.now():
            # Vary cost slightly day by day
            cost_variation = random.uniform(0.8, 1.2)
            daily_cost_adjusted = int(daily_cost * cost_variation)

            # Generate random time within the day (business hours weighted)
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

            timestamp = current_date.replace(hour=hour, minute=minute, second=second)

            ads_data.append(
                [
                    ad_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_medium,
                    campaign,
                    daily_cost_adjusted,
                ]
            )

            current_date += timedelta(days=1)

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH, JAFFLE_SHOP_SEEDS_PATH, "ads", "stg_facebook_ads.csv"
    )
    write_to_csv(output_path, headers, ads_data)
    print(f"Generated {len(ads_data)} Facebook ads records")


def generate_instagram_ads_data():
    """Generate Instagram Ads data"""
    ads_data = []
    headers = ["ad_id", "date", "utm_medium", "utm_campain", "cost"]

    # Generate ads for Instagram
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_id = f"i{ad_num}"

        # Random start date within the year
        start_date = datetime.now() - timedelta(
            days=random.randint(30, TIME_SPAN_IN_DAYS)
        )

        # Generate daily data from start date to now
        current_date = start_date
        daily_cost = random.randint(MIN_DAILY_COST, MAX_DAILY_COST)
        utm_medium = random.choice(UTM_MEDIUMS)
        campaign = random.choice(CAMPAIGNS)

        while current_date <= datetime.now():
            # Vary cost slightly day by day
            cost_variation = random.uniform(0.8, 1.2)
            daily_cost_adjusted = int(daily_cost * cost_variation)

            # Generate random time within the day (business hours weighted)
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

            timestamp = current_date.replace(hour=hour, minute=minute, second=second)

            ads_data.append(
                [
                    ad_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    utm_medium,
                    campaign,
                    daily_cost_adjusted,
                ]
            )

            current_date += timedelta(days=1)

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH, JAFFLE_SHOP_SEEDS_PATH, "ads", "stg_instagram_ads.csv"
    )
    write_to_csv(output_path, headers, ads_data)
    print(f"Generated {len(ads_data)} Instagram ads records")


def get_all_ad_ids():
    """Get all ad IDs that will be generated (for use by session generator)"""
    ad_ids = []

    # Google ads
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_ids.append(f"g{ad_num}")

    # Facebook ads
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_ids.append(f"f{ad_num}")

    # Instagram ads
    for ad_num in range(1, ADS_PER_PLATFORM + 1):
        ad_ids.append(f"i{ad_num}")

    return ad_ids


if __name__ == "__main__":
    generate_ads_data()
