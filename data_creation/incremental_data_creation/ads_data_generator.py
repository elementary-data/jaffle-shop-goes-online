"""
JAFFLE SHOP ADS DATA GENERATOR

This script generates realistic advertising campaign data for marketing attribution and
spend analysis. It creates ad performance data across multiple platforms and campaigns.

PURPOSE:
- Creates advertising campaign data for marketing attribution modeling
- Generates realistic ad spend, impressions, and clicks data
- Simulates multi-platform advertising campaigns (Google, Facebook, Instagram)
- Provides foundation for marketing ROI and attribution analysis

BUSINESS CONTEXT:
- Jaffle Shop: Multi-platform advertising for online sandwich ordering
- Platforms: Google Ads, Facebook Ads, Instagram Ads
- Campaign types: Search, display, social media advertising
- Performance metrics: Spend, impressions, clicks, CPM, CPC

ATTRIBUTION CONTEXT:
- Ad data supports marketing attribution analysis
- Links to sessions data via utm_source tracking
- Enables cost per acquisition (CPA) and ROAS calculations
- Provides budget optimization insights

GENERATED FILES:
- stg_google_ads.csv: Google Ads campaign data
- stg_facebook_ads.csv: Facebook Ads campaign data
- stg_instagram_ads.csv: Instagram Ads campaign data

IMPORTANT NOTES:
- Ad IDs are used for attribution tracking across systems
- Spend amounts are realistic for small-medium food delivery business
- Performance metrics follow platform-typical patterns
- Campaign dates align with session and order data timing
"""

import random
import os
from datetime import datetime, timedelta
import uuid
from utils.csv import write_to_csv

# =============================================================================
# CONFIGURATION PARAMETERS
# =============================================================================

CURRENT_DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
ADS_DATA_DIRECTORY_RELATIVE_PATH = "../../jaffle_shop_online/seeds/ads"

# Business Parameters
TIME_SPAN_IN_DAYS = 60  # Training data time span
DAILY_RECORDS_PER_PLATFORM = 3  # Ad records per platform per day

# Extend coverage to include validation day (for order drop ROAS detection)
VALIDATION_EXTENSION_DAYS = 1  # Extend 1 day beyond training to cover validation period
TOTAL_TIME_SPAN = TIME_SPAN_IN_DAYS + VALIDATION_EXTENSION_DAYS  # 61 days total

# Platform-specific configurations
GOOGLE_ADS_CONFIG = {
    "platform": "google",
    "campaign_types": ["search", "display", "shopping"],
    "avg_cpc": 0.85,  # Average cost per click
    "avg_spend_per_day": 150.0,  # Average daily spend
    "spend_variance": 0.3,  # Spend variability (30%)
}

FACEBOOK_ADS_CONFIG = {
    "platform": "facebook",
    "campaign_types": ["feed", "stories", "video"],
    "avg_cpc": 0.65,  # Average cost per click
    "avg_spend_per_day": 120.0,  # Average daily spend
    "spend_variance": 0.4,  # Spend variability (40%)
}

INSTAGRAM_ADS_CONFIG = {
    "platform": "instagram",
    "campaign_types": ["feed", "stories", "reels"],
    "avg_cpc": 0.55,  # Average cost per click
    "avg_spend_per_day": 100.0,  # Average daily spend
    "spend_variance": 0.35,  # Spend variability (35%)
}


def generate_ad_id(platform, campaign_type):
    """
    Generate a unique ad ID for tracking across attribution systems

    Args:
        platform (str): Advertising platform (google, facebook, instagram)
        campaign_type (str): Type of campaign (search, display, feed, etc.)

    Returns:
        str: Unique ad ID formatted as platform_campaign_uuid
    """
    return f"{platform[0]}{campaign_type[0]}{str(uuid.uuid4())[:8]}"


def generate_ads_data():
    """
    Main function to generate all advertising data

    Generates ad performance data for all platforms:
    1. Google Ads (search and display campaigns)
    2. Facebook Ads (social media campaigns)
    3. Instagram Ads (visual content campaigns)

    Each platform has realistic spend patterns and performance metrics
    """
    generate_google_ads_data()
    generate_facebook_ads_data()
    generate_instagram_ads_data()


def generate_platform_ads_data(platform_config, output_filename):
    """
    Generate advertising data for a specific platform

    Args:
        platform_config (dict): Platform-specific configuration
        output_filename (str): Output CSV filename

    Creates realistic ad campaign data with:
    - Daily spend variations within platform norms
    - Realistic click-through rates and conversion metrics
    - Campaign performance following platform patterns
    - Proper attribution tracking IDs
    """
    ads_data = []
    headers = [
        "ad_id",
        "date",
        "utm_medium",
        "utm_campain",
        "cost",
    ]

    # Generate data for each day in the extended time span (training + validation period)
    # Start from TIME_SPAN_IN_DAYS back, go down to -VALIDATION_EXTENSION_DAYS (inclusive) to cover validation day
    for day in range(TIME_SPAN_IN_DAYS, -VALIDATION_EXTENSION_DAYS - 1, -1):
        current_date = datetime.now() - timedelta(days=day)

        # Generate multiple ad records per day (different campaigns)
        for record in range(DAILY_RECORDS_PER_PLATFORM):
            # Random campaign type for variety
            campaign_type = random.choice(platform_config["campaign_types"])

            # Generate unique ad ID for attribution tracking
            ad_id = generate_ad_id(platform_config["platform"], campaign_type)

            # Generate realistic campaign name
            campaign_name = f"{platform_config['platform']}_{campaign_type}_campaign_{random.randint(1, 10)}"

            # Generate realistic daily spend with variance
            base_spend = (
                platform_config["avg_spend_per_day"] / DAILY_RECORDS_PER_PLATFORM
            )
            spend_variance = base_spend * platform_config["spend_variance"]
            daily_spend = base_spend + random.uniform(-spend_variance, spend_variance)
            daily_spend = max(10.0, round(daily_spend, 2))  # Minimum $10 spend

            # Generate realistic impressions based on spend
            # Higher spend typically generates more impressions
            impressions = int(
                daily_spend * random.uniform(50, 150)
            )  # 50-150 impressions per dollar

            # Generate realistic clicks based on impressions
            # Click-through rate varies by platform and campaign type
            if platform_config["platform"] == "google":
                ctr = random.uniform(0.02, 0.08)  # 2-8% CTR for Google
            elif platform_config["platform"] == "facebook":
                ctr = random.uniform(0.01, 0.05)  # 1-5% CTR for Facebook
            else:  # Instagram
                ctr = random.uniform(0.01, 0.04)  # 1-4% CTR for Instagram

            clicks = int(impressions * ctr)

            # Validate that spend aligns with clicks (basic CPC check)
            if clicks > 0:
                calculated_cpc = daily_spend / clicks
                # Adjust spend if CPC is unrealistic
                if calculated_cpc > platform_config["avg_cpc"] * 2:
                    daily_spend = (
                        clicks * platform_config["avg_cpc"] * random.uniform(0.8, 1.2)
                    )
                    daily_spend = round(daily_spend, 2)

                    # Generate UTM tracking parameters
            utm_medium = random.choice(
                ["cpc", "display", "social", "video", "shopping"]
            )

            # Generate UTM campaign with the typo as expected by the model
            utm_campain = f"{campaign_name.lower().replace(' ', '_')}"

            ads_data.append(
                [
                    ad_id,  # AD_ID (attribution tracking)
                    current_date.strftime("%Y-%m-%d"),  # DATE
                    utm_medium,  # UTM_MEDIUM (traffic medium)
                    utm_campain,  # UTM_CAMPAIN (campaign name with typo as expected)
                    daily_spend,  # COST (daily spend)
                ]
            )

    # Write to CSV
    output_path = os.path.join(
        CURRENT_DIRECTORY_PATH, ADS_DATA_DIRECTORY_RELATIVE_PATH, output_filename
    )
    write_to_csv(output_path, headers, ads_data)

    # Log generation summary
    total_spend = sum(float(row[4]) for row in ads_data)  # cost is at index 4

    print(f"{platform_config['platform'].title()} Ads Generated:")
    print(f"  Records: {len(ads_data)}")
    print(f"  Total Spend: ${total_spend:,.2f}")


def generate_google_ads_data():
    """
    Generate Google Ads campaign data

    Creates Google Ads data with:
    - Search, display, and shopping campaigns
    - Higher CPC typical of Google Ads platform
    - Search-focused campaign performance patterns
    - Professional B2B and consumer targeting

    Output: stg_google_ads.csv
    Schema: ad_id, date, utm_medium, utm_campain, cost
    """
    generate_platform_ads_data(GOOGLE_ADS_CONFIG, "stg_google_ads.csv")


def generate_facebook_ads_data():
    """
    Generate Facebook Ads campaign data

    Creates Facebook Ads data with:
    - Feed, stories, and video campaigns
    - Social media engagement patterns
    - Community and interest-based targeting
    - Visual content performance metrics

    Output: stg_facebook_ads.csv
    Schema: ad_id, date, utm_medium, utm_campain, cost
    """
    generate_platform_ads_data(FACEBOOK_ADS_CONFIG, "stg_facebook_ads.csv")


def generate_instagram_ads_data():
    """
    Generate Instagram Ads campaign data

    Creates Instagram Ads data with:
    - Feed, stories, and reels campaigns
    - Visual-first content performance
    - Younger demographic targeting patterns
    - Mobile-optimized campaign metrics

    Output: stg_instagram_ads.csv
    Schema: ad_id, date, utm_medium, utm_campain, cost
    """
    generate_platform_ads_data(INSTAGRAM_ADS_CONFIG, "stg_instagram_ads.csv")


def get_all_ad_ids():
    """
    Utility function to get all ad IDs across platforms

    Returns:
        list: All ad IDs from all platforms for attribution tracking

    Note: This function is used by other modules for attribution linking
    """
    all_ad_ids = []

    # This is a simplified version - in practice you'd read from generated files
    # or maintain a registry of ad IDs for attribution tracking
    for platform_config in [
        GOOGLE_ADS_CONFIG,
        FACEBOOK_ADS_CONFIG,
        INSTAGRAM_ADS_CONFIG,
    ]:
        for campaign_type in platform_config["campaign_types"]:
            for _ in range(TIME_SPAN_IN_DAYS * DAILY_RECORDS_PER_PLATFORM):
                all_ad_ids.append(
                    generate_ad_id(platform_config["platform"], campaign_type)
                )

    return all_ad_ids


if __name__ == "__main__":
    generate_ads_data()
