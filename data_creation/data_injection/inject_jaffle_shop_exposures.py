import os
from pathlib import Path
from typing import Optional
from elementary.clients.dbt.dbt_runner import DbtRunner

from datetime import datetime
from data_creation.data_injection.data_generator.exposures_data_generator import (
    ExposuresDataGenerator,
)
from data_creation.data_injection.data_generator.specs.exposures.exposure_spec import (
    ExposureSpec,
    Column,
)


REPO_DIR = Path(os.path.dirname(__file__)).parent.parent.absolute()
INJECTION_DBT_PROJECT_DIR = os.path.join(
    REPO_DIR, "data_creation/data_injection/dbt_project"
)


def inject_jaffle_shop_exposures(
    target: Optional[str] = None, profiles_dir: Optional[str] = None
):
    dbt_runner = DbtRunner(
        project_dir=INJECTION_DBT_PROJECT_DIR, profiles_dir=profiles_dir, target=target
    )
    dbt_runner.deps()

    start_time = datetime.now()

    generator = ExposuresDataGenerator(
        dbt_runner, profiles_dir=profiles_dir, target=target
    )
    generator.delete_generated_exposures()
    exposure_specs = [
        # Orders
        ExposureSpec(
            unique_id="orders_explore",
            full_path="JaffleShop/Orders",
            name="orders explore",
            type="explore",
            label="Orders Explore",
            description="Explore of all the data needed for Orders dashboards.",
            columns=[
                Column(node_name="orders", name="order_id"),
                Column(node_name="orders", name="customer_id"),
                Column(node_name="orders", name="order_date"),
                Column(node_name="orders", name="status"),
                Column(node_name="orders", name="amount"),
            ],
            owner_name="Idan Shavit",
            owner_email="idan@jaffle.com",
            url="https://bi.tool",
            tags=["orders"],
            meta=dict(platform="Looker", path="JaffleShop/Orders"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="lost_orders_explore",
            full_path="JaffleShop/Orders",
            name="lost orders explore",
            type="explore",
            label="Lost Orders Explore",
            description="Explore of all the data needed for Lost Orders dashboards.",
            columns=[
                Column(node_name="orders", name="order_id"),
                Column(node_name="orders", name="customer_id"),
                Column(node_name="orders", name="order_date"),
                Column(node_name="orders", name="status"),
                Column(node_name="orders", name="amount"),
            ],
            owner_name="Idan Shavit",
            owner_email="idan@jaffle.com",
            url="https://bi.tool",
            tags=["orders"],
            meta=dict(platform="Looker", path="JaffleShop/Orders"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="inventory_dashboard",
            full_path="JaffleShop/Orders",
            name="inventory dashboard",
            type="dashboard",
            label="Inventory Dashboard",
            description="This analysis sums up our current inventory, missing inventory, and ordered inventory.",
            columns=[
                Column(node_id="orders_explore", name="order_id"),
                Column(
                    node_id="orders_explore",
                    name="pending_orders",
                    target_name="order_id",
                ),
                Column(
                    node_id="orders_explore",
                    name="pending_orders",
                    target_name="status",
                ),
                Column(
                    node_id="lost_orders_explore",
                    name="lost_orders",
                    target_name="order_id",
                ),
                Column(
                    node_id="lost_orders_explore",
                    name="lost_orders",
                    target_name="status",
                ),
            ],
            owner_name="Idan Shavit",
            owner_email="idan@jaffle.com",
            url="https://bi.tool",
            tags=["orders", "inventory"],
            meta=dict(platform="Looker", path="JaffleShop/Orders"),
            raw_queries=None,
        ),
        # Users
        ExposureSpec(
            unique_id="users_explore",
            full_path="JaffleShop/Users",
            name="users explore",
            type="explore",
            label="Users Explore",
            description="Explore of all the data needed for Users dashboards.",
            columns=[
                Column(node_name="customers", name="signup_date"),
                Column(node_name="customers", name="first_name"),
                Column(node_name="customers", name="last_name"),
                Column(node_name="customers", name="customer_email"),
                Column(node_name="customers", name="first_order"),
                Column(node_name="customers", name="number_of_orders"),
                Column(node_name="customers", name="customer_lifetime_value"),
                Column(node_name="customers", name="customer_id"),
                Column(node_name="customers", name="most_recent_order"),
            ],
            owner_name="Elon Gliksberg",
            owner_email="elon@jaffle.com",
            url="https://bi.tool",
            tags=["customers"],
            meta=dict(platform="Looker", path="JaffleShop/Users"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="daily_active_users_dashboard",
            full_path="JaffleShop/Users",
            name="daily active users dashboard",
            type="dashboard",
            label="Daily Active Users Dashboard",
            description="This analysis shows the daily active users on the platform.",
            columns=[
                Column(node_id="orders_explore", name="date", target_name="order_date"),
                Column(
                    node_id="users_explore", name="user_id", target_name="customer_id"
                ),
                Column(
                    node_id="users_explore", name="user_name", target_name="first_name"
                ),
                Column(
                    node_id="users_explore", name="user_name", target_name="last_name"
                ),
            ],
            owner_name="Elon Gliksberg",
            owner_email="elon@jaffle.com",
            url="https://bi.tool",
            tags=["customers", "daily"],
            meta=dict(platform="Looker", path="JaffleShop/Users"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="weekly_active_users_dashboard",
            full_path="JaffleShop/Users",
            name="weekly active users dashboard",
            type="dashboard",
            label="Weekly Active Users Dashboard",
            description="This analysis shows the weekly active users on the platform.",
            columns=[
                Column(node_id="orders_explore", name="date", target_name="order_date"),
                Column(
                    node_id="users_explore", name="user_id", target_name="customer_id"
                ),
                Column(
                    node_id="users_explore", name="user_name", target_name="first_name"
                ),
                Column(
                    node_id="users_explore", name="user_name", target_name="last_name"
                ),
            ],
            owner_name="Elon Gliksberg",
            owner_email="elon@jaffle.com",
            url="https://bi.tool",
            tags=["customers", "weekly"],
            meta=dict(platform="Looker", path="JaffleShop/Users"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="monthly_signups_dashboard",
            full_path="JaffleShop/Users",
            name="monthly signups dashboard",
            type="dashboard",
            label="Monthly Signups Dashboard",
            description="This analysis shows the monthly signups on the platform.",
            columns=[
                Column(node_id="users_explore", name="date", target_name="signup_date"),
                Column(
                    node_id="users_explore", name="amount", target_name="signup_date"
                ),
                Column(
                    node_id="users_explore", name="amount", target_name="customer_id"
                ),
            ],
            owner_name="Elon Gliksberg",
            owner_email="elon@jaffle.com",
            url="https://bi.tool",
            tags=["customers", "signups", "monthly"],
            meta=dict(platform="Looker", path="JaffleShop/Users"),
            raw_queries=None,
        ),
        # CAC
        ExposureSpec(
            unique_id="cac_explore",
            full_path="JaffleShop/Finance/CAC",
            name="cac explore",
            type="explore",
            label="CAC Explore",
            description="Explore of all the data needed for Finance CAC dashboard.",
            columns=[
                Column(node_name="marketing_ads", name="ad_id"),
                Column(node_name="marketing_ads", name="date"),
                Column(node_name="marketing_ads", name="cost"),
                Column(node_name="ads_spend", name="spend"),
                Column(node_name="ads_spend", name="date_day"),
                Column(node_name="ads_spend", name="utm_medium"),
                Column(node_name="ads_spend", name="utm_source"),
                Column(node_name="ads_spend", name="utm_campain"),
                Column(node_name="customer_conversions", name="customer_id"),
                Column(node_name="customer_conversions", name="revenue"),
            ],
            owner_name="Itamar",
            owner_email="itamar@jaffle.com",
            url="https://bi.tool",
            tags=["cac", "finance"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/CAC"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="cac_dashboard",
            full_path="JaffleShop/Finance/CAC",
            name="cac dashboard",
            type="dashboard",
            label="CAC Dashboard",
            description="This analysis shows the customer acquisition cost on the platform.",
            columns=[
                Column(node_id="cac_explore", name="total_spend", target_name="spend"),
                Column(
                    node_id="cac_explore", name="total_spend", target_name="customer_id"
                ),
                Column(
                    node_id="cac_explore",
                    name="total_cost_per_acquisition",
                    target_name="spend",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_cost_per_acquisition",
                    target_name="revenue",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_cost_per_acquisition",
                    target_name="customer_id",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_cost_per_convertion",
                    target_name="spend",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_cost_per_convertion",
                    target_name="customer_id",
                ),
                Column(
                    node_id="cac_explore",
                    name="return_on_advertising_spend",
                    target_name="spend",
                ),
                Column(
                    node_id="cac_explore",
                    name="return_on_advertising_spend",
                    target_name="ad_id",
                ),
                Column(
                    node_id="cac_explore",
                    name="return_on_advertising_spend",
                    target_name="revenue",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_spend_per_campaign:",
                    target_name="cost",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_spend_per_campaign:",
                    target_name="ad_id",
                ),
                Column(
                    node_id="cac_explore",
                    name="total_spend_per_campaign:",
                    target_name="utm_campain",
                ),
            ],
            owner_name="Itamar",
            owner_email="itamar@jaffle.com",
            url="https://bi.tool",
            tags=["cac", "finance"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/CAC"),
            raw_queries=None,
        ),
        # MRR
        ExposureSpec(
            unique_id="mrr_dashboard",
            full_path="JaffleShop/Finance/MRR",
            name="mrr dashboard",
            type="dashboard",
            label="MRR Dashboard",
            description="This analysis shows the monthly recurring revenue on the platform.",
            columns=[
                Column(node_id="orders_explore", name="date", target_name="order_date"),
                Column(
                    node_id="orders_explore", name="total_mrr", target_name="order_date"
                ),
                Column(
                    node_id="orders_explore", name="total_mrr", target_name="order_id"
                ),
                Column(
                    node_id="orders_explore", name="total_mrr", target_name="amount"
                ),
                Column(
                    node_id="orders_explore", name="new_mrr", target_name="order_date"
                ),
                Column(
                    node_id="orders_explore", name="new_mrr", target_name="order_id"
                ),
                Column(node_id="orders_explore", name="new_mrr", target_name="amount"),
                Column(
                    node_id="orders_explore",
                    name="net_mrr_growth",
                    target_name="order_date",
                ),
                Column(
                    node_id="orders_explore",
                    name="net_mrr_growth",
                    target_name="amount",
                ),
                Column(
                    node_id="orders_explore",
                    name="average_revenue",
                    target_name="order_date",
                ),
                Column(
                    node_id="orders_explore",
                    name="average_revenue",
                    target_name="amount",
                ),
                Column(
                    node_id="orders_explore",
                    name="average_revenue_per_user",
                    target_name="order_date",
                ),
                Column(
                    node_id="orders_explore",
                    name="average_revenue_per_user",
                    target_name="amount",
                ),
                Column(
                    node_id="orders_explore",
                    name="average_revenue_per_user",
                    target_name="customer_id",
                ),
            ],
            owner_name="Maayan",
            owner_email="maayan@jaffle.com",
            url="https://bi.tool",
            tags=["mrr", "finance"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/MRR"),
            raw_queries=None,
        ),
        # Ads
        ExposureSpec(
            unique_id="ads_spend_explore",
            full_path="JaffleShop/Finance/Ads",
            name="ads spend explore",
            type="explore",
            label="Ads Spend Explore",
            description="Explore of all the data needed for Finance Ads Spend dashboard.",
            columns=[
                Column(node_name="ads_spend", name="spend"),
                Column(node_name="ads_spend", name="date_day"),
                Column(node_name="ads_spend", name="utm_medium"),
                Column(node_name="ads_spend", name="utm_source"),
                Column(node_name="ads_spend", name="utm_campain"),
            ],
            owner_name="Maayan",
            owner_email="maayan@jaffle.com",
            url="https://bi.tool",
            tags=["ads", "finance"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/Ads"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="marketing_ads_explore",
            full_path="JaffleShop/Finance/Ads",
            name="marketing ads explore",
            type="explore",
            label="Marketing Ads Explore",
            description="Explore of all the data needed for Finance Marketing Ads dashboard.",
            columns=[
                Column(node_name="marketing_ads", name="date"),
                Column(node_name="marketing_ads", name="cost"),
                Column(node_name="marketing_ads", name="utm_medium"),
                Column(node_name="marketing_ads", name="utm_source"),
                Column(node_name="marketing_ads", name="utm_campain"),
                Column(node_name="marketing_ads", name="ad_id"),
            ],
            owner_name="Maayan",
            owner_email="maayan@jaffle.com",
            url="https://bi.tool",
            tags=["ads", "finance"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/Ads"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="ad_spend_roi_dashboard",
            full_path="JaffleShop/Finance/Ads",
            name="ad spend roi dashboard",
            type="dashboard",
            label="Ad Spend ROI Dashboard",
            description="This analysis shows the ad spend ROI on the platform.",
            columns=[
                Column(
                    node_id="marketing_ads_explore", name="date", target_name="date"
                ),
                Column(
                    node_id="marketing_ads_explore",
                    name="ads_cost",
                    target_name="ad_id",
                ),
                Column(
                    node_id="marketing_ads_explore", name="ads_cost", target_name="cost"
                ),
                Column(
                    node_id="ads_spend_explore",
                    name="total_ads_spend",
                    target_name="date_day",
                ),
                Column(
                    node_id="ads_spend_explore",
                    name="total_ads_spend",
                    target_name="spend",
                ),
                Column(
                    node_id="ads_spend_explore",
                    name="total_ads_spend_per_campain",
                    target_name="spend",
                ),
                Column(
                    node_id="ads_spend_explore",
                    name="total_ads_spend_per_campain",
                    target_name="utm_campain",
                ),
            ],
            owner_name="Maayan",
            owner_email="maayan@jaffle.com",
            url="https://bi.tool",
            tags=["ads", "finance", "roi"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/Ads"),
            raw_queries=None,
        ),
        # ltv
        ExposureSpec(
            unique_id="ltv_explore",
            full_path="JaffleShop/Finance/LTV",
            name="ltv explore",
            type="explore",
            label="LTV Explore",
            description="Explore of all the data needed for Finance LTV dashboard.",
            columns=[
                Column(node_name="cpa_and_roas", name="attribution_points"),
                Column(node_name="cpa_and_roas", name="cost_per_acquisition"),
                Column(node_name="cpa_and_roas", name="attribution_revenue"),
                Column(node_name="cpa_and_roas", name="utm_source"),
                Column(node_name="cpa_and_roas", name="total_spend"),
                Column(node_name="cpa_and_roas", name="date_month"),
                Column(node_name="cpa_and_roas", name="return_on_advertising_spend"),
            ],
            owner_name="Erik",
            owner_email="erik@jaffle.com",
            url="https://bi.tool",
            tags=["ltv", "finance", "funnel"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/LTV"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="ltv_forecast_dashboard",
            full_path="JaffleShop/Finance/LTV",
            name="ltv forecast",
            type="dashboard",
            label="LTV Forecast Dashboard",
            description="This analysis shows the lifetime value forecast of a customer on the platform.",
            columns=[
                Column(node_id="ltv_explore", name="date", target_name="date_month"),
                Column(
                    node_id="ltv_explore",
                    name="total_revenue",
                    target_name="date_month",
                ),
                Column(
                    node_id="ltv_explore",
                    name="total_revenue",
                    target_name="attribution_revenue",
                ),
                Column(
                    node_id="ltv_explore",
                    name="average_revenue",
                    target_name="date_month",
                ),
                Column(
                    node_id="ltv_explore",
                    name="average_revenue",
                    target_name="attribution_revenue",
                ),
                Column(
                    node_id="ltv_explore",
                    name="total_acquisition_cost",
                    target_name="date_month",
                ),
                Column(
                    node_id="ltv_explore",
                    name="total_acquisition_cost",
                    target_name="cost_per_acquisition",
                ),
                Column(
                    node_id="ltv_explore",
                    name="predicted_lifetime_value",
                    target_name="attribution_points",
                ),
                Column(
                    node_id="ltv_explore",
                    name="predicted_lifetime_value",
                    target_name="cost_per_acquisition",
                ),
                Column(
                    node_id="ltv_explore",
                    name="predicted_lifetime_value",
                    target_name="attribution_revenue",
                ),
                Column(
                    node_id="ltv_explore",
                    name="predicted_lifetime_value",
                    target_name="total_spend",
                ),
                Column(
                    node_id="ltv_explore",
                    name="predicted_lifetime_value",
                    target_name="return_on_advertising_spend",
                ),
                Column(
                    node_id="ltv_explore",
                    name="roas",
                    target_name="return_on_advertising_spend",
                ),
                Column(
                    node_id="ltv_explore",
                    name="roas",
                    target_name="date_month",
                ),
            ],
            owner_name="Erik",
            owner_email="erik@jaffle.com",
            url="https://bi.tool",
            tags=["ltv", "finance", "funnel"],
            meta=dict(platform="Looker", path="JaffleShop/Finance/LTV"),
            raw_queries=None,
        ),
    ]
    generator.generate(exposure_specs)
    print(f"Done, total time: {datetime.now() - start_time}")
