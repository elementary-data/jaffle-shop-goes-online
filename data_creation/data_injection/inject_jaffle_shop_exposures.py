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

    generator = ExposuresDataGenerator(dbt_runner)
    generator.delete_generated_exposures()
    exposure_specs = [
        ExposureSpec(
            unique_id="cac_explore",
            full_path="JaffleShop/Finance",
            name="cac explore",
            type="explore",
            label="CAC Explore",
            description="This analysis shows the customer acquisition cost on the platform.",
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
            meta=dict(platform="Looker", path="JaffleShop/Finance"),
            raw_queries=None,
        ),
        ExposureSpec(
            unique_id="cac_dashboard",
            full_path="JaffleShop/Finance",
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
            meta=dict(platform="Looker", path="JaffleShop/Finance"),
            raw_queries=None,
        ),
    ]
    generator.generate(exposure_specs)
    print(f"Done, total time: {datetime.now() - start_time}")
