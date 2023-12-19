import os
from pathlib import Path
from typing import Optional
from uuid import uuid4
from elementary.clients.dbt.dbt_runner import DbtRunner

from datetime import datetime, timedelta
from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
)
from data_creation.data_injection.data_generator.specs.tests.automated_test_spec import (
    AutomatedTestsSpec,
)
from data_creation.data_injection.data_generator.specs.tests.dbt_test_spec import (
    DbtTestSpec,
    TestStatuses,
)
from data_creation.data_injection.data_generator.specs.tests.dimension_anomaly_test_spec import (
    DimensionAnomalyTestSpec,
)
from data_creation.data_injection.data_generator.specs.tests.schema_change_test_spec import (
    SchemaChangeTestSpec,
)

from data_creation.data_injection.data_generator.test_data_generator import (
    TestDataGenerator,
)
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SchemaChangeTestResult,
    SourceFreshnessPeriod,
)
from data_creation.data_injection.injectors.tests.tests_injector import TestSubTypes
from data_creation.data_injection.utils import (
    get_values_around_middle,
    get_values_around_middle_anomalous,
    get_values_around_middle_anomalous_weekly_seasonality,
)


REPO_DIR = Path(os.path.dirname(__file__)).parent.parent.absolute()
INJECTION_DBT_PROJECT_DIR = os.path.join(
    REPO_DIR, "data_creation/data_injection/dbt_project"
)


def inject_jaffle_shop_tests(
    target: Optional[str] = None, profiles_dir: Optional[str] = None
):
    dbt_runner = DbtRunner(
        project_dir=INJECTION_DBT_PROJECT_DIR, profiles_dir=profiles_dir, target=target
    )
    dbt_runner.deps()

    start_time = datetime.now()

    generator = TestDataGenerator(dbt_runner)
    generator.delete_generated_tests()

    test_specs = [
        SchemaChangeTestSpec(
            model_name="stg_orders",
            test_name="schema_changes_from_baseline",
            results=[
                SchemaChangeTestResult(
                    test_timestamp=datetime.utcnow(),
                    column_name="order_date",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                    from_type="TIMESTAMP",
                    to_type="STRING",
                )
            ],
            from_baseline=True,
        ),
        SchemaChangeTestSpec(
            model_name="orders",
            test_name="schema_changes_from_baseline",
            results=[
                SchemaChangeTestResult(
                    test_timestamp=datetime.utcnow(),
                    column_name="order_date",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                    from_type="TIMESTAMP",
                    to_type="STRING",
                )
            ],
            from_baseline=True,
        ),
        SchemaChangeTestSpec(
            model_name="customer_conversions",
            test_name="schema_changes_from_baseline",
            results=[
                SchemaChangeTestResult(
                    test_timestamp=datetime.utcnow(),
                    column_name="converted_at",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                    from_type="TIMESTAMP",
                    to_type="STRING",
                )
            ],
            from_baseline=True,
        ),
        SchemaChangeTestSpec(
            model_name="attribution_touches",
            test_name="schema_changes_from_baseline",
            results=[
                SchemaChangeTestResult(
                    test_timestamp=datetime.utcnow(),
                    column_name="converted_at",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                    from_type="TIMESTAMP",
                    to_type="STRING",
                )
            ],
            from_baseline=True,
        ),
        SchemaChangeTestSpec(
            model_name="cpa_and_roas",
            test_name="schema_changes_from_baseline",
            results=[
                SchemaChangeTestResult(
                    test_timestamp=datetime.utcnow(),
                    column_name="date_month",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                    from_type="TIMESTAMP",
                    to_type="STRING",
                )
            ],
            from_baseline=True,
        ),
        DimensionAnomalyTestSpec(
            model_name="agg_sessions",
            test_name="dimension_anomalies",
            is_automated=False,
            metric_values=dict(
                app=get_values_around_middle_anomalous(40, 3),
                website=get_values_around_middle_anomalous(75, 14),
            ),
            timestamp_column=None,
            dimension="platform",
        ),
        DimensionAnomalyTestSpec(
            model_name="marketing_ads",
            test_name="dimension_anomalies",
            is_automated=False,
            metric_values=dict(
                google=get_values_around_middle_anomalous(20, 3),
                facebook=get_values_around_middle_anomalous(40, 5),
                instagram=get_values_around_middle_anomalous(85, 12),
            ),
            timestamp_column=None,
            dimension="utm_source",
        ),
        DbtTestSpec(
            model_name="stg_orders",
            test_name="not_null",
            test_column_name="order_id",
            status=TestStatuses.FAIL,
            result_rows=[
                dict(
                    order_date=datetime.utcnow().isoformat(),
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
                dict(
                    order_date=datetime.utcnow().isoformat(),
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
            ],
        ),
        DbtTestSpec(
            model_name="orders",
            test_name="not_null",
            test_column_name="order_id",
            status=TestStatuses.FAIL,
            result_rows=[
                dict(
                    coupon_amount=100,
                    git_card_amount=0,
                    credit_card_amount=300,
                    bank_transfer_amount=0,
                    order_date=datetime.utcnow().isoformat(),
                    amount=400,
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
                dict(
                    coupon_amount=150,
                    git_card_amount=200,
                    credit_card_amount=0,
                    bank_transfer_amount=0,
                    order_date=datetime.utcnow().isoformat(),
                    amount=350,
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
            ],
        ),
        DbtTestSpec(
            model_name="order_items",
            test_name="not_null",
            test_column_name="order_id",
            status=TestStatuses.FAIL,
            result_rows=[
                dict(
                    coupon_amount=100,
                    git_card_amount=0,
                    credit_card_amount=300,
                    bank_transfer_amount=0,
                    order_date=datetime.utcnow().isoformat(),
                    amount=400,
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
                dict(
                    coupon_amount=150,
                    git_card_amount=200,
                    credit_card_amount=0,
                    bank_transfer_amount=0,
                    order_date=datetime.utcnow().isoformat(),
                    amount=350,
                    order_id=None,
                    status="shipped",
                    customer_id=str(uuid4()),
                ),
            ],
        ),
        AutomatedTestsSpec(
            exceptions={
                ("customers", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous(70, 3),
                ),
                ("orders", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous_weekly_seasonality(
                        700, 30, 1100, is_spike=True, num_entries=95
                    ),
                    day_of_week_seasonality=True,
                ),
                ("stg_payments", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous(100000, 10000),
                ),
                ("stg_google_ads", "freshness"): dict(
                    max_loaded_at=datetime.utcnow() - timedelta(hours=9),
                    status="fail",
                    warn_after=SourceFreshnessPeriod(period="hour", count=3),
                    error_after=SourceFreshnessPeriod(period="hour", count=6),
                ),
            }
        ),
        AnomalyTestSpec(
            model_name="orders",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle_anomalous(20, 5, is_spike=True),
            timestamp_column=None,
            test_column_name="email",
            test_sub_type="missing_count",
        ),
        AnomalyTestSpec(
            model_name="returned_orders",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="order_category",
            test_sub_type="null_count",
            bucket_period="hour",
        ),
        AnomalyTestSpec(
            model_name="ads_spend",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="campaign_name",
            test_sub_type="null_count",
            bucket_period="day",
        ),
        AnomalyTestSpec(
            model_name="marketing_ads",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="impressions",
            test_sub_type="zero_count",
            bucket_period="day",
        ),
        AnomalyTestSpec(
            model_name="cpa_and_roas",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle_anomalous_weekly_seasonality(
                700, 30, 1100, is_spike=True, num_entries=95
            ),
            timestamp_column=None,
            test_column_name="revenue",
            test_sub_type="zero_count",
            bucket_period="hour",
            day_of_week_seasonality=True,
        ),
    ]
    generator.generate(test_specs)
    print(f"Done, total time: {datetime.now() - start_time}")
