import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from elementary.clients.dbt.subprocess_dbt_runner import (
    SubprocessDbtRunner as DbtRunner,
)

from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
    PeriodSchema,
)
from data_creation.data_injection.data_generator.specs.tests.automated_tests.automated_freshness_test_spec import (
    AutomatedFreshnessTestsSpec,
)
from data_creation.data_injection.data_generator.specs.tests.automated_tests.automated_volume_test_spec import (
    AutomatedVolumeTestsSpec,
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
)
from data_creation.data_injection.injectors.tests.tests_injector import TestSubTypes
from data_creation.data_injection.utils import (
    get_values_around_middle,
    get_values_around_middle_anomalous,
    get_values_around_middle_anomalous_weekly_seasonality,
    get_values_around_middle_weekly_seasonality,
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
                    column_name="day",
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
            no_bucket=False,
            metric_values=dict(
                app=get_values_around_middle_anomalous(40, 3),
                website=get_values_around_middle_anomalous(75, 14),
            ),
            timestamp_column=None,
            dimensions="platform",
            detection_period=PeriodSchema(count=2, period="day"),
            sensitivity=3,
            max_execution_time=5.6,
        ),
        DimensionAnomalyTestSpec(
            model_name="marketing_ads",
            test_name="dimension_anomalies",
            no_bucket=False,
            metric_values=dict(
                google=get_values_around_middle_anomalous(20, 3),
                facebook=get_values_around_middle_anomalous(40, 5),
                instagram=get_values_around_middle_anomalous(85, 12),
            ),
            timestamp_column=None,
            dimensions="utm_source",
            detection_period=PeriodSchema(count=2, period="day"),
            sensitivity=3,
            max_execution_time=8.1,
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
            max_execution_time=12.6,
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
            max_execution_time=10.3,
        ),
        DbtTestSpec(
            model_name="order_items",
            test_name="not_null",
            test_column_name="order_id",
            status=TestStatuses.FAIL,
            description="The order_id column in the order_items table should not contain null values",
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
            max_execution_time=8.7,
        ),
        # We use DAG to inject the AM tests directly to the cloud_schema.
        # AutomatedVolumeTestsSpec(
        #     exceptions={
        #         "customers": dict(
        #             growth_or_static="growth",
        #         ),
        #         "orders": dict(
        #             growth_or_static="growth",
        #         ),
        #         "stg_payments": dict(
        #             growth_or_static="growth",
        #         ),
        #     }
        # ),
        # AutomatedFreshnessTestsSpec(
        #     exceptions={
        #         "stg_google_ads": dict(),
        #     }
        # ),
        AnomalyTestSpec(
            model_name="orders",
            test_name="column_anomalies",
            no_bucket=False,
            metric_values=get_values_around_middle_anomalous(20, 5, is_spike=True),
            historic_success_metric_values=get_values_around_middle(20, 5),
            historic_failure_metric_values=get_values_around_middle_anomalous(
                20, 5, is_spike=True
            ),
            timestamp_column=None,
            test_column_name="gift_card_amount",
            test_sub_type="missing_count",
            detection_period=PeriodSchema(count=2, period="day"),
            sensitivity=3,
            max_execution_time=6.7,
        ),
        AnomalyTestSpec(
            model_name="returned_orders",
            test_name="column_anomalies",
            no_bucket=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            historic_success_metric_values=get_values_around_middle(
                40, 3, num_entries=72
            ),
            historic_failure_metric_values=get_values_around_middle_anomalous(
                40, 3, num_entries=72, is_spike=True
            ),
            timestamp_column=None,
            test_column_name="amount",
            test_sub_type="null_count",
            detection_period=PeriodSchema(count=2, period="hour"),
            sensitivity=3,
            max_execution_time=5.9,
        ),
        AnomalyTestSpec(
            model_name="ads_spend",
            test_name="column_anomalies",
            no_bucket=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            historic_success_metric_values=get_values_around_middle(
                40, 3, num_entries=72
            ),
            historic_failure_metric_values=get_values_around_middle_anomalous(
                40, 3, num_entries=72, is_spike=True
            ),
            timestamp_column=None,
            test_column_name="spend",
            test_sub_type="null_count",
            detection_period=PeriodSchema(count=2, period="day"),
            sensitivity=3,
            max_execution_time=6.1,
        ),
        AnomalyTestSpec(
            model_name="marketing_ads",
            test_name="column_anomalies",
            no_bucket=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            historic_success_metric_values=get_values_around_middle(
                40, 3, num_entries=72
            ),
            historic_failure_metric_values=get_values_around_middle_anomalous(
                40, 3, num_entries=72, is_spike=True
            ),
            timestamp_column=None,
            test_column_name="cost",
            test_sub_type="zero_count",
            detection_period=PeriodSchema(count=2, period="day"),
            sensitivity=3,
            max_execution_time=7.3,
        ),
        AnomalyTestSpec(
            model_name="cpa_and_roas",
            test_name="column_anomalies",
            no_bucket=False,
            metric_values=get_values_around_middle_anomalous_weekly_seasonality(
                700, 30, 1100, is_spike=True, num_entries=72
            ),
            historic_success_metric_values=get_values_around_middle_weekly_seasonality(
                700, 30, 1100, num_entries=72
            ),
            historic_failure_metric_values=get_values_around_middle_anomalous_weekly_seasonality(
                700, 30, 1100, is_spike=True, num_entries=72
            ),
            timestamp_column=None,
            test_column_name="total_spend",
            test_sub_type="zero_count",
            detection_period=PeriodSchema(count=2, period="day"),
            day_of_week_seasonality=True,
            sensitivity=3,
            max_execution_time=4.7,
        ),
    ]
    generator.generate(test_specs)
    print(f"Done, total time: {datetime.now() - start_time}")
