from data_injection.test_data_generator import TestDataGenerator, AnomalyTestSpec, AutomatedTestsSpec, get_values_around_middle, get_values_around_middle_anomalous, get_values_around_middle_anomalous_weekly_seasonality, SourceFreshnessPeriod
from elementary.clients.dbt.dbt_runner import DbtRunner

from datetime import datetime, timedelta


def inject_jaffle_shop_tests(dbt_runner: DbtRunner):
    start_time = datetime.now()

    generator = TestDataGenerator(dbt_runner)
    generator.delete_generated_tests()

    test_specs = [
        AutomatedTestsSpec(
            exceptions={
                ("customers", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous(70, 3),
                ),
                ("orders", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous_weekly_seasonality(700, 30, 1100, is_spike=True, num_entries=95),
                    day_of_week_seasonality=True
                ),
                ("stg_payments", "volume"): dict(
                    metric_values=get_values_around_middle_anomalous(100000, 10000),
                ),
                ("stg_google_ads", "freshness"): dict(
                    max_loaded_at=datetime.utcnow() - timedelta(hours=9),
                    status="fail",
                    warn_after=SourceFreshnessPeriod(period="hour", count=3),
                    error_after=SourceFreshnessPeriod(period="hour", count=6)
                )
            }
        ),
        AnomalyTestSpec(
            model_name="orders",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle_anomalous(20, 5, is_spike=True),
            timestamp_column=None,
            test_column_name="email",
            test_sub_type="missing_count"
        ),
        AnomalyTestSpec(
            model_name="returned_orders",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="order_category",
            test_sub_type="null_count",
            bucket_period="hour"
        ),
        AnomalyTestSpec(
            model_name="ads_spend",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="campaign_name",
            test_sub_type="null_count",
            bucket_period="day"
        ),
        AnomalyTestSpec(
            model_name="marketing_ads",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle(40, 3, num_entries=72),
            timestamp_column=None,
            test_column_name="impressions",
            test_sub_type="zero_count",
            bucket_period="day"
        ),
        AnomalyTestSpec(
            model_name="cpa_and_roas",
            test_name="column_anomalies",
            is_automated=False,
            metric_values=get_values_around_middle_anomalous_weekly_seasonality(700, 30, 1100, is_spike=True, num_entries=95),
            timestamp_column=None,
            test_column_name="revenue",
            test_sub_type="zero_count",
            bucket_period="hour",
            day_of_week_seasonality=True
        )
    ]
    generator.generate(test_specs)
    print(f"Done, total time: {datetime.now() - start_time}")
