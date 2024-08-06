from datetime import datetime, timedelta
import random
from typing import Any, Optional, Tuple

from pydantic import Field

from data_creation.data_injection.data_generator.specs.tests.automated_tests.automated_test_spec import (
    AutomatedTestsSpec,
)
from data_creation.data_injection.data_generator.specs.tests.test_spec import TestSpec
from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    AutomatedAnomalyTestMetric,
    AutomatedAnomalyTestResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestSubTypes,
    TestTypes,
)
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class AutomatedVolumeTestsSpec(AutomatedTestsSpec):
    def generate_failed_test(self, node: dict, exception: dict, *args, **kwargs):
        return AutomatedVolumeAnomalyTestSpec(
            model_name=node["model_name"], is_anomalous=True, **exception
        )

    def generate_passed_test(self, node: dict, *args, **kwargs):
        return AutomatedVolumeAnomalyTestSpec(
            model_name=node["model_name"],
            is_anomalous=False,
        )


class AutomatedVolumeAnomalyTestSpec(TestSpec):
    test_name: str = Field("volume_anomalies", const=True)
    # test_type must be "anomaly_detection" for the data to be generated in the report
    test_type: TestTypes = Field(TestTypes.ANOMALY_DETECTION, const=True)
    # test_sub_type must be "automated" to be recognized by the UI
    test_sub_type: TestSubTypes = Field(TestSubTypes.AUTOMATED, const=True)
    test_params: Optional[dict] = None
    is_anomalous: bool = False
    growth_or_static: str = "growth"

    @property
    def description(self):
        return (
            "An automated volume test detects anomalies in the current total row count of a table. "
            "The total row count of a table is monitored accurately over time by collecting metadata on table updates from the query history."
        )

    def get_test_params(self) -> dict[str, Any]:
        test_params = dict(**self.test_params) if self.test_params else {}
        return dict(
            training_period=dict(count=14, period="day"),
            detection_period=dict(count=2, period="day"),
            anomaly_direction="BOTH",
            **test_params,
        )

    def get_result_description(self, last_metric: AutomatedAnomalyTestMetric):
        if last_metric.is_anomalous:
            return (
                f"The total row count of this table is {last_metric.value}. "
                f"The total row count was expected to be between {last_metric.min_value} and {last_metric.max_value}"
            )
        else:
            return "No anomaly detected"

    @staticmethod
    def get_timestamps(num_entries=14) -> list[Tuple[str]]:
        timestamps = []
        current_timestamp = datetime.utcnow()
        for i in range(num_entries):
            current_timestamp = current_timestamp - timedelta(days=1)
            timestamps.append(
                (
                    (current_timestamp - timedelta(days=1)).isoformat(),
                    current_timestamp.isoformat(),
                )
            )
        return list(reversed(timestamps))

    def get_growing_metrics(self) -> list[AutomatedAnomalyTestMetric]:
        num_entries = 14
        starting_value = random.randint(1000, 3000) * random.choice([10, 100, 1000])
        ending_value = round(starting_value * random.uniform(1.5, 1.9))
        mean_growth = round((ending_value - starting_value) / num_entries)
        space = round(mean_growth / num_entries * 3)

        timestamps = self.get_timestamps(num_entries)
        metrics: list[AutomatedAnomalyTestMetric] = [
            AutomatedAnomalyTestMetric(
                value=starting_value,
                min_value=starting_value - (mean_growth / random.randint(2, 3)),
                max_value=starting_value + (mean_growth * 2),
                start_time=timestamps[0][0],
                end_time=timestamps[0][1],
                bucket_start=timestamps[0][0],
                bucket_end=timestamps[0][1],
                metric_name="Total row count",
                is_anomalous=False,
            )
        ]
        last_growth = mean_growth
        for i in range(1, num_entries):
            last_metric = metrics[-1]
            growth = round(
                random.randint(last_growth - space, last_growth + space) * 1.01
            )
            value = last_metric.value + growth
            min_value = round(
                last_metric.min_value + round(growth * random.uniform(1.00, 1.01), 3)
            )
            max_value = round(
                last_metric.max_value + round(growth * random.uniform(0.99, 1.00), 3)
            )
            metrics.append(
                AutomatedAnomalyTestMetric(
                    value=value,
                    min_value=min_value,
                    max_value=max_value,
                    start_time=timestamps[i][0],
                    end_time=timestamps[i][1],
                    bucket_start=timestamps[i][0],
                    bucket_end=timestamps[i][1],
                    metric_name="Total row count",
                    is_anomalous=False,
                )
            )

        if self.is_anomalous:
            metrics_with_anomalous = []
            amount_of_anomalous_points = random.randint(1, 3)
            anomalous_metric = metrics[-1 * (amount_of_anomalous_points + 1)]
            for metric in metrics:
                metrics_with_anomalous.append(
                    AutomatedAnomalyTestMetric(
                        value=(
                            metric.value
                            if metric.value < anomalous_metric.value
                            else anomalous_metric.value
                        ),
                        min_value=metric.min_value,
                        max_value=metric.max_value,
                        start_time=metric.start_time,
                        end_time=metric.end_time,
                        bucket_start=metric.bucket_start,
                        bucket_end=metric.bucket_end,
                        metric_name="Total row count",
                        is_anomalous=True,
                    )
                )
            metrics = metrics_with_anomalous

        return metrics

    def get_static_metrics(self) -> list[AutomatedAnomalyTestMetric]:
        num_entries = 14
        value = random.randint(1000, 3000) * random.choice([10, 100, 1000])
        min_value = round(value * 0.95)
        max_value = round(value * 1.05)
        timestamps = self.get_timestamps(num_entries)
        metrics: list[AutomatedAnomalyTestMetric] = []

        for i in range(num_entries):
            metrics.append(
                AutomatedAnomalyTestMetric(
                    value=value,
                    min_value=min_value,
                    max_value=max_value,
                    start_time=timestamps[i][0],
                    end_time=timestamps[i][1],
                    bucket_start=timestamps[i][0],
                    bucket_end=timestamps[i][1],
                    metric_name="Total row count",
                    is_anomalous=False,
                )
            )

        if self.is_anomalous:
            metrics_with_anomalous = [*metrics]
            metrics_with_anomalous[-1].value = round(
                metrics_with_anomalous[-1].value * random.uniform(1.2, 1.5)
            )
            metrics_with_anomalous[-1].is_anomalous = metrics_with_anomalous[
                -1
            ].is_anomalous
            metrics = metrics_with_anomalous

        return metrics

    def generate(self, dbt_runner: SubprocessDbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        model_id = models_injector.get_model_id_from_name(self.model_name)

        injector = TestRunResultsInjector(dbt_runner)

        test = TestSchema(
            test_id=f"{self.model_name}_{self.test_name}_{self.test_sub_type.value}"
            + (f"_{self.test_column_name}" if self.test_column_name else ""),
            test_name=self.test_name,
            test_column_name=self.test_column_name,
            test_type=self.test_type,
            test_sub_type=self.test_sub_type,
            test_params=self.get_test_params(),
            description=self.description,
            model_id=model_id,
            model_name=self.model_name,
        )

        injector.inject_test(test)

        metrics = (
            self.get_growing_metrics()
            if self.growth_or_static == "growth"
            else self.get_static_metrics()
        )
        test_result = AutomatedAnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status="fail" if metrics[-1].is_anomalous else "pass",
            test_metrics=metrics,
            result_description=self.get_result_description(metrics[-1]),
        )

        injector.inject_anomaly_test_result(test, test_result)

        cur_timestamp = datetime.utcnow()
        for i in range(14):
            cur_timestamp = cur_timestamp - timedelta(days=1)
            if metrics[-1].is_anomalous:
                status = random.choice(["fail"] + ["pass"] * 3)
            else:
                status = "pass"
            prev_test_result = AutomatedAnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=[],
                result_description="",
            )
            injector.inject_anomaly_test_result(test, prev_test_result)
