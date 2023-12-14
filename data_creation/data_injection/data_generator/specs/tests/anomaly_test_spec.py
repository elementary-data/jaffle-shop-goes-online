from datetime import date, datetime, timedelta
import random
import time
from typing import Any, Optional

import numpy
from data_creation.data_injection.data_generator.specs.tests.test_spec import TestSpec
from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    AnomalyTestMetric,
    AnomalyTestResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestTypes,
)

from elementary.clients.dbt.dbt_runner import DbtRunner


class AnomalyTestSpec(TestSpec):
    is_automated: bool
    metric_values: list[float]
    timestamp_column: Optional[str] = None
    sensitivity: int = 3
    min_values_bound: int = 0
    bucket_period: str = "day"
    test_type: TestTypes = TestTypes.ANOMALY_DETECTION
    day_of_week_seasonality: bool = False

    @property
    def description(self):
        if self.test_name == "volume":
            return "Monitors the row count of your table over time."
        elif self.test_name == "column_anomalies":
            return (
                "Column-level anomaly monitors (null_count, null_percent, zero_count, string_length, "
                "variance, etc.) on the column according to its data type."
            )
        return ""

    def get_result_description(self, last_metric: AnomalyTestMetric):
        metric_average = (last_metric.min_value + last_metric.max_value) / 2
        if self.test_name == "volume":
            return f"The last volume value is {last_metric.value}, the average for this metric is {metric_average}"
        elif self.test_name == "column_anomalies":
            return (
                f"In column {self.test_column_name}, the last {self.test_sub_type} value is {last_metric.value}. "
                f"The average for this metric is {metric_average}."
            )
        return ""

    def get_metric_timestamps(self):
        if self.is_automated:
            # Not bucketed
            timestamps = []
            cur_timestamp = datetime.utcnow()
            for i in range(len(self.metric_values)):
                cur_timestamp = cur_timestamp - timedelta(
                    minutes=random.randint(3 * 60, 5 * 60)
                )
                timestamps.append((None, cur_timestamp))
            return reversed(timestamps)
        else:
            if self.bucket_period == "day":
                start_bucket = datetime.combine(
                    date.today() - timedelta(len(self.metric_values)), time.min
                )
                return [
                    (start_bucket + timedelta(i), start_bucket + timedelta(i + 1))
                    for i in range(len(self.metric_values))
                ]
            elif self.bucket_period == "hour":
                start_bucket = datetime.utcnow().replace(
                    minute=0, second=0, microsecond=0
                ) - timedelta(hours=len(self.metric_values) + 1)
                return [
                    (
                        start_bucket + timedelta(hours=i),
                        start_bucket + timedelta(hours=i + 1),
                    )
                    for i in range(len(self.metric_values))
                ]
            else:
                raise ValueError(
                    f"I don't know how to handle bucket size {self.bucket_period}"
                )

    def get_metrics(self):
        metric_timestamps = self.get_metric_timestamps()
        metrics = []

        for i, (value, (start_time, end_time)) in enumerate(
            zip(self.metric_values, metric_timestamps)
        ):
            if self.day_of_week_seasonality:
                relevant_metrics = list(reversed(self.metric_values[i:0:-7]))
                if i % 7 == 0:
                    relevant_metrics.insert(0, self.metric_values[0])
            else:
                relevant_metrics = self.metric_values[: (i + 1)]
            average = numpy.average(relevant_metrics)
            stddev = numpy.std(relevant_metrics)

            metric = AnomalyTestMetric(
                value=value,
                min_value=max(
                    average - self.sensitivity * stddev, self.min_values_bound
                ),
                max_value=average + self.sensitivity * stddev,
                start_time=start_time.isoformat() if start_time else None,
                end_time=end_time.isoformat(),
            )
            if metric.is_anomalous:
                if self.day_of_week_seasonality:
                    last_metric = metrics[-7]
                else:
                    last_metric = metrics[-1]
                metric.min_value = last_metric.min_value
                metric.max_value = last_metric.max_value

            metrics.append(metric)
        return metrics

    def generate(self, dbt_runner: DbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        model_id = models_injector.get_model_id_from_name(self.model_name)

        test_params: dict[str, Any] = {}
        if self.timestamp_column:
            test_params["timestamp_column"] = self.timestamp_column
        if not self.is_automated:
            test_params["time_bucket"] = {"period": self.bucket_period, "count": 1}
        test_params["sensitivity"] = self.sensitivity
        if self.day_of_week_seasonality:
            test_params["seasonality"] = "day_of_week"

        injector = TestRunResultsInjector(dbt_runner)

        test = TestSchema(
            test_id=f"{self.model_name}_{self.test_name}_{self.test_sub_type}"
            + (f"_{self.test_column_name}" if self.test_column_name else ""),
            test_name=self.test_name,
            test_column_name=self.test_column_name,
            test_type=self.test_type,
            test_sub_type=self.test_sub_type,
            test_params=test_params,
            description=self.description,
            model_id=model_id,
            model_name=self.model_name,
        )

        injector.inject_test(test)

        metrics = self.get_metrics()
        test_result = AnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status="fail" if metrics[-1].is_anomalous else "pass",
            test_metrics=metrics,
            result_description=self.get_result_description(metrics[-1]),
        )

        injector.inject_anomaly_test_result(test, test_result)

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            if metrics[-1].is_anomalous:
                status = random.choice(["fail"] + ["pass"] * 3)
            else:
                status = "pass"
            prev_test_result = AnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=[],
                result_description="",
            )
            injector.inject_anomaly_test_result(test, prev_test_result)
