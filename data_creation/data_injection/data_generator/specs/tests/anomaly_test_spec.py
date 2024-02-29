from datetime import date, datetime, time, timedelta
import random
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
    no_bucket: bool
    metric_values: list[float]
    timestamp_column: Optional[str] = None
    sensitivity: Optional[int] = None
    min_values_bound: int = 0
    bucket_period: str = "day"
    test_type: TestTypes = TestTypes.ANOMALY_DETECTION
    day_of_week_seasonality: bool = False
    test_params: Optional[dict] = None

    @property
    def description(self):
        if self.test_name == "volume":
            return "Monitors the row count of your table over time."
        elif self.test_name == "column_anomalies":
            return (
                "Column-level anomaly monitors (null_count, null_percent, zero_count, string_length, "
                "variance, etc.) on the column according to its data type."
            )
        elif (
            self.test_type.value == "anomaly_detection"
            and self.test_sub_type.value == "automated"
            and self.test_name == "volume_anomalies"
        ):
            return (
                "An automated volume test detects anomalies in the current total row count of a table. "
                "The total row count of a table is monitored accurately over time by collecting metadata on table updates from the query history."
            )
        return (
            "Elementary test is an advance dbt test that is used to validate your data"
        )

    def get_test_params(self) -> dict[str, Any]:
        test_params = dict(**self.test_params) if self.test_params else {}
        if self.timestamp_column:
            test_params["timestamp_column"] = self.timestamp_column
        if not self.no_bucket:
            test_params["time_bucket"] = {"period": self.bucket_period, "count": 1}
        if self.sensitivity:
            test_params["sensitivity"] = self.sensitivity
        if self.day_of_week_seasonality:
            test_params["seasonality"] = "day_of_week"
        return test_params

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

    def get_metric_timestamps(self, metric_values: list):
        if self.no_bucket:
            timestamps = []
            cur_timestamp = datetime.utcnow()
            for i in range(len(metric_values)):
                cur_timestamp = cur_timestamp - timedelta(days=1)
                timestamps.append((None, cur_timestamp))
            return reversed(timestamps)
        else:
            if self.bucket_period == "day":
                start_bucket = datetime.combine(
                    date.today() - timedelta(len(metric_values)), time.min
                )
                return [
                    (start_bucket + timedelta(i), start_bucket + timedelta(i + 1))
                    for i in range(len(metric_values))
                ]
            elif self.bucket_period == "hour":
                start_bucket = datetime.utcnow().replace(
                    minute=0, second=0, microsecond=0
                ) - timedelta(hours=len(metric_values) + 1)
                return [
                    (
                        start_bucket + timedelta(hours=i),
                        start_bucket + timedelta(hours=i + 1),
                    )
                    for i in range(len(metric_values))
                ]
            else:
                raise ValueError(
                    f"I don't know how to handle bucket size {self.bucket_period}"
                )

    def get_metrics(self):
        metric_timestamps = self.get_metric_timestamps(self.metric_values)
        metrics = []
        sensitivity = self.sensitivity or 3

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
                min_value=max(average - sensitivity * stddev, self.min_values_bound),
                max_value=average + sensitivity * stddev,
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
