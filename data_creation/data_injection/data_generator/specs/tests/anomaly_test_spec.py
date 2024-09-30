import random
from datetime import date, datetime, time, timedelta
from typing import Any, Optional

import numpy
from elementary.clients.dbt.dbt_runner import DbtRunner
from pydantic import BaseModel

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


class PeriodSchema(BaseModel):
    count: int
    period: str


class AnomalyTestSpec(TestSpec):
    no_bucket: bool
    metric_values: list[float]
    historic_success_metric_values: list[float] = []
    historic_failure_metric_values: list[float] = []
    timestamp_column: Optional[str] = None
    sensitivity: Optional[int] = None
    min_values_bound: int = 0
    test_type: TestTypes = TestTypes.ANOMALY_DETECTION
    day_of_week_seasonality: bool = False
    test_params: Optional[dict] = None
    detection_period: PeriodSchema
    detection_delay: PeriodSchema = None

    @property
    def description(self):
        if self.test_name == "volume":
            return "Monitors the row count of your table over time."
        elif self.test_name == "column_anomalies":
            return (
                "Column-level anomaly monitors (null_count, null_percent, zero_count, string_length, "
                "variance, etc.) on the column according to its data type."
            )
        return (
            "Elementary test is an advance dbt test that is used to validate your data"
        )

    def get_test_params(self) -> dict[str, Any]:
        test_params = dict(**self.test_params) if self.test_params else {}
        if self.timestamp_column:
            test_params["timestamp_column"] = self.timestamp_column
        if not self.no_bucket:
            test_params["detection_period"] = self.detection_period.dict()
            if self.detection_delay:
                test_params["detection_delay"] = self.detection_delay.dict()
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
                f"In column {self.test_column_name}, the last {self.test_sub_type.value} value is {last_metric.value}. "
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
            if self.detection_period.period == "day":
                start_bucket = datetime.combine(
                    date.today() - timedelta(len(metric_values)), time.min
                )
                return [
                    (start_bucket + timedelta(i), start_bucket + timedelta(i + 1))
                    for i in range(len(metric_values))
                ]
            elif self.detection_period.period == "hour":
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
                    f"I don't know how to handle bucket size {self.detection_period.period}"
                )

    def get_metrics(self, metric_values: list[float]):
        metric_timestamps = self.get_metric_timestamps(metric_values)
        metrics = []
        sensitivity = self.sensitivity or 3

        for i, (value, (start_time, end_time)) in enumerate(
            zip(metric_values, metric_timestamps)
        ):
            if self.day_of_week_seasonality:
                relevant_metrics = list(reversed(metric_values[i:0:-7]))
                if i % 7 == 0:
                    relevant_metrics.insert(0, metric_values[0])
            else:
                relevant_metrics = metric_values[: (i + 1)]
            average = numpy.average(relevant_metrics)
            stddev = numpy.std(relevant_metrics)

            metric = AnomalyTestMetric(
                value=value,
                min_value=max(average - sensitivity * stddev, self.min_values_bound),
                max_value=average + sensitivity * stddev,
                start_time=start_time.isoformat() if start_time else None,
                end_time=end_time.isoformat(),
                metric_name=self.test_sub_type.value,
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

        execution_time = self.max_execution_time
        metrics = self.get_metrics(self.metric_values)
        test_result = AnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status="fail" if metrics[-1].is_anomalous else "pass",
            test_metrics=metrics,
            result_description=self.get_result_description(metrics[-1]),
            execution_time=execution_time,
        )
        injector.inject_anomaly_test_result(test, test_result)

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            if metrics[-1].is_anomalous:
                status = random.choice(["fail"] + ["pass"] * 3)
            else:
                status = "pass"
            metric_values = (
                self.historic_success_metric_values
                if status == "pass"
                else self.historic_failure_metric_values
            )
            execution_time = execution_time * ((100 - random.uniform(1, 3)) / 100)
            prev_test_result = AnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=self.get_metrics(metric_values),
                result_description="",
                execution_time=execution_time,
            )
            injector.inject_anomaly_test_result(test, prev_test_result)
