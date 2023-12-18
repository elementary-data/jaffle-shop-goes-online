from datetime import datetime, timedelta
import random
from typing import Any

import numpy
from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
)
from data_creation.data_injection.data_generator.specs.tests.test_spec import TestSpec
from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    DimensionAnomalyTestMetric,
    DimensionAnomalyTestResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestSubTypes,
)

from elementary.clients.dbt.dbt_runner import DbtRunner


class DimensionAnomalyTestSpec(AnomalyTestSpec):
    test_sub_type: TestSubTypes = TestSubTypes.DIMENSION
    dimension: str
    metric_values: dict[str, list[float]]

    @property
    def description(self):
        return (
            "Monitors the frequency of values in the configured dimensions over time."
        )

    def get_test_params(self) -> dict[str, Any]:
        test_params = super().get_test_params()
        test_params["dimension"] = self.dimension
        return test_params

    def get_result_description(self, last_metric: DimensionAnomalyTestMetric):
        metric_average = (last_metric.min_value + last_metric.max_value) / 2
        return f"The last dimension value for dimension status - returned is {last_metric.value}. The average for this metric is {metric_average}."

    def get_metrics(self):
        metric_timestamps = self.get_metric_timestamps()
        metrics = []

        for dimension_value, dimension_metric_values in self.metric_values.items():
            for i, (value, (start_time, end_time)) in enumerate(
                zip(dimension_metric_values, metric_timestamps)
            ):
                if self.day_of_week_seasonality:
                    relevant_metrics = list(reversed(self.metric_values[i:0:-7]))
                    if i % 7 == 0:
                        relevant_metrics.insert(0, self.metric_values[0])
                else:
                    relevant_metrics = self.metric_values[: (i + 1)]
                average = numpy.average(relevant_metrics)
                stddev = numpy.std(relevant_metrics)

                metric = DimensionAnomalyTestMetric(
                    value=value,
                    min_value=max(
                        average - self.sensitivity * stddev, self.min_values_bound
                    ),
                    max_value=average + self.sensitivity * stddev,
                    start_time=start_time.isoformat() if start_time else None,
                    end_time=end_time.isoformat(),
                    dimension=self.dimension,
                    dimension_value=dimension_value,
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
            test_id=f"{self.model_name}_{self.test_name}_{self.test_sub_type}"
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
        test_result = DimensionAnomalyTestResult(
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
            prev_test_result = DimensionAnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=[],
                result_description="",
            )
            injector.inject_anomaly_test_result(test, prev_test_result)
