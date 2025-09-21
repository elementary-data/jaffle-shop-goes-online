import random
from datetime import datetime, timedelta
from typing import Any

import numpy
from elementary.clients.dbt.dbt_runner import DbtRunner

from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
)
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


class DimensionAnomalyTestSpec(AnomalyTestSpec):
    test_sub_type: TestSubTypes = TestSubTypes.DIMENSION
    dimensions: str
    metric_values: dict[str, list[float]]

    @property
    def description(self):
        return (
            "Monitors the frequency of values in the configured dimensions over time."
        )

    def get_test_params(self) -> dict[str, Any]:
        test_params = super().get_test_params()
        test_params["dimensions"] = self.dimensions
        return test_params

    def get_result_description(self, last_metric: DimensionAnomalyTestMetric):
        metric_average = (last_metric.min_value + last_metric.max_value) / 2
        return f"The last dimension value for dimension status - returned is {last_metric.value}. The average for this metric is {metric_average}."

    # the package returns all the metrics of the anomalous dimension values 
    def get_anmalous_metrics(self):
        metrics = []
        anomalous_metrics = []
        metrics_of_anomalous_dimension_values = []

        for dimension_value, dimension_metric_values in self.metric_values.items():
            metric_timestamps = self.get_metric_timestamps(dimension_metric_values)
            is_dimension_value_anomalous = False
            dimension_value_metrics = []
            for i, (value, (start_time, end_time)) in enumerate(
                zip(dimension_metric_values, metric_timestamps)
            ):
                if self.day_of_week_seasonality:
                    relevant_metrics = list(reversed(dimension_metric_values[i:0:-7]))
                    if i % 7 == 0:
                        relevant_metrics.insert(0, dimension_metric_values[0])
                else:
                    relevant_metrics = dimension_metric_values[: (i + 1)]
                average = numpy.average(relevant_metrics)
                stddev = numpy.std(relevant_metrics)

                metric = DimensionAnomalyTestMetric(
                    value=value,
                    min_value=max(
                        average - self.sensitivity * stddev, self.min_values_bound
                    ),
                    max_value=average + self.sensitivity * stddev,
                    metric_name="row_count",
                    average=average,
                    start_time=start_time.isoformat() if start_time else None,
                    end_time=end_time.isoformat(),
                    dimension=self.dimensions,
                    dimension_value=dimension_value,
                )
                dimension_value_metrics.append(metric)
                if metric.is_anomalous:
                    is_dimension_value_anomalous = True
                    if self.day_of_week_seasonality:
                        last_metric = metrics[-7]
                    else:
                        last_metric = metrics[-1]
                    metric.min_value = last_metric.min_value
                    metric.max_value = last_metric.max_value
                    anomalous_metrics.append(metric)
                dimension_value_metrics.append(metric)
                metrics.append(metric)

            if is_dimension_value_anomalous:
                metrics_of_anomalous_dimension_values.append(dimension_value_metrics)

        return metrics_of_anomalous_dimension_values


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
        anomalous_metrics = self.get_anmalous_metrics()
        test_result = DimensionAnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status="fail" if anomalous_metrics else "pass",
            test_metrics=anomalous_metrics,
            result_description=(
                self.get_result_description(anomalous_metrics[-1])
                if anomalous_metrics
                else ""
            ),
            execution_time=execution_time,
        )
        injector.inject_anomaly_test_result(test, test_result)

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            if anomalous_metrics and anomalous_metrics[-1].is_anomalous:
                status = random.choice(["fail"] + ["pass"] * 3)
            else:
                status = "pass"
            execution_time = execution_time * ((100 - random.uniform(1, 3)) / 100)
            prev_test_result = DimensionAnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=[] if status == "pass" else anomalous_metrics,
                result_description="",
                execution_time=execution_time,
            )
            injector.inject_anomaly_test_result(test, prev_test_result)
