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
from elementary.clients.dbt.dbt_runner import DbtRunner


class AutomatedFreshnessTestsSpec(AutomatedTestsSpec):
    def generate_failed_test(self, node: dict, exception: dict, *args, **kwargs):
        return AutomatedFreshnessAnomalyTestSpec(
            model_name=node["model_name"], is_anomalous=True, **exception
        )

    def generate_passed_test(self, node: dict, *args, **kwargs):
        return AutomatedFreshnessAnomalyTestSpec(
            model_name=node["model_name"],
            is_anomalous=False,
        )


class AutomatedFreshnessAnomalyTestSpec(TestSpec):
    test_name: str = Field("freshness_anomalies", const=True)
    # test_type must be "anomaly_detection" for the data to be generated in the report
    test_type: TestTypes = Field(TestTypes.ANOMALY_DETECTION, const=True)
    # test_sub_type must be "automated" to be recognized by the UI
    test_sub_type: TestSubTypes = Field(TestSubTypes.AUTOMATED, const=True)
    test_params: Optional[dict] = None
    is_anomalous: bool = False

    @property
    def description(self):
        return "An automated freshness test determines whether the table has not been updated by validating if the time elapsed since the last update significantly exceeds the typical interval between updates."

    def get_test_params(self) -> dict[str, Any]:
        test_params = dict(**self.test_params) if self.test_params else {}
        return dict(
            training_period=dict(count=14, period="day"),
            detection_period=dict(count=2, period="day"),
            anomaly_direction="SPIKE",
            **test_params,
        )

    def get_result_description(self, last_metric: AutomatedAnomalyTestMetric):
        if last_metric.anomalous:
            return (
                f"The maximum time between updates expected to be up to {round(last_metric.max_value / 60 / 60, 1)} hours. "
                f"The time elapsed since the last update is {round(last_metric.value / 60 / 60, 1)}"
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

    def get_metrics(self) -> list[AutomatedAnomalyTestMetric]:
        num_entries = 14

        timestamps = self.get_timestamps(num_entries)
        metrics: list[AutomatedAnomalyTestMetric] = []
        for i in range(num_entries):
            value = round(random.uniform(22.5, 23.9) * 60 * 60)
            metrics.append(
                AutomatedAnomalyTestMetric(
                    value=value,
                    min_value=0,
                    max_value=24 * 60 * 60,
                    start_time=timestamps[i][0],
                    end_time=timestamps[i][1],
                    bucket_start=timestamps[i][0],
                    bucket_end=timestamps[i][1],
                    metric_name="Table updates over time",
                    is_anomalous=False,
                )
            )

        if self.is_anomalous:
            metrics[-1].value = round(random.uniform(24.5, 27.0) * 60 * 60)
            metrics[-1].is_anomalous = True

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
        test_result = AutomatedAnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status="fail" if metrics[-1].anomalous else "pass",
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
