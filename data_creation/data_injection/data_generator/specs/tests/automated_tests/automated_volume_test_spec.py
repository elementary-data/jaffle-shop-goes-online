import random
from typing import Any
from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
)
from data_creation.data_injection.data_generator.specs.tests.automated_tests.automated_test_spec import (
    AutomatedTestsSpec,
)

from data_creation.data_injection.utils import (
    get_values_around_middle,
)

from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSubTypes,
    TestTypes,
)


class AutomatedVolumeTestsSpec(AutomatedTestsSpec):
    def generate_failed_test(self, node: dict, exception: dict, *args, **kwargs):
        return AnomalyTestSpec(
            model_name=node["model_name"],
            test_name="volume_anomalies",
            no_bucket=True,
            # test_type must be "anomaly_detection" for the data to be generated in the report
            test_type=TestTypes.ANOMALY_DETECTION.value,
            # test_sub_type must be "automated" to be recognized by the UI
            test_sub_type=TestSubTypes.AUTOMATED.value,
            test_params=dict(
                training_period=dict(count=14, period="day"),
                detection_period=dict(count=2, period="day"),
                anomaly_direction="BOTH",
            ),
            **exception,
        )

    def generate_passed_test(self, node: dict, *args, **kwargs):
        return AnomalyTestSpec(
            model_name=node["model_name"],
            test_name="volume_anomalies",
            no_bucket=True,
            # test_type must be "anomaly_detection" for the data to be generated in the report
            test_type=TestTypes.ANOMALY_DETECTION.value,
            # test_sub_type must be "automated" to be recognized by the UI
            test_sub_type=TestSubTypes.AUTOMATED.value,
            test_params=dict(
                training_period=dict(count=14, period="day"),
                detection_period=dict(count=2, period="day"),
                anomaly_direction="BOTH",
            ),
            metric_values=self.get_random_values(),
        )

    @staticmethod
    def get_random_values():
        settings = random.choice([(10000, 1000), (500, 10), (3000, 300)])
        return get_values_around_middle(*settings)
