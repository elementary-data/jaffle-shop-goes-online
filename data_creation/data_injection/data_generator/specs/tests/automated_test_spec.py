from datetime import datetime, timedelta
import random
from typing import Union
from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec

from elementary.clients.dbt.dbt_runner import DbtRunner
from data_creation.data_injection.data_generator.specs.source_freshness_spec import (
    SourceFreshnessSpec,
)
from data_creation.data_injection.data_generator.specs.tests.anomaly_test_spec import (
    AnomalyTestSpec,
)
from data_creation.data_injection.utils import (
    get_values_around_middle,
)

from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SourceFreshnessPeriod,
    SourceFreshnessResult,
)
from data_creation.data_injection.injectors.tests.tests_injector import TestSubTypes


class AutomatedTestsSpec(BaseSpec):
    include_tests: list[str] = ["volume", "freshness"]
    exceptions: dict[tuple[str, str], dict]

    def generate(self, dbt_runner: DbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        all_nodes = models_injector.get_nodes()

        all_tests: list[Union[SourceFreshnessSpec, AnomalyTestSpec]] = []
        for node in all_nodes:
            for test_name in self.include_tests:
                if test_name == "freshness" and not (
                    node["model_id"].startswith("source")
                ):
                    continue

                test_key = (node["model_name"], test_name)
                if test_key in self.exceptions:
                    if test_name == "freshness":
                        all_tests.append(
                            SourceFreshnessSpec(
                                result=SourceFreshnessResult(
                                    model_id=node["model_id"],
                                    **self.exceptions[test_key],
                                )
                            )
                        )
                    else:
                        all_tests.append(
                            AnomalyTestSpec(
                                model_name=node["model_name"],
                                test_name=test_name,
                                is_automated=True,
                                test_sub_type=TestSubTypes.AUTOMATED.value,
                                **self.exceptions[test_key],
                            )
                        )
                else:
                    if test_name == "freshness":
                        all_tests.append(
                            self.generate_source_freshness_test(node["model_id"])
                        )
                    else:
                        all_tests.append(
                            AnomalyTestSpec(
                                model_name=node["model_name"],
                                test_name=test_name,
                                is_automated=True,
                                test_sub_type=TestSubTypes.AUTOMATED.value,
                                metric_values=self.get_random_values(),
                            )
                        )

        for i, test in enumerate(all_tests):
            print(f"* Generating automated test {i + 1} / {len(all_tests)} - {test}")
            test.generate(dbt_runner)

    @staticmethod
    def generate_source_freshness_test(model_id: str):
        utc_now = datetime.utcnow()
        return random.choice(
            [
                SourceFreshnessSpec(
                    result=SourceFreshnessResult(
                        model_id=model_id,
                        max_loaded_at=utc_now - timedelta(hours=3),
                        status="pass",
                        warn_after=SourceFreshnessPeriod(period="hour", count=4),
                        error_after=SourceFreshnessPeriod(period="hour", count=6),
                    )
                ),
                SourceFreshnessSpec(
                    result=SourceFreshnessResult(
                        model_id=model_id,
                        max_loaded_at=utc_now - timedelta(hours=1),
                        status="pass",
                        warn_after=SourceFreshnessPeriod(period="hour", count=3),
                        error_after=SourceFreshnessPeriod(period="hour", count=5),
                    )
                ),
            ]
        )

    @staticmethod
    def get_random_values():
        settings = random.choice([(10000, 1000), (500, 10), (3000, 300)])
        return get_values_around_middle(*settings)
