from datetime import datetime, timedelta
import random

from data_creation.data_injection.data_generator.specs.source_freshness_spec import (
    SourceFreshnessSpec,
)
from data_creation.data_injection.data_generator.specs.tests.automated_tests.automated_test_spec import (
    AutomatedTestsSpec,
)

from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SourceFreshnessPeriod,
    SourceFreshnessResult,
)


class AutomatedFreshnessTestsSpec(AutomatedTestsSpec):
    def generate_failed_test(self, node: dict, exception: dict, *args, **kwargs):
        return SourceFreshnessSpec(
            result=SourceFreshnessResult(
                model_id=node["model_id"],
                **exception,
            )
        )

    def generate_passed_test(self, node: dict, *args, **kwargs):
        return self.generate_source_freshness_test(node["model_id"])

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
