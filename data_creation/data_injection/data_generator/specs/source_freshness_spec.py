from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SourceFreshnessResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec

from elementary.clients.dbt.dbt_runner import DbtRunner


class SourceFreshnessSpec(BaseSpec):
    result: SourceFreshnessResult

    def generate(self, dbt_runner: DbtRunner):
        injector = TestRunResultsInjector(dbt_runner)
        injector.inject_source_freshness_result(self.result)
