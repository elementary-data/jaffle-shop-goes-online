from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SourceFreshnessResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec

from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class SourceFreshnessSpec(BaseSpec):
    result: SourceFreshnessResult

    def generate(self, dbt_runner: SubprocessDbtRunner):
        injector = TestRunResultsInjector(dbt_runner)
        injector.inject_source_freshness_result(self.result)
