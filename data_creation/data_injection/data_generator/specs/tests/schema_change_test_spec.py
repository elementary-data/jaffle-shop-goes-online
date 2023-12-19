from datetime import datetime, timedelta
import random
from typing import Any

from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec
from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    SchemaChangeTestResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestSubTypes,
    TestTypes,
)

from elementary.clients.dbt.dbt_runner import DbtRunner


class SchemaChangeTestSpec(BaseSpec):
    model_name: str
    test_name: str
    results: list[SchemaChangeTestResult]
    # Schema changes tests are currently of type generic due to tech dep we have.
    test_type: TestTypes = TestTypes.GENGERIC
    from_baseline: bool = True

    @property
    def description(self) -> str:
        if self.from_baseline:
            return "Compares the table's schema against a baseline contract of columns defined in the table's configuration."
        return "Monitors schema changes on the table of deleted, added, type changed columns over time. The test will fail if the table's schema changed from the previous execution of the test."

    def generate(self, dbt_runner: DbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        model_id = models_injector.get_model_id_from_name(self.model_name)

        test_params: dict[str, Any] = {}

        injector = TestRunResultsInjector(dbt_runner)

        test = TestSchema(
            test_id=f"{self.model_name}_{self.test_name}",
            test_name=self.test_name,
            test_type=self.test_type,
            test_params=test_params,
            description=self.description,
            model_id=model_id,
            model_name=self.model_name,
        )
        injector.inject_test(test)

        for result in self.results:
            injector.inject_failed_schema_change_test_result(test, result)

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            is_failure = random.choice([True] + [False] * 3)

            if is_failure:
                prev_test_result = SchemaChangeTestResult(
                    test_timestamp=cur_timestamp,
                    column_name=self.results[0].column_name if self.results else "",
                    test_sub_type=TestSubTypes.TYPE_CHANGED,
                )
                injector.inject_failed_schema_change_test_result(test, prev_test_result)
            else:
                injector.inject_passed_schema_change_test_result(test, cur_timestamp)
