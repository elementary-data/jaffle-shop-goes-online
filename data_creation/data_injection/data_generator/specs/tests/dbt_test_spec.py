from datetime import datetime, timedelta
from enum import Enum
import random
from typing import Any, Optional

from pydantic import validator
from data_creation.data_injection.data_generator.specs.tests.test_spec import TestSpec
from data_creation.data_injection.injectors.models.models_injector import ModelsInjector
from data_creation.data_injection.injectors.tests.test_run_results_injector import (
    DbtTestResult,
    TestRunResultsInjector,
)
from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestTypes,
)

from elementary.clients.dbt.api_dbt_runner import APIDbtRunner


class TestStatuses(Enum):
    PASS = "pass"
    FAIL = "fail"


class DbtTestSpec(TestSpec):
    status: TestStatuses
    result_rows: list[dict]
    test_type: TestTypes = TestTypes.DBT_TEST
    description: Optional[str] = None

    @validator("description", pre=True, always=True)
    def description_validator(cls, description: Optional[str]) -> str:
        default_description = "A test that is used to validate your data"
        return description or default_description

    @property
    def result_description(self) -> str:
        if self.result_rows:
            return f"Got {len(self.result_rows)} result, configured to fail if != 0"
        return ""

    def generate(self, dbt_runner: APIDbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        model_id = models_injector.get_model_id_from_name(self.model_name)

        test_params: dict[str, Any] = {}

        injector = TestRunResultsInjector(dbt_runner)

        test = TestSchema(
            test_id=f"{self.model_name}_{self.test_name}_{self.test_sub_type.value}"
            + (f"_{self.test_column_name}" if self.test_column_name else ""),
            test_name=self.test_name,
            test_column_name=self.test_column_name,
            test_type=self.test_type,
            test_sub_type=self.test_sub_type,
            test_params=test_params,
            description=self.description,
            model_id=model_id,
            model_name=self.model_name,
        )
        injector.inject_test(test)

        execution_time = self.max_execution_time
        test_result = DbtTestResult(
            test_timestamp=datetime.utcnow(),
            test_status=self.status.value,
            test_result_rows=self.result_rows,
            result_description=self.result_description,
            execution_time=execution_time,
        )
        injector.inject_dbt_test_result(test, test_result)

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            status = random.choice(
                [TestStatuses.FAIL.value] + [TestStatuses.PASS.value] * 3
            )
            execution_time = execution_time * ((100 - random.uniform(1, 3)) / 100)
            prev_test_result = DbtTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_result_rows=[],
                result_description="",
                execution_time=execution_time,
            )
            injector.inject_dbt_test_result(test, prev_test_result)
