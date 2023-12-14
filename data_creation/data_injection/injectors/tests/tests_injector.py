from enum import Enum
import json
from typing import List, Optional
from elementary.clients.dbt.dbt_runner import DbtRunner
from pydantic import BaseModel

from data_creation.data_injection.injectors.base_injector import BaseInjector


class TestTypes(Enum):
    ANOMALY_DETECTION = "anomaly_detection"
    SCHEMA_CHANGE = "schema_change"
    DBT_TEST = "dbt_test"


class TestSchema(BaseModel):
    test_id: str
    test_name: str
    test_column_name: Optional[str]
    test_type: TestTypes
    test_sub_type: str
    test_params: dict
    description: str
    model_id: str
    model_name: str


class TestsInjector(BaseInjector):
    def __init__(
        self,
        dbt_runner: Optional[DbtRunner] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
    ) -> None:
        super().__init__(dbt_runner, target, profiles_dir)

    def inject_test(self, test: TestSchema):
        self.dbt_runner.run_operation(
            macro_name="data_injection.inject_dbt_test",
            macro_args=dict(
                test_id=test.test_id,
                test_name=test.test_name,
                test_column_name=test.test_column_name,
                test_params=test.test_params,
                description=test.description,
                type=test.test_type.value,
            ),
        )

    def inject_tests(self, tests: List[TestSchema]):
        for test in tests:
            self.inject_test(test)

    def delete_test_data(self, test_id: str):
        self.dbt_runner.run_operation(
            "data_injection.delete_test_data", macro_args=dict(test_id=test_id)
        )
