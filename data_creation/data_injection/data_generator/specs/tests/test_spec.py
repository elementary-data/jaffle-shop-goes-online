from typing import Optional
from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec
from data_creation.data_injection.injectors.tests.tests_injector import TestSubTypes


class TestSpec(BaseSpec):
    model_name: str
    test_name: str
    test_column_name: Optional[str] = None
    test_sub_type: TestSubTypes = TestSubTypes.GENERIC
    max_execution_time: float
