from elementary.clients.dbt.subprocess_dbt_runner import (
    SubprocessDbtRunner as DbtRunner,
)
from data_creation.data_injection.data_generator.specs.tests.test_spec import TestSpec


class TestDataGenerator:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner

    def generate(self, test_specs: list[TestSpec]):
        for i, test_spec in enumerate(test_specs):
            print(f"Generating {i + 1}/{len(test_specs)} - {test_spec}")
            test_spec.generate(self.dbt_runner)

    def delete_generated_tests(self):
        print("Deleting existing generated tests")
        self.dbt_runner.run_operation("data_injection.delete_generated_tests")
