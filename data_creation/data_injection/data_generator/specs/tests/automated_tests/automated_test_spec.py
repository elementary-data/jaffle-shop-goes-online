from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner as DbtRunner,
)

from data_creation.data_injection.injectors.models.models_injector import ModelsInjector


class AutomatedTestsSpec(BaseSpec):
    exceptions: dict[str, dict]

    def generate(self, dbt_runner: DbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        all_nodes = models_injector.get_nodes()

        all_tests = []
        for node in all_nodes:
            test_key = node["model_name"]
            if test_key in self.exceptions:
                exception = self.exceptions[test_key]
                all_tests.append(self.generate_failed_test(node, exception))

            else:
                all_tests.append(self.generate_passed_test(node))

        for i, test in enumerate(all_tests):
            print(f"* Generating automated test {i + 1} / {len(all_tests)} - {test}")
            test.generate(dbt_runner)

    def generate_failed_test(self, node: dict, exception: dict, *args, **kwargs):
        raise NotImplementedError()

    def generate_passed_test(self, node: dict, *args, **kwargs):
        raise NotImplementedError()
