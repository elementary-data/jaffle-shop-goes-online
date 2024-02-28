from elementary.clients.dbt.dbt_runner import DbtRunner
from data_creation.data_injection.data_generator.specs.exposures.exposure_spec import (
    ExposureSpec,
)
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.config.config import Config


class ExposuresDataGenerator:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner
        self.elementary_cli_dbt_runner = self._init_elementary_cli_dbt_runner()

    def _init_elementary_cli_dbt_runner(self) -> DbtRunner:
        elementary_config = Config(
            config_dir=Config.DEFAULT_CONFIG_DIR,
            profile_target=self.dbt_runner.target,
            dbt_quoting=None,
        )
        data_monitoring = DataMonitoring(config=elementary_config)
        data_monitoring.internal_dbt_runner.run(
            models="elementary_cli.elementary_exposures.elementary_exposures",
        )
        return data_monitoring.internal_dbt_runner

    def generate(self, exposure_specs: list[ExposureSpec]):
        for i, exposure_spec in enumerate(exposure_specs):
            print(
                f"Generating exposure {i + 1}/{len(exposure_specs)} - {exposure_spec}"
            )
            exposure_spec.generate(self.dbt_runner)

    def delete_generated_exposures(self):
        print("Deleting existing generated exposures")
        self.dbt_runner.run_operation("data_injection.delete_generated_exposures")
