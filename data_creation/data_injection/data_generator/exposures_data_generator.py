from typing import Optional
from elementary.clients.dbt.api_dbt_runner import APIDbtRunner
from data_creation.data_injection.data_generator.specs.exposures.exposure_spec import (
    ExposureSpec,
)
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.config.config import Config


class ExposuresDataGenerator:
    def __init__(
        self,
        dbt_runner: APIDbtRunner,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
    ):
        self.dbt_runner = dbt_runner
        self.profiles_dir = profiles_dir
        self.target = target
        self.elementary_cli_dbt_runner = self._init_elementary_cli_dbt_runner()

    def _init_elementary_cli_dbt_runner(self) -> APIDbtRunner:
        elementary_config = Config(
            config_dir=Config.DEFAULT_CONFIG_DIR,
            profiles_dir=self.profiles_dir,
            profile_target=self.target,
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
