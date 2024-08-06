from typing import Optional
from elementary.clients.dbt.dbt_runner import DbtRunner
from data_creation.data_injection.injectors.base_injector import BaseInjector


class ExposuresInjector(BaseInjector):
    def __init__(
        self,
        dbt_runner: Optional[DbtRunner] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
    ) -> None:
        super().__init__(dbt_runner, target, profiles_dir)

    def inject_exposures(self, exposures: list[dict]):
        self.dbt_runner.run_operation(
            macro_name="data_injection.inject_exposures",
            macro_args=dict(exposures=exposures),
        )
