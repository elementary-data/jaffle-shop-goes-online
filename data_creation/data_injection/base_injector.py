import os
from pathlib import Path
from typing import Optional
from elementary.clients.dbt.dbt_runner import DbtRunner

DATA_INJECTION_DB_PROJECT_DIR_NAME = "dbt_project"
DATA_INJECTION_DIR = Path(os.path.dirname(__file__)).absolute()
DBT_PROJECT_DIR = os.path.join(DATA_INJECTION_DIR, DATA_INJECTION_DB_PROJECT_DIR_NAME)
DBT_PROFILES_DIR = os.path.join(os.path.expanduser("~"), ".dbt")


class BaseInjector:
    def __init__(
        self, dbt_runner: Optional[DbtRunner] = None, target: Optional[str] = None
    ) -> None:
        self.dbt_runner = dbt_runner or DbtRunner(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
            target=target,
            raise_on_failure=False,
        )
