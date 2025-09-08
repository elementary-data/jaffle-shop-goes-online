import json
import os
from pathlib import Path
from typing import Optional
from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner as DbtRunner,
)

DATA_INJECTION_DB_PROJECT_DIR_NAME = "dbt_project"
DATA_INJECTION_DIR = Path(os.path.dirname(__file__)).parent.absolute()
DBT_PROJECT_DIR = os.path.join(DATA_INJECTION_DIR, DATA_INJECTION_DB_PROJECT_DIR_NAME)
DBT_PROFILES_DIR = os.path.join(os.path.expanduser("~"), ".dbt")


class BaseInjector:
    def __init__(
        self,
        dbt_runner: Optional[DbtRunner] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
    ) -> None:
        self.dbt_runner = dbt_runner or DbtRunner(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=profiles_dir or DBT_PROFILES_DIR,
            target=target,
            raise_on_failure=False,
        )

    def run_query(self, query: str):
        return json.loads(
            self.dbt_runner.run_operation(
                "elementary.render_run_query",
                macro_args={"prerendered_query": query},
            )[0]
        )
