from pydantic import BaseModel

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner as DbtRunner,
)


class BaseSpec(BaseModel):
    def generate(self, dbt_runner: DbtRunner):
        raise NotImplementedError()
