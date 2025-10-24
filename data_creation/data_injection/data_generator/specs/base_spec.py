from pydantic import BaseModel

from elementary.clients.dbt.subprocess_dbt_runner import (
    SubprocessDbtRunner as DbtRunner,
)


class BaseSpec(BaseModel):
    def generate(self, dbt_runner: DbtRunner):
        raise NotImplementedError()
