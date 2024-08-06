from pydantic import BaseModel

from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


class BaseSpec(BaseModel):
    def generate(self, dbt_runner: SubprocessDbtRunner):
        raise NotImplementedError()
