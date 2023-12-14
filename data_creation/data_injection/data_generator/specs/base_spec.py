from pydantic import BaseModel

from elementary.clients.dbt.dbt_runner import DbtRunner


class BaseSpec(BaseModel):
    def generate(self, dbt_runner: DbtRunner):
        raise NotImplementedError()
