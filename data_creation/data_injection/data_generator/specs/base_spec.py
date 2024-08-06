from pydantic import BaseModel

from elementary.clients.dbt.api_dbt_runner import APIDbtRunner


class BaseSpec(BaseModel):
    def generate(self, dbt_runner: APIDbtRunner):
        raise NotImplementedError()
