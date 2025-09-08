import json
from typing import List, Optional
from elementary.clients.dbt.subprocess_dbt_runner import (
    SubprocessDbtRunner as DbtRunner,
)
from data_creation.data_injection.injectors.base_injector import BaseInjector


class ModelsInjector(BaseInjector):
    def __init__(
        self,
        dbt_runner: Optional[DbtRunner] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
    ) -> None:
        super().__init__(dbt_runner, target, profiles_dir)

    def get_model_ids(self, select: Optional[str] = None) -> List[str]:
        model_ids_output = self.dbt_runner.run_operation(
            macro_name="data_injection.get_models_unique_ids",
            macro_args=dict(filter=select),
        )
        model_ids = json.loads(model_ids_output[0])
        return model_ids

    def get_model_id_from_name(self, model_name: str) -> str:
        return self.run_query(
            """
            (
                select unique_id as model_id
                from {{ ref('elementary', 'dbt_models') }}
                where alias = '%(model_name)s'                        
            )
            union all
            (
                select unique_id as model_id
                from {{ ref('elementary', 'dbt_sources') }}
                where name = '%(model_name)s'                        
            )
            """
            % {"model_name": model_name},
        )[0]["model_id"]

    def get_nodes(self):
        return self.run_query(
            """
            select unique_id as model_id, alias as model_name from {{ ref('elementary', 'dbt_models') }} where package_name <> 'elementary'
            union all
            select unique_id as model_id, name as model_name from {{ ref('elementary', 'dbt_sources') }}
            """,
        )
