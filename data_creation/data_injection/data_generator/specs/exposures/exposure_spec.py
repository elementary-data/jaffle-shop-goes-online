from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel
from data_creation.data_injection.data_generator.specs.base_spec import BaseSpec
from data_creation.data_injection.injectors.exposures.exposures_injector import (
    ExposuresInjector,
)

from elementary.clients.dbt.command_line_dbt_runner import (
    CommandLineDbtRunner as DbtRunner,
)

from data_creation.data_injection.injectors.models.models_injector import ModelsInjector


class Column(BaseModel):
    node_name: Optional[str] = None
    node_id: Optional[str] = None
    name: str
    target_name: Optional[str] = None


class ExposureSpec(BaseSpec):
    unique_id: str
    full_path: Optional[str] = None
    name: str
    type: str
    label: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[Column]]
    owner_name: Optional[str] = None
    owner_email: Optional[str] = None
    url: Optional[str] = None
    tags: Optional[List[str]] = None
    meta: Dict[str, Any]
    raw_queries: Optional[List[str]] = None

    def generate(self, dbt_runner: DbtRunner):
        models_injector = ModelsInjector(dbt_runner)
        all_models = models_injector.get_nodes()

        full_path = self.full_path.replace("/", ".") if self.full_path else ""
        path = full_path.split(".")[-1]
        columns = [
            self._parse_depends_on_column(column, all_models) for column in self.columns
        ]

        exposures_injector = ExposuresInjector(dbt_runner)
        exposure = dict(
            name=self.name,
            unique_id=self.unique_id,
            tags=self.tags,
            description=self.description,
            full_path=full_path,
            meta={
                "platform": "str",
                **(self.meta or {}),
                "sub_type": self.type,
                "injected_exposure": True,
            },
            label=self.label,
            url=self.url,
            type=self.type,
            original_path=full_path,
            path=path,
            depends_on_nodes=list(set([column[0] for column in columns])),
            depends_on_columns=columns,
            owner_email=self.owner_email,
            owner_name=self.owner_name,
            raw_queries=self.raw_queries,
        )
        exposures_injector.inject_exposures([exposure])

    @staticmethod
    def _parse_depends_on_column(column: Column, models: List) -> Tuple[str]:
        if column.node_id:
            return (column.node_id, column.name, column.target_name)
        elif column.node_name:
            node = [
                model
                for model in models
                if model["model_name"].lower() == column.node_name.lower()
            ]
            if node:
                return (node[0]["model_id"], column.name, column.target_name)
        raise Exception("Exposure - could not match depends on column")
