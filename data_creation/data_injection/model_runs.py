from datetime import datetime
from enum import Enum
import json
from typing import List, Optional
from elementary.clients.dbt.dbt_runner import DbtRunner
from pydantic import BaseModel

from data_creation.data_injection.base_injector import BaseInjector


class ModelRunStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"


class ModelMaterialization(Enum):
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"


class ModelRunSchema(BaseModel):
    unique_id: str
    generated_at: datetime
    status: ModelRunStatus = ModelRunStatus.SUCCESS
    materialization: ModelMaterialization = ModelMaterialization.TABLE
    run_duration: Optional[int] = None

    class Config:
        use_enum_values = True

    @property
    def generated_at_str(self) -> str:
        return self.generated_at.strftime("%Y-%m-%d %H:%M:%S")


class ModelRunsInjector(BaseInjector):
    def __init__(
        self, dbt_runner: Optional[DbtRunner] = None, target: Optional[str] = None
    ) -> None:
        super().__init__(dbt_runner, target)

    def inject_model_run(self, model_run: ModelRunSchema):
        self.dbt_runner.run_operation(
            macro_name="data_injection.inject_model_run",
            macro_args=dict(
                model_id=model_run.unique_id,
                generated_at=model_run.generated_at_str,
                run_duration=model_run.run_duration,
                run_status=model_run.status.value,
                materialization=model_run.materialization.value,
            ),
        )

    def inject_model_runs(self, model_runs: List[ModelRunSchema]):
        for model_run in model_runs:
            self.inject_model_run(model_run)

    def get_model_ids(self, select: Optional[str] = None) -> List[str]:
        model_ids_output = self.dbt_runner.run_operation(
            macro_name="data_injection.get_models_unique_ids",
            macro_args=dict(filter=select),
        )
        model_ids = json.loads(model_ids_output[0])
        return model_ids
