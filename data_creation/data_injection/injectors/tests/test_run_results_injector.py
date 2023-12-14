from datetime import datetime
from enum import Enum
from typing import Optional

from elementary.clients.dbt.dbt_runner import DbtRunner
from pydantic import BaseModel

from data_creation.data_injection.injectors.tests.tests_injector import (
    TestSchema,
    TestsInjector,
)


class TestSubTypes(Enum):
    GENGERIC = "generic"

    # dbt test
    DBT_EXPECTATIONS = "expectation"
    SINGULAR = "singular"

    # schema change
    TYPE_CHANGED = "type_changed"

    # anomaly detection
    ROW_COUNT = "row_count"
    ZERO_COUNT = "zero_count"
    ZERO_PRECENT = "zero_precent"
    MISSING_COUNT = "missing_count"
    FRESHNESS = "freshness"
    NULL_COUNT = "null_count"

    # automated test
    AUTOMATED = "automated"


class TestResult(BaseModel):
    pass


class AnomalyTestMetric(BaseModel):
    value: float
    min_value: float
    max_value: float
    start_time: Optional[str]
    end_time: str

    @property
    def is_anomalous(self):
        return (self.value < self.min_value) or (self.value > self.max_value)


class AnomalyTestResult(BaseModel):
    test_timestamp: datetime
    test_status: str
    test_metrics: list[AnomalyTestMetric]
    result_description: str


class SourceFreshnessPeriod(BaseModel):
    period: str
    count: int


class SourceFreshnessResult(BaseModel):
    # model_id, max_loaded_at, max_loaded_at_time_ago_in_s, status, warn_after, error_after
    model_id: str
    max_loaded_at: datetime
    status: str
    warn_after: SourceFreshnessPeriod
    error_after: SourceFreshnessPeriod

    @property
    def max_loaded_at_time_ago_in_s(self) -> float:
        return (datetime.utcnow() - self.max_loaded_at).total_seconds()


class TestRunResultsInjector(TestsInjector):
    def __init__(self, dbt_runner: DbtRunner) -> None:
        super().__init__(dbt_runner)

    def inject_test_result(self, test: TestSchema, test_result: TestResult):
        pass

    def inject_anomaly_test_result(
        self, test: TestSchema, test_result: AnomalyTestResult
    ):
        self.dbt_runner.run_operation(
            macro_name="data_injection.inject_elementary_test_result",
            macro_args=dict(
                test_id=test.test_id,
                test_name=test.test_name,
                test_column_name=test.test_column_name,
                test_type=test.test_type,
                test_sub_type=test.test_sub_type,
                test_params=test.test_params,
                test_timestamp=test_result.test_timestamp.isoformat(),
                test_status=test_result.test_status,
                model_id=test.model_id,
                model_name=test.model_name,
                test_metrics=[metric.dict() for metric in test_result.test_metrics],
                result_description=test_result.result_description,
            ),
        )

    def inject_source_freshness_result(self, test_result: SourceFreshnessResult):
        self.dbt_runner.run_operation(
            macro_name="data_injection.inject_source_freshness_result",
            macro_args=dict(
                model_id=test_result.model_id,
                max_loaded_at=test_result.max_loaded_at.isoformat(),
                max_loaded_at_time_ago_in_s=test_result.max_loaded_at_time_ago_in_s,
                status=test_result.status,
                warn_after=test_result.warn_after.dict(),
                error_after=test_result.error_after.dict(),
            ),
        )

    def delete_test_data(self, test_id: str):
        self.dbt_runner.run_operation(
            "data_injection.delete_test_data", macro_args=dict(test_id=test_id)
        )
