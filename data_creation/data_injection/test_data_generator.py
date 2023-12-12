import json
import random
from datetime import datetime, date, timedelta, time
from typing import Optional, Any

import numpy
from elementary.clients.dbt.dbt_runner import DbtRunner
from pydantic import BaseModel

from data_injection.anomaly_test_injector import AnomalyTestInjector, DbtTest, AnomalyTestMetric, AnomalyTestResult, \
    SourceFreshnessPeriod, SourceFreshnessResult


def run_query(dbt_runner: DbtRunner, query: str):
    return json.loads(
        dbt_runner.run_operation(
            "elementary.render_run_query",
            macro_args={"prerendered_query": query})[0]
    )


def get_model_id_from_name(dbt_runner: DbtRunner, model_name: str):
    return run_query(
        dbt_runner,
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
        """ % {"model_name": model_name})[0]["model_id"]


def get_nodes(dbt_runner: DbtRunner):
    return run_query(
        dbt_runner,
        """
        select unique_id as model_id, alias as model_name from {{ ref('elementary', 'dbt_models') }} where package_name <> 'elementary'
        union all
        select unique_id as model_id, name as model_name from {{ ref('elementary', 'dbt_sources') }}
        """
    )


def get_values_around_middle(middle, space, num_entries=14):
    return [random.randint(middle - space, middle + space) for i in range(num_entries)]


def get_values_around_middle_anomalous(middle, space, is_spike=False, num_entries=14):
    values = [random.randint(middle - space, middle + space) for i in range(num_entries - 1)]
    if not is_spike:
        values.append(0)
    else:
        values.append(middle + random.randint(space * 5, space * 7))
    return values


def get_values_around_middle_anomalous_weekly_seasonality(middle, space, weekly_middle, is_spike=False, num_entries=14*7 + 3):
    values = [random.randint(weekly_middle - space, weekly_middle + space)
              if i % 7 <= 1
              else random.randint(middle - space, middle + space)
              for i in range(num_entries - 1)]
    if not is_spike:
        values.append(0)
    else:
        if (num_entries - 1) % 7 <= 1:
            values.append(weekly_middle + random.randint(space * 5, space * 7))
        else:
            values.append(middle + random.randint(space * 5, space * 7))
    return values


class TestSpec(BaseModel):
    def generate(self, dbt_runner: DbtRunner):
        raise NotImplementedError()


class SourceFreshnessSpec(TestSpec):
    result: SourceFreshnessResult

    def generate(self, dbt_runner: DbtRunner):
        injector = AnomalyTestInjector(dbt_runner)
        injector.inject_source_freshness_result(self.result)


class AnomalyTestSpec(TestSpec):
    model_name: str
    test_name: str
    is_automated: bool
    metric_values: list[float]
    test_column_name: Optional[str] = None
    timestamp_column: Optional[str] = None
    sensitivity: int = 3
    min_values_bound: int = 0
    bucket_period: str = "day"
    test_sub_type: str = "generic"
    day_of_week_seasonality: bool = False

    @property
    def description(self):
        if self.test_name == "volume":
            return "Monitors the row count of your table over time."
        elif self.test_name == "column_anomalies":
            return ("Column-level anomaly monitors (null_count, null_percent, zero_count, string_length, "
                    "variance, etc.) on the column according to its data type.")
        return ""

    def get_result_description(self, last_metric: AnomalyTestMetric):
        metric_average = (last_metric.min_value + last_metric.max_value) / 2
        if self.test_name == "volume":
            return f"The last volume value is {last_metric.value}, the average for this metric is {metric_average}"
        elif self.test_name == "column_anomalies":
            return (f"In column {self.test_column_name}, the last {self.test_sub_type} value is {last_metric.value}. "
                    f"The average for this metric is {metric_average}.")
        return ""

    def get_metric_timestamps(self):
        if self.is_automated:
            # Not bucketed
            timestamps = []
            cur_timestamp = datetime.utcnow()
            for i in range(len(self.metric_values)):
                cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(3*60, 5*60))
                timestamps.append((None, cur_timestamp))
            return reversed(timestamps)
        else:
            if self.bucket_period == "day":
                start_bucket = datetime.combine(date.today() - timedelta(len(self.metric_values)), time.min)
                return [
                    (start_bucket + timedelta(i), start_bucket + timedelta(i + 1))
                    for i in range(len(self.metric_values))
                ]
            elif self.bucket_period == "hour":
                start_bucket = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
                    hours=len(self.metric_values) + 1
                )
                return [
                    (start_bucket + timedelta(hours=i), start_bucket + timedelta(hours=i + 1))
                    for i in range(len(self.metric_values))
                ]
            else:
                raise ValueError(f"I don't know how to handle bucket size {self.bucket_period}")

    def get_metrics(self):
        metric_timestamps = self.get_metric_timestamps()
        metrics = []

        for i, (value, (start_time, end_time)) in enumerate(zip(self.metric_values, metric_timestamps)):
            if self.day_of_week_seasonality:
                relevant_metrics = list(reversed(self.metric_values[i:0:-7]))
                if i % 7 == 0:
                    relevant_metrics.insert(0, self.metric_values[0])
            else:
                relevant_metrics = self.metric_values[:(i + 1)]
            average = numpy.average(relevant_metrics)
            stddev = numpy.std(relevant_metrics)

            metric = AnomalyTestMetric(
                value=value,
                min_value=max(average - self.sensitivity * stddev, self.min_values_bound),
                max_value=average + self.sensitivity * stddev,
                start_time=start_time.isoformat() if start_time else None,
                end_time=end_time.isoformat(),
            )
            if metric.is_anomalous:
                if self.day_of_week_seasonality:
                    last_metric = metrics[-7]
                else:
                    last_metric = metrics[-1]
                metric.min_value = last_metric.min_value
                metric.max_value = last_metric.max_value

            metrics.append(
                metric
            )
        return metrics

    def generate(self, dbt_runner: DbtRunner):
        model_id = get_model_id_from_name(dbt_runner, self.model_name)

        test_params: dict[str, Any] = {}
        if self.timestamp_column:
            test_params["timestamp_column"] = self.timestamp_column
        if not self.is_automated:
            test_params["time_bucket"] = {"period": self.bucket_period, "count": 1}
        test_params["sensitivity"] = self.sensitivity
        if self.day_of_week_seasonality:
            test_params["seasonality"] = "day_of_week"

        injector = AnomalyTestInjector(dbt_runner)

        test_sub_type = self.test_sub_type if not self.is_automated else "automated"
        test = DbtTest(
            test_id=f"{self.model_name}_{self.test_name}_{test_sub_type}" + (
                f"_{self.test_column_name}" if self.test_column_name else ""
            ),
            test_name=self.test_name,
            test_column_name=self.test_column_name,
            test_sub_type=test_sub_type,
            test_params=test_params,
            description=self.description,
            model_id=model_id,
            model_name=self.model_name
        )

        injector.inject_test(
            test
        )

        metrics = self.get_metrics()
        test_result = AnomalyTestResult(
            test_timestamp=datetime.utcnow(),
            test_status='fail' if metrics[-1].is_anomalous else 'pass',
            test_metrics=metrics,
            result_description=self.get_result_description(metrics[-1])
        )

        injector.inject_test_result(
            test, test_result
        )

        cur_timestamp = datetime.utcnow()
        for i in range(10):
            cur_timestamp = cur_timestamp - timedelta(minutes=random.randint(120, 180))
            if metrics[-1].is_anomalous:
                status = random.choice(['fail'] + ['pass'] * 3)
            else:
                status = "pass"
            prev_test_result = AnomalyTestResult(
                test_timestamp=cur_timestamp,
                test_status=status,
                test_metrics=[],
                result_description=""
            )
            injector.inject_test_result(test, prev_test_result)


class AutomatedTestsSpec(TestSpec):
    include_tests: list[str] = ['volume', 'freshness']
    exceptions: dict[tuple[str, str], dict]

    def generate(self, dbt_runner: DbtRunner):
        all_nodes = get_nodes(dbt_runner)

        all_tests: list[TestSpec] = []
        for node in all_nodes:
            for test_name in self.include_tests:
                if test_name == "freshness" and not(node["model_id"].startswith("source")):
                    continue

                test_key = (node["model_name"], test_name)
                if test_key in self.exceptions:
                    if test_name == "freshness":
                        all_tests.append(
                            SourceFreshnessSpec(
                                result=SourceFreshnessResult(
                                    model_id=node["model_id"],
                                    **self.exceptions[test_key]
                                )
                            )
                        )
                    else:
                        all_tests.append(
                            AnomalyTestSpec(
                                model_name=node["model_name"],
                                test_name=test_name,
                                is_automated=True,
                                **self.exceptions[test_key]
                            )
                        )
                else:
                    if test_name == "freshness":
                        all_tests.append(
                            self.generate_source_freshness_test(node["model_id"])
                        )
                    else:
                        all_tests.append(
                            AnomalyTestSpec(
                                model_name=node["model_name"],
                                test_name=test_name,
                                is_automated=True,
                                metric_values=self.get_random_values(),
                            )
                        )

        for i, test in enumerate(all_tests):
            print(f"* Generating automated test {i + 1} / {len(all_tests)} - {test}")
            test.generate(dbt_runner)

    @staticmethod
    def generate_source_freshness_test(model_id: str):
        utc_now = datetime.utcnow()
        return random.choice([
            SourceFreshnessSpec(
                result=SourceFreshnessResult(
                    model_id=model_id,
                    max_loaded_at=utc_now - timedelta(hours=3),
                    status="pass",
                    warn_after=SourceFreshnessPeriod(period="hour", count=4),
                    error_after=SourceFreshnessPeriod(period="hour", count=6)
                )
            ),
            SourceFreshnessSpec(
                result=SourceFreshnessResult(
                    model_id=model_id,
                    max_loaded_at=utc_now - timedelta(hours=1),
                    status="pass",
                    warn_after=SourceFreshnessPeriod(period="hour", count=3),
                    error_after=SourceFreshnessPeriod(period="hour", count=5)
                )
            )
        ])

    @staticmethod
    def get_random_values():
        settings = random.choice([(10000, 1000), (500, 10), (3000, 300)])
        return get_values_around_middle(*settings)


class TestDataGenerator:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner

    def generate(self, test_specs: list[TestSpec]):
        for i, test_spec in enumerate(test_specs):
            print(f"Generating {i + 1}/{len(test_specs)} - {test_spec}")
            test_spec.generate(self.dbt_runner)

    def delete_generated_tests(self):
        print("Deleting existing generated tests")
        self.dbt_runner.run_operation("data_injection.delete_generated_tests")
