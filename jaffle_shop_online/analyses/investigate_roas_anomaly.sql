-- Query to investigate ROAS anomalies
select 
    end_time,
    value as current_roas,
    average as expected_roas,
    anomaly_score,
    anomaly_description
from ELEMENTARY_TESTS.mika_jaffle_shop_online_dbt_test__audit.elementary_column_anomalies_cp_4ff2aeeb3711b8a0ed2adb70f66a0d36
order by end_time;






