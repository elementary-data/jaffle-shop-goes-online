#!/bin/bash

# Multi-day pipeline runner.
# Generates the same seed data as run_pipeline.sh, then replays
# dbt run + dbt test over N days with backdated timestamps so
# Elementary records a multi-day run history.
#
# Days 1..(N-1): training data  (most tests pass, some intentional failures)
# Day N (today): validation data (more failures + ROAS anomaly)
#
# Usage:
#   ./run_pipeline_multiday.sh <TARGET> [DAYS]
#   ./run_pipeline_multiday.sh keypair 8

set -e

TARGET="${1:?Usage: $0 <TARGET> [DAYS]}"
DAYS="${2:-8}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

gen_uuid() {
    if command -v uuidgen &>/dev/null; then
        uuidgen
    else
        python -c "import uuid; print(uuid.uuid4())"
    fi
}
DBT_PROJECT_DIR="$PROJECT_ROOT/jaffle_shop_online"

echo "=== Multi-day pipeline: $DAYS days, target=$TARGET ==="

# --- Data generation (identical to run_pipeline.sh) ---

cd "$PROJECT_ROOT"
echo "Generating training data..."
python -m data_creation.incremental_data_creation.training_data_generator

echo "Generating validation data..."
python -m data_creation.incremental_data_creation.validation_data_generator

echo "Generating ads data..."
python -m data_creation.incremental_data_creation.ads_data_generator

echo "Generating sessions data..."
python -m data_creation.incremental_data_creation.sessions_data_generator

# --- dbt setup (once) ---

cd "$DBT_PROJECT_DIR"
echo "Installing dbt packages..."
dbt deps --target "$TARGET"

echo "Loading seed data..."
dbt seed --target "$TARGET"

# --- Multi-day dbt run/test loop ---

for (( day=1; day<=DAYS; day++ )); do
    days_ago=$(( DAYS - day ))
    if date -v-1d &>/dev/null; then
        # macOS
        run_timestamp=$(date -u -v-${days_ago}d '+%Y-%m-%dT%H:%M:%S')
    else
        # Linux (CI)
        run_timestamp=$(date -u -d "${days_ago} days ago" '+%Y-%m-%dT%H:%M:%S')
    fi

    if [ "$day" -lt "$DAYS" ]; then
        echo ""
        echo "=== Day $day/$DAYS ($run_timestamp) - training ==="
        dbt run --target "$TARGET" \
            --vars "{\"custom_run_started_at\": \"$run_timestamp\", \"orchestrator\": \"dbt_cloud\", \"job_name\": \"jaffle_shop_online_data_load\", \"job_id\": \"$(gen_uuid)\"}" \
            || echo "Day $day: dbt run completed with errors (non-critical)"
        dbt test --target "$TARGET" \
            --vars "{\"custom_run_started_at\": \"$run_timestamp\", \"orchestrator\": \"dbt_cloud\", \"job_name\": \"jaffle_shop_online_data_test\", \"job_id\": \"$(gen_uuid)\"}" \
            || echo "Day $day: dbt test completed with failures (expected)"
    else
        echo ""
        echo "=== Day $day/$DAYS ($run_timestamp) - validation ==="
        dbt run --target "$TARGET" \
            --vars "{\"custom_run_started_at\": \"$run_timestamp\", \"validation\": true, \"orchestrator\": \"dbt_cloud\", \"job_name\": \"jaffle_shop_online_data_load\", \"job_id\": \"$(gen_uuid)\"}" \
            || echo "Final day: dbt run completed with errors (non-critical)"
        dbt test --target "$TARGET" \
            --vars "{\"custom_run_started_at\": \"$run_timestamp\", \"validation\": true, \"orchestrator\": \"dbt_cloud\", \"job_name\": \"jaffle_shop_online_data_test\", \"job_id\": \"$(gen_uuid)\"}" \
            || echo "Final day: dbt test completed with failures (expected)"
    fi
done

echo ""
echo "=== Done: $DAYS days simulated ==="
