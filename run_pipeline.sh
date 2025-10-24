#!/bin/bash

# Jaffle Shop Goes Online - Complete Pipeline Runner
# This script generates all data, runs dbt models, and executes tests

set -e  # Exit on any error

echo "🚀 Starting Jaffle Shop Goes Online Pipeline..."
echo "==============================================="

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROJECT_DIR="$PROJECT_ROOT/jaffle_shop_online"

echo "📂 Project root: $PROJECT_ROOT"
echo "📂 dbt project: $DBT_PROJECT_DIR"

# Step 1: Generate Training Data
echo ""
echo "📊 Step 1: Generating training data..."
cd "$PROJECT_ROOT"
python -m data_creation.incremental_data_creation.training_data_generator

# Step 2: Generate Validation Data
echo ""
echo "📊 Step 2: Generating validation data..."
python -m data_creation.incremental_data_creation.validation_data_generator

# Step 3: Generate Ads Data
echo ""
echo "📊 Step 3: Generating ads data..."
python -m data_creation.incremental_data_creation.ads_data_generator

# Step 4: Generate Sessions Data
echo ""
echo "📊 Step 4: Generating sessions data..."
python -m data_creation.incremental_data_creation.sessions_data_generator

# Step 5: Load Seeds
echo ""
echo "🌱 Step 5: Loading seed data into database..."
cd "$DBT_PROJECT_DIR"
dbt seed

# Step 6: Run Models
echo ""
echo "🏗️  Step 6: Building dbt models..."
dbt run

# Step 7: Run Tests
echo ""
echo "🧪 Step 7: Running data quality tests..."
dbt test

echo ""
echo "✅ Pipeline completed successfully!"
echo ""
echo "📋 Summary:"
echo "   - Training data generated"
echo "   - Validation data generated" 
echo "   - Marketing ads data generated"
echo "   - Session data generated"
echo "   - Seeds loaded to database"
echo "   - All dbt models built"
echo "   - Data quality tests executed"
echo ""
echo "🔍 Check the test results above for any anomalies detected."
echo "   ROAS anomaly tests should be FAILING (indicating anomalies detected)." 