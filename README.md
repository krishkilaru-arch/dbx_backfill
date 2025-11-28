# Databricks Backfill Demo Project

A complete implementation demonstrating **Databricks Backfill Jobs** for processing historical data across multiple dates efficiently and reliably.

> 📖 **Reference**: [Databricks Backfill Jobs Documentation](https://docs.databricks.com/aws/en/jobs/backfill-jobs)

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Detailed Component Guide](#detailed-component-guide)
5. [Setup Instructions](#setup-instructions)
6. [Usage Guide](#usage-guide)
7. [Key Concepts](#key-concepts)
8. [Best Practices](#best-practices)

---

## Overview

### What is This Project?

This project demonstrates how to implement **backfill operations** in Databricks - the process of reprocessing historical data for specific date ranges. It's designed to be:

- **Production-Ready**: Robust error handling, logging, and monitoring
- **Scalable**: Parallel processing of multiple dates using Databricks Jobs
- **Idempotent**: Safe to rerun without creating duplicate data
- **Reusable**: Generic design applicable to any backfill scenario

### What Problem Does It Solve?

When you need to:
- **Reprocess historical data** after fixing bugs or updating logic
- **Fill gaps** in data processing due to failures
- **Process multiple dates** efficiently without manual intervention
- **Track backfill progress** and monitor success/failure rates

### Real-World Use Cases

1. **Data Quality Fixes**: Reprocess last 30 days after fixing data transformation logic
2. **Gap Filling**: Backfill missing dates due to pipeline failures
3. **Historical Migrations**: Process years of historical data when onboarding new datasets
4. **Compliance**: Reprocess data with updated business rules or regulations

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    User / Scheduler                          │
└────────────────────┬────────────────────────────────────────┘
                     │ Trigger with date range
                     ▼
┌─────────────────────────────────────────────────────────────┐
│           04_job_manager.ipynb (Setup)                       │
│  • Creates/Updates Databricks Jobs                           │
│  • Manages job configurations                                │
└────────────────────┬────────────────────────────────────────┘
                     │ Jobs created
                     ▼
┌─────────────────────────────────────────────────────────────┐
│      03_backfill_orchestrator.ipynb (Orchestration)          │
│  • Validates business days (calendar check)                  │
│  • Triggers process_data job for valid dates                 │
│  • Monitors job execution                                    │
│  • Logs status to backfill_log table                         │
└────────────────────┬────────────────────────────────────────┘
                     │ For each date
                     ▼
┌─────────────────────────────────────────────────────────────┐
│         02_process_data.ipynb (Data Processing)              │
│  • Accepts position_date parameter                           │
│  • Reads from source_data table                              │
│  • Writes to destination_data table (partition overwrite)    │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Calendar Table → Orchestrator → Process Data Job → Destination Table
     ↓                ↓
Business Day       Backfill Log
Validation         (Tracking)
```

---

## Project Structure

```
C:\Users\convwk\dbx_backfilling\backfill_project\la\
│
├── config.py                          # Centralized configuration
├── 01_setup.ipynb                     # One-time setup notebook
├── 02_process_data.ipynb              # Single-date data processing
├── 03_backfill_orchestrator.ipynb     # Orchestration & monitoring
├── 04_job_manager.ipynb               # Databricks jobs management
├── 05_retry_failed_jobs.ipynb         # Automated retry for failed jobs
└── README.md                          # This file
```

---

## Detailed Component Guide

### 1. `config.py` - Configuration Management

**Purpose**: Centralized configuration to eliminate hardcoded values and enable portability.

**Key Components**:

```python
# Database Configuration
CATALOG = "demos"              # Unity Catalog name
SCHEMA = "backfill_demo"       # Schema name

# Table Names (Fully Qualified)
SOURCE_TABLE = "demos.backfill_demo.source_data"
DEST_TABLE = "demos.backfill_demo.destination_data"
CALENDAR_TABLE = "demos.backfill_demo.calendar"
BACKFILL_LOG_TABLE = "demos.backfill_demo.backfill_log"
```

**Why It Exists**:
- ✅ **Single Source of Truth**: Change catalog/schema once, affects all notebooks
- ✅ **Environment Flexibility**: Easy to switch between dev/qa/prod
- ✅ **Maintainability**: No scattered hardcoded values across notebooks
- ✅ **Blog-Ready**: Generic names suitable for public sharing

**How It's Used**:
All notebooks import this using dynamic path detection:
```python
sys.path.append(base_path)
from config import CATALOG, SOURCE_TABLE, DEST_TABLE, etc.
```

---

### 2. `01_setup.ipynb` - Environment Setup

**Purpose**: One-time setup to create all required tables and populate test data.

**What It Creates**:

#### Tables Created:

1. **Calendar Table** (`demos.backfill_demo.calendar`)
   - **Purpose**: Business day calendar for date validation
   - **Schema**:
     - `calendar_date` (DATE): The date
     - `calendar_year` (INT): Year
     - `quarter_of_the_year` (INT): Quarter (1-4)
     - `month_of_the_year` (INT): Month (1-12)
     - `week_of_the_month` (INT): Week of month
     - `us_business_or_holiday_flag` (STRING): 'B' = Business day, 'H' = Holiday/Weekend
     - `global_business_or_holiday_flag` (STRING): Same as US flag (for demo)
   - **Data**: 2020-2026 (7 years)
   - **Holiday Logic**: Uses `pandas.tseries.holiday.USFederalHolidayCalendar`
     - Marks weekends (Saturday/Sunday) as 'H'
     - Marks US federal holidays as 'H' (New Year's, MLK Day, Presidents' Day, Memorial Day, July 4th, Labor Day, Columbus Day, Veterans Day, Thanksgiving, Christmas)
     - All other Monday-Friday dates as 'B'

2. **Source Data Table** (`demos.backfill_demo.source_data`)
   - **Purpose**: Historical data to be processed
   - **Schema**:
     - `position_date` (DATE): Business date
     - `id` (BIGINT): Record identifier (0-9)
     - `value` (DOUBLE): Random value (0-100)
     - `category` (STRING): Category (A, B, or C)
   - **Data**: 2024-01-01 to 2025-11-20 (business days only)
   - **Volume**: 10 records per date (~4,700 total records)

3. **Destination Data Table** (`demos.backfill_demo.destination_data`)
   - **Purpose**: Target table for processed data
   - **Schema**: Identical to source_data
   - **Key Feature**: **Partitioned by position_date** for efficient writes
   - **Initial State**: Empty (populated by backfill)

4. **Backfill Log Table** (`demos.backfill_demo.backfill_log`)
   - **Purpose**: Track all backfill operations
   - **Schema**:
     - `run_id` (STRING): Unique run identifier
     - `position_date` (DATE): Date being processed
     - `job_name` (STRING): Name of processing job
     - `job_id` (STRING): Databricks job ID
     - `status` (STRING): STARTED/SUCCESS/FAILED/SKIPPED
     - `start_time` (TIMESTAMP): When processing started
     - `end_time` (TIMESTAMP): When processing completed
     - `backfill_job_id` (STRING): Triggered job run ID
     - `error_message` (STRING): Error details if failed
     - `creator_user_name` (STRING): Who triggered the job
     - `run_page_url` (STRING): URL to job run in Databricks UI

**Key Features**:

- **No Python Loops**: Uses Spark SQL `sequence()` and cross joins for performance
- **Real US Holidays**: Leverages pandas built-in holiday calendar (no external packages)
- **Reproducible**: Uses random seeds for consistent test data
- **Optimized**: Vectorized operations, no row-by-row processing

**When to Run**:
- ✅ First time setting up the project
- ✅ After dropping tables for testing
- ✅ When changing catalog/schema in config.py

**Important Notes**:
- Creates **managed Delta tables** (storage handled by Unity Catalog)
- Can be safely rerun (uses `CREATE TABLE IF NOT EXISTS` and `mode("overwrite")`)
- Source table generates data for weekdays only (filters on `dayofweek`)

---

### 3. `02_process_data.ipynb` - Data Processing Worker

**Purpose**: Single-date ETL job that processes data for one position date.

**Design Philosophy**: 
- **Simple**: Pure ETL logic (Read → Write)
- **Focused**: Does ONE thing well
- **Reusable**: Called repeatedly by orchestrator for different dates

**How It Works**:

```python
# 1. Accept parameter via widget
position_date = "2024-06-15"  # From widget

# 2. Read source data for that date
source_df = spark.table(SOURCE_TABLE).filter(
    F.col("position_date") == F.to_date(F.lit(position_date))
)

# 3. Write to destination with partition overwrite
source_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"position_date = '{position_date}'") \
    .saveAsTable(DEST_TABLE)
```

**Key Features**:

1. **Widget-Based Parameters**:
   ```python
   dbutils.widgets.text("position_date", "", "Position Date (YYYY-MM-DD)")
   ```
   - Allows job to accept parameters from orchestrator
   - Format: YYYY-MM-DD (e.g., "2024-06-15")

2. **Idempotent Writes**:
   ```python
   .option("replaceWhere", f"position_date = '{position_date}'")
   ```
   - Overwrites ONLY the specific date partition
   - Safe to rerun without duplicating data
   - Other dates remain untouched

3. **Error Handling**:
   - Checks for empty results
   - Logs record counts
   - Re-raises exceptions to fail job properly

4. **Dynamic Config Loading**:
   - Uses same path detection as other notebooks
   - Imports table names from config.py

**When It Runs**:
- ✅ Triggered by orchestrator for each date
- ✅ Run manually for testing specific dates
- ✅ Called by Databricks Workflow Jobs

**Execution Context**:
- Runs as a **Databricks Job Task** (not interactive)
- Receives `position_date` parameter from caller
- Returns success/failure via job status

**Performance Considerations**:
- Filters data at source (predicate pushdown)
- Uses partition overwrite (faster than MERGE)
- Processes single date per run (enables parallelism at orchestrator level)

---

### 4. `03_backfill_orchestrator.ipynb` - Orchestration & Monitoring

**Purpose**: Coordinates backfill operations across multiple dates with business day validation and comprehensive logging.

**Key Responsibilities**:

1. **Business Day Validation**
2. **Job Triggering**
3. **Execution Monitoring**
4. **Status Logging**

**Detailed Flow**:

```python
# 1. Get Parameters
position_date = "2024-06-15"
job_name = "02_process_data"

# 2. Validate Business Day
query = f"""
    SELECT COUNT(1) AS is_business_day
    FROM {CALENDAR_TABLE}
    WHERE is_business_day = 1
      AND date = '{position_date}'
"""
is_business_day = spark.sql(query).first()["is_business_day"] > 0

if not is_business_day:
    logger.skip("Not a business day")
    exit()

# 3. Trigger Job
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
response = w.jobs.run_now(
    job_id=job_id,
    notebook_params={"position_date": position_date}
)

# 4. Monitor Execution
while True:
    run_status = w.jobs.get_run(run_id=response.run_id)
    if run_status.state.life_cycle_state == "TERMINATED":
        if run_status.state.result_state == "SUCCESS":
            logger.success(...)
        else:
            logger.fail(...)
        break
    time.sleep(10)  # Poll every 10 seconds
```

**BackfillLogger Class**:

A custom logging utility for tracking backfill operations:

```python
class BackfillLogger:
    def __init__(self, run_id, position_date, job_name, job_id):
        # Initialize logger for this run
        
    def start(self):
        # Log "STARTED" status
        
    def success(self, backfill_job_id=None, creator=None, url=None):
        # Log "SUCCESS" status with job details
        
    def fail(self, error_message):
        # Log "FAILED" status with error
        
    def skip(self, reason):
        # Log "SKIPPED" status (e.g., non-business day)
```

**Usage Example**:

```python
logger = BackfillLogger(
    run_id="downstream_job_123",
    position_date="2024-06-15",
    job_name="02_process_data",
    job_id="987654321"
).start()

# ... processing ...

logger.success(
    backfill_job_id="run_456",
    creator="user@domain.com",
    url="https://databricks.com/jobs/runs/456"
)
```

**Key Features**:

1. **Calendar Integration**:
   - Queries calendar table for business day validation
   - Skips non-business days (weekends/holidays)
   - Logs skip reason

2. **Job Status Tracking**:
   - Polls job status every 10 seconds
   - Detects terminal states (SUCCESS/FAILED/TERMINATED)
   - Captures error messages on failure

3. **Comprehensive Logging**:
   - Logs to `backfill_log` table (queryable for reporting)
   - Captures job metadata (run ID, creator, URL)
   - Provides audit trail for all backfill operations

4. **Error Resilience**:
   - Handles job trigger failures
   - Catches monitoring errors
   - Logs failures for investigation

**When It Runs**:
- ✅ For single-date backfill operations
- ✅ Can be called in a loop for date ranges (see Usage Guide)
- ✅ Invoked by higher-level orchestration workflows

**Important Notes**:
- Uses `databricks-sdk` for programmatic job control
- Requires workspace client initialization
- Designed to be called repeatedly for multiple dates
- Each call processes ONE date (parallel execution at job level)

---

### 5. `04_job_manager.ipynb` - Job Configuration & Management

**Purpose**: Manages Databricks Job definitions programmatically - create, update, and cleanup jobs.

---

### 6. `05_retry_failed_jobs.ipynb` - Automated Retry System

**Purpose**: Automatically identifies and retries failed backfill jobs from the log table.

**Key Functions**:

#### 1. `list_jobs_by_name(job_name)`
Lists all active jobs with a given name (handles stale metadata).

```python
def list_jobs_by_name(job_name):
    # Query system.lakeflow.jobs metadata table
    # Verify each job exists via API (handles deleted jobs)
    # Return only verified active jobs
```

**Why It's Complex**:
- `system.lakeflow.jobs` metadata can be stale
- Jobs may show as active but not exist in API
- Requires dual verification: metadata + API check

#### 2. `cleanup_duplicate_jobs(job_name, keep_latest=True)`
Removes duplicate job definitions, optionally keeping the latest.

```python
cleanup_duplicate_jobs("02_process_data", keep_latest=True)
# Keeps newest version, deletes old ones
```

**Use Cases**:
- After repeatedly creating jobs during development
- Cleaning up test jobs
- Maintaining single active job version

#### 3. `create_or_update_job(job_name, job_config, force_create=False)`
Smart function that updates existing jobs or creates new ones.

```python
job_config = Job.from_dict({
    "name": "02_process_data",
    "max_concurrent_runs": 20,
    "tasks": [{...}],
    ...
})

job_id = create_or_update_job("02_process_data", job_config)
```

**Logic Flow**:
1. Check if job exists
2. If exists:
   - Try to update via `w.jobs.reset()`
   - If update fails (job doesn't exist), create new
3. If doesn't exist:
   - Create new job

**Why This Matters**:
- Avoids creating duplicates on every run
- Handles API errors gracefully
- Enables version updates without manual deletion

**Job Configurations**:

#### Process Data Job (`02_process_data`)

```python
{
    "name": "02_process_data",
    "max_concurrent_runs": 20,  # Up to 20 dates in parallel
    "tasks": [{
        "task_key": "process_data",
        "notebook_task": {
            "notebook_path": "/path/to/02_process_data",
            "base_parameters": {"position_date": ""},
            "source": "WORKSPACE"
        },
        "max_retries": 3,
        "min_retry_interval_millis": 60000,  # 1 minute
        "retry_on_timeout": True,
        "timeout_seconds": 1800  # 30 minutes
    }],
    "tags": {
        "app_name": "backfill_demo",
        "environment": "dev"
    },
    "queue": {"enabled": True},
}
```

**Configuration Explained**:
- `max_concurrent_runs: 20`: Process up to 20 dates simultaneously
- `max_retries: 3`: Retry failed runs 3 times
- `retry_on_timeout: True`: Retry if job times out
- `timeout_seconds: 1800`: 30-minute max per date
- `queue.enabled: True`: Queue runs if all slots busy

#### Orchestrator Job (`03_backfill_orchestrator`)

```python
{
    "name": "03_backfill_orchestrator",
    "max_concurrent_runs": 10,  # Up to 10 parallel orchestrations
    "tasks": [{
        "task_key": "orchestrate_backfill",
        "notebook_task": {
            "notebook_path": "/path/to/03_backfill_orchestrator",
            "base_parameters": {
                "start_date": "",
                "end_date": "",
                "job_name": "02_process_data"
            },
            "source": "WORKSPACE"
        },
        "max_retries": 2,
        "min_retry_interval_millis": 120000,  # 2 minutes
        "retry_on_timeout": False,
        "timeout_seconds": 3600  # 1 hour
    }],
    ...
}
```

**When to Run**:
- ✅ Initial setup (creates jobs)
- ✅ After modifying job configurations
- ✅ When cleaning up duplicate/test jobs
- ✅ To verify current job state

**Key Features**:

1. **Dynamic Path Detection**:
   ```python
   notebook_path = dbutils.notebook.entry_point.getDbutils()...
   base_path = f"/Workspace{notebook_path}".rsplit('/', 1)[0]
   ```
   - No hardcoded paths
   - Works in any workspace location

2. **Robust Error Handling**:
   - Handles stale job metadata
   - Gracefully falls back to create if update fails
   - Verifies job existence via API

3. **Workspace ID Detection**:
   ```python
   workspace_id = dbutils.entry_point.getDbutils()...workspaceId().get()
   ```
   - Queries jobs for current workspace only
   - Prevents cross-workspace conflicts

---

### 6. `05_retry_failed_jobs.ipynb` - Automated Retry System

**Purpose**: Automatically identifies and retries failed backfill jobs from the log table.

**Key Responsibilities**:

1. **Query Failed Jobs**
2. **Build Retry Metadata**
3. **Trigger Orchestrator with Retry Tracking**
4. **Monitor Retry Results**

**Detailed Flow**:

```python
# 1. Query Failed Jobs
failed_jobs_query = f"""
    SELECT position_date, job_name, job_id, run_id, error_message
    FROM {BACKFILL_LOG_TABLE}
    WHERE status = 'FAILED'
      AND retry_metadata IS NULL  -- Only original failures
      AND position_date >= CURRENT_DATE - INTERVAL '{retry_days_back}' DAYS
    LIMIT {max_retries}
"""

# 2. Build Retry Metadata
retry_metadata = {
    "is_retry": True,
    "original_run_id": original_run_id,
    "retry_count": retry_attempt_number,
    "retry_triggered_by": "05_retry_notebook",
    "retry_triggered_at": timestamp
}

# 3. Trigger Orchestrator
w.jobs.run_now(
    job_id=orchestrator_job_id,
    notebook_params={
        "position_date": position_date,
        "job_name": job_name,
        "retry_metadata": json.dumps(retry_metadata)
    }
)
```

**Retry Metadata Schema**:

The `retry_metadata` column stores JSON with the following structure:

```json
{
  "is_retry": true,
  "original_run_id": "abc123",
  "retry_count": 2,
  "retry_triggered_by": "05_retry_notebook",
  "retry_triggered_at": "2025-11-27T10:30:00"
}
```

**Key Features**:

1. **Backward Compatible**:
   - Checks if `retry_metadata` column exists
   - Works with old and new table schemas
   - Adds column automatically if missing

2. **Smart Filtering**:
   ```python
   # Only retry original failures, not failed retries
   WHERE retry_metadata IS NULL
   ```
   - Prevents retry loops
   - Tracks retry attempts per position_date

3. **Flexible Scheduling**:
   - **Daily**: Set `retry_days_back=1` (retry yesterday's failures)
   - **Weekly**: Set `retry_days_back=7` (retry past week)
   - **Custom Range**: Use `start_date` and `end_date` parameters
   - **Batch Limit**: `max_retries` prevents overwhelming the system

4. **Comprehensive Tracking**:
   - Logs each retry with metadata in backfill_log
   - Links retry runs to original failed runs
   - Enables retry analytics and success rate reporting

**Retry Analytics Queries**:

```sql
-- Find position dates that succeeded after retries
SELECT 
    position_date,
    COUNT(*) as total_attempts,
    MAX(CAST(retry_metadata:retry_count AS INT)) as max_retry_count,
    MIN(CASE WHEN status = 'FAILED' THEN start_time END) as first_failure,
    MAX(CASE WHEN status = 'SUCCESS' THEN start_time END) as final_success
FROM demos.backfill_demo.backfill_log
WHERE position_date IN (
    SELECT position_date FROM demos.backfill_demo.backfill_log 
    WHERE retry_metadata IS NOT NULL
)
GROUP BY position_date
HAVING MAX(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) = 1;

-- Retry success rate by attempt number
SELECT 
    retry_metadata:retry_count as retry_attempt,
    COUNT(*) as total_retries,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM demos.backfill_demo.backfill_log
WHERE retry_metadata IS NOT NULL
GROUP BY retry_metadata:retry_count
ORDER BY retry_attempt;
```

**When to Run**:
- ✅ Scheduled daily (e.g., 9 AM) to automatically retry previous day's failures
- ✅ Scheduled weekly for broader cleanup
- ✅ Manually after investigating failed jobs
- ✅ After fixing data issues that caused failures

**Scheduling Options**:

1. **Create Databricks Job for Daily Retries**:
   ```python
   # In 04_job_manager.ipynb, add:
   retry_job_config = Job.from_dict({
       "name": "05_retry_failed_jobs",
       "schedule": {
           "quartz_cron_expression": "0 0 9 * * ?",  # 9 AM daily
           "timezone_id": "America/New_York"
       },
       "tasks": [{
           "task_key": "retry_failures",
           "notebook_task": {
               "notebook_path": f"{base_path}/05_retry_failed_jobs",
               "base_parameters": {
                   "retry_days_back": "1",
                   "max_retries": "10"
               }
           }
       }]
   })
   ```

2. **Manual Execution**:
   - Run notebook directly with custom parameters
   - Useful for investigating specific failure patterns
   - Adjust `max_retries` based on failure volume

**Important Notes**:
- Requires `03_backfill_orchestrator` job to exist (created by `04_job_manager.ipynb`)
- Each retry creates a new log entry with `retry_metadata` populated
- Original failed runs remain in the log (audit trail)
- Retry metadata enables linking retries to original failures

---

## Setup Instructions

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Permissions to create catalogs, schemas, tables, and jobs
- Access to Databricks SQL warehouse or compute cluster

### Step-by-Step Setup

#### 1. Create Project Structure

```bash
# Create project directory (if using Databricks Repos)
# Or upload files directly to Workspace
```

#### 2. Configure Catalog & Schema

Edit `config.py`:

```python
CATALOG = "your_catalog_name"    # Change to your catalog
SCHEMA = "backfill_demo"         # Or your preferred schema name
```

#### 3. Run Setup Notebook

Execute `01_setup.ipynb`:

```python
# Run all cells in order:
1. Load Configuration
2. Create Catalog and Schema
3. Create Tables (DDLs)
4. Populate Source Data
5. Populate Calendar Table
6. Verify Setup
```

**Expected Results**:
- 4 tables created (calendar, source_data, destination_data, backfill_log)
- ~2,556 calendar days (2020-2026)
- ~4,700 source records (2024-2025 business days)
- Destination and log tables empty

#### 4. Create Databricks Jobs

Execute `04_job_manager.ipynb`:

```python
# Run cells in order:
1. Setup (imports, workspace client)
2. Helper functions
3. Cleanup duplicates (optional, safe to run)
4. Create/Update process_data job
5. Create/Update orchestrator job
6. Verify jobs
```

**Expected Results**:
- 2 Databricks Jobs created:
  - `02_process_data` (job_id returned)
  - `03_backfill_orchestrator` (job_id returned)
- Jobs visible in Databricks Workflows UI

#### 5. Test Single Date Processing

Run `02_process_data.ipynb` interactively:

```python
# Set widget value to a business date:
position_date = "2024-06-17"  # Must be a Monday-Friday

# Execute all cells
# Should process ~10 records
```

**Verify**:
```sql
SELECT * FROM demos.backfill_demo.destination_data 
WHERE position_date = '2024-06-17';
```

#### 6. Test Orchestrator

Run `03_backfill_orchestrator.ipynb`:

```python
# Set widget values:
position_date = "2024-06-18"
job_name = "02_process_data"

# Execute all cells
# Should trigger job and monitor to completion
```

**Verify**:
```sql
SELECT * FROM demos.backfill_demo.backfill_log 
ORDER BY start_time DESC LIMIT 10;
```

---

## Usage Guide

### Use Case 1: Backfill Single Date

**Scenario**: Process one specific date.

**Method 1 - Interactive**:
```python
# In 02_process_data.ipynb
position_date = "2024-07-01"
# Run notebook
```

**Method 2 - Via Orchestrator**:
```python
# In 03_backfill_orchestrator.ipynb
position_date = "2024-07-01"
job_name = "02_process_data"
# Run notebook
```

### Use Case 2: Backfill Date Range

**Scenario**: Process last 30 days.

**Create a new orchestrator notebook** or add to existing:

```python
from datetime import datetime, timedelta

# Define date range
end_date = datetime.today()
start_date = end_date - timedelta(days=30)

# Generate date list
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime('%Y-%m-%d')
    
    # Call orchestrator for each date
    # (In production, trigger jobs via Databricks SDK)
    dbutils.notebook.run(
        "./03_backfill_orchestrator",
        timeout_seconds=3600,
        arguments={
            "position_date": date_str,
            "job_name": "02_process_data"
        }
    )
    
    current_date += timedelta(days=1)
```

**Better Approach - Parallel Execution**:

```python
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

w = WorkspaceClient()
job_id = "<orchestrator_job_id>"

# Generate dates
dates = []
current_date = start_date
while current_date <= end_date:
    dates.append(current_date.strftime('%Y-%m-%d'))
    current_date += timedelta(days=1)

# Trigger job for each date (runs in parallel up to max_concurrent_runs)
run_ids = []
for date_str in dates:
    response = w.jobs.run_now(
        job_id=job_id,
        notebook_params={
            "position_date": date_str,
            "job_name": "02_process_data"
        }
    )
    run_ids.append(response.run_id)
    print(f"Triggered {date_str}: run_id={response.run_id}")

print(f"\nTriggered {len(run_ids)} backfill jobs")
```

### Use Case 3: Monitor Backfill Progress

**Query Backfill Log**:

```sql
-- Overall Status Summary
SELECT 
    status,
    COUNT(*) as count,
    MIN(position_date) as earliest_date,
    MAX(position_date) as latest_date
FROM demos.backfill_demo.backfill_log
GROUP BY status
ORDER BY status;

-- Recent Runs
SELECT 
    position_date,
    status,
    start_time,
    end_time,
    TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
    error_message
FROM demos.backfill_demo.backfill_log
ORDER BY start_time DESC
LIMIT 20;

-- Failed Runs (for retry)
SELECT position_date, error_message
FROM demos.backfill_demo.backfill_log
WHERE status = 'FAILED'
ORDER BY position_date;

-- Business Day Coverage
SELECT 
    c.calendar_date,
    CASE WHEN bl.position_date IS NOT NULL THEN 'Processed' ELSE 'Missing' END as status
FROM demos.backfill_demo.calendar c
LEFT JOIN (
    SELECT DISTINCT position_date 
    FROM demos.backfill_demo.backfill_log 
    WHERE status = 'SUCCESS'
) bl ON c.calendar_date = bl.position_date
WHERE c.us_business_or_holiday_flag = 'B'
  AND c.calendar_date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY c.calendar_date;
```

### Use Case 4: Retry Failed Dates (Automated)

**Recommended Approach**: Use `05_retry_failed_jobs.ipynb`

This notebook provides automated retry with tracking:

```python
# In 05_retry_failed_jobs.ipynb, set parameters:
retry_days_back = 1  # Retry yesterday's failures
max_retries = 10     # Limit to 10 retries per run

# Run the notebook - it will:
# 1. Query failed jobs from the log table
# 2. Build retry metadata for tracking
# 3. Trigger orchestrator for each failed date
# 4. Log retry attempts with metadata
```

**Manual Approach** (if needed):

```python
# Query failed dates
failed_dates = spark.sql("""
    SELECT DISTINCT position_date
    FROM demos.backfill_demo.backfill_log
    WHERE status = 'FAILED'
      AND retry_metadata IS NULL  -- Only original failures
    ORDER BY position_date
""").collect()

# Retry each failed date
for row in failed_dates:
    date_str = row['position_date'].strftime('%Y-%m-%d')
    print(f"Retrying {date_str}...")
    
    w.jobs.run_now(
        job_id=orchestrator_job_id,
        notebook_params={
            "position_date": date_str,
            "job_name": "02_process_data"
        }
    )
```

**Benefits of Using 05_retry_failed_jobs.ipynb**:
- ✅ Automatic retry metadata tracking
- ✅ Links retry runs to original failures
- ✅ Prevents retry loops
- ✅ Enables retry analytics
- ✅ Can be scheduled for automatic execution

---

## Key Concepts

### 1. Idempotency

**Definition**: An operation that can be run multiple times with the same result.

**Implementation**:
```python
.option("replaceWhere", f"position_date = '{position_date}'")
```

**Why It Matters**:
- ✅ Safe to rerun failed jobs
- ✅ No duplicate data
- ✅ Enables retry logic

### 2. Partition Overwrite

**What It Does**: Overwrites only specific partitions, leaving others untouched.

**Example**:
```python
# Process June 15
.option("replaceWhere", "position_date = '2024-06-15'")
# Only June 15 data is replaced, all other dates remain
```

**Benefits**:
- Faster than full table overwrite
- Safer than DELETE + INSERT
- Works with Delta Lake time travel

### 3. Business Day Validation

**Why It Exists**: Many business processes only run on business days.

**Implementation**:
- Calendar table with US federal holidays
- Pre-validation before triggering jobs
- Logs skipped dates for audit

**Alternatives**:
- Could validate in process_data notebook (wastes job runs)
- Could skip validation entirely (processes unnecessary dates)

### 4. Job Orchestration Patterns

**Pattern 1: Sequential Processing**
```python
for date in dates:
    process(date)
    wait_for_completion()
```
- ❌ Slow (one at a time)
- ✅ Simple to implement

**Pattern 2: Parallel Processing** (This Project)
```python
for date in dates:
    trigger_job(date)  # Don't wait

# Jobs run in parallel up to max_concurrent_runs
```
- ✅ Fast (multiple dates simultaneously)
- ✅ Databricks handles queuing

**Pattern 3: Databricks Workflows Multi-Task**
```yaml
tasks:
  - task_key: date_1
    notebook: 02_process_data
    params: {position_date: "2024-06-01"}
  - task_key: date_2
    notebook: 02_process_data
    params: {position_date: "2024-06-02"}
```
- ✅ Visual workflow DAG
- ❌ Hard to generate dynamically for many dates

### 5. Delta Lake Features Used

- **Partitioning**: Fast reads/writes for specific dates
- **ACID Transactions**: Atomic writes (all-or-nothing)
- **replaceWhere**: Partition-level overwrite
- **Managed Tables**: Unity Catalog handles storage

---

## Best Practices

### Performance

1. **Partition Strategy**:
   - Partition by date (position_date)
   - Avoids scanning entire table
   - Enables efficient replaceWhere

2. **Parallel Execution**:
   - Set appropriate `max_concurrent_runs`
   - Balance parallelism vs. cluster resources
   - Monitor cluster auto-scaling

3. **Date Range Sizing**:
   - Process 30-90 days at a time
   - Avoid single job for years of data
   - Break large ranges into batches

### Reliability

1. **Retry Logic**:
   - Enable retry on timeout/transient errors
   - Set reasonable retry intervals
   - Don't retry indefinitely

2. **Error Logging**:
   - Log ALL runs (success/failure/skip)
   - Capture error messages
   - Include job URLs for debugging

3. **Monitoring**:
   - Query backfill_log regularly
   - Set up alerts for high failure rates
   - Track processing duration trends

### Maintainability

1. **Configuration Management**:
   - Use config.py for all constants
   - Never hardcode catalog/schema names
   - Document environment-specific values

2. **Code Reusability**:
   - Keep notebooks focused (single responsibility)
   - Extract common logic to shared modules
   - Use widgets for parameterization

3. **Testing**:
   - Test on single date first
   - Verify idempotency (run twice, check counts)
   - Test failure scenarios

### Security

1. **Access Control**:
   - Grant minimal permissions
   - Use service principals for production
   - Audit backfill_log for user tracking

2. **Data Validation**:
   - Check record counts before/after
   - Validate data quality in process_data
   - Log anomalies for investigation

---

## Common Issues & Solutions

### Issue 1: "Job not found" error

**Cause**: Stale job metadata in system.lakeflow.jobs

**Solution**: 
- Run `04_job_manager.ipynb` to recreate jobs
- Use `list_jobs_by_name()` which verifies via API

### Issue 2: Duplicate job runs

**Cause**: Triggering multiple runs for same date simultaneously

**Solution**:
- Check backfill_log before triggering
- Use queue enabled to handle overlaps
- Implement locking mechanism (advanced)

### Issue 3: Slow processing

**Cause**: Sequential execution or insufficient parallelism

**Solution**:
- Increase `max_concurrent_runs`
- Use larger cluster for process_data job
- Enable auto-scaling

### Issue 4: Calendar validation failures

**Cause**: Calendar table missing dates or incorrect flags

**Solution**:
- Verify calendar table coverage: `SELECT MIN(date), MAX(date) FROM calendar`
- Check holiday logic for your region
- Regenerate calendar if needed

---

## Next Steps

### Enhancements

1. **Add Data Quality Checks**:
   - Record count validation
   - Schema validation
   - Business rule checks

2. **Implement Alerting**:
   - Email on failures
   - Slack notifications
   - PagerDuty integration

3. **Build Dashboard**:
   - Backfill progress visualization
   - Success rate charts
   - Duration trends

4. **Add Data Lineage**:
   - Track upstream dependencies
   - Version control for job configs
   - Audit trail for schema changes

### Production Readiness

- [ ] Configure production catalog/schema
- [ ] Set up service principal for job execution
- [ ] Implement monitoring alerts
- [ ] Create runbook for common issues
- [ ] Document SLAs and escalation paths
- [ ] Set up backup/disaster recovery

---

## Additional Resources

- [Databricks Backfill Jobs Documentation](https://docs.databricks.com/aws/en/jobs/backfill-jobs)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io/)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)

---

## Support

For issues or questions:
1. Check backfill_log table for error details
2. Review job run logs in Databricks UI
3. Verify calendar table and config.py settings
4. Test with single date before full backfill

---

---

## Feature Highlights

### 🔄 Automated Retry System
- **Smart Failure Detection**: Automatically identifies failed jobs from log table
- **Retry Tracking**: JSON metadata links retries to original failures
- **Retry Analytics**: Query success rates and retry patterns
- **Schedulable**: Set up daily/weekly automated retry jobs
- **Backward Compatible**: Works with existing tables (adds columns automatically)

### 📊 Comprehensive Logging
- **Complete Audit Trail**: Every run logged (SUCCESS/FAILED/SKIPPED)
- **Job Metadata**: Captures run IDs, creator, timestamps, error messages
- **Business Day Validation**: Automatically skips non-business dates
- **Retry Lineage**: Track original failures and subsequent retry attempts

### ⚡ Production-Ready Features
- **Idempotent Writes**: Safe to rerun without duplicate data
- **Parallel Processing**: Up to 20 concurrent date processing jobs
- **Retry Logic**: Automatic retries for transient failures
- **Error Handling**: Comprehensive error capture and logging
- **Dynamic Configuration**: Centralized config for easy environment changes

---

**Last Updated**: November 27, 2025
**Project Version**: 2.0 (with Retry System)
**Compatible Databricks Runtime**: 13.0+
