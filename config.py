# Backfill Demo - Configuration File
# This file contains environment-specific settings used across all notebooks

# Database Configuration
CATALOG = "demos"
SCHEMA = "backfill_demo"

# Table Names
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.source_data"
DEST_TABLE = f"{CATALOG}.{SCHEMA}.destination_data"
CALENDAR_TABLE = f"{CATALOG}.{SCHEMA}.calendar"
BACKFILL_LOG_TABLE = f"{CATALOG}.{SCHEMA}.backfill_log"

# Display configuration
def print_config():
    """Print current configuration"""
    print(f"📁 Catalog: {CATALOG}")
    print(f"📂 Schema: {SCHEMA}")
    print(f"📊 Tables:")
    print(f"  • Source: {SOURCE_TABLE}")
    print(f"  • Destination: {DEST_TABLE}")
    print(f"  • Calendar: {CALENDAR_TABLE}")
    print(f"  • Backfill Log: {BACKFILL_LOG_TABLE}")
