import uuid
from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from datetime import datetime, timedelta
import json
import urllib.request
import pandas as pd # type: ignore
from snowflake.connector.pandas_tools import write_pandas # type: ignore
import requests # type: ignore
from pyproj import Transformer # type: ignore


# Get a Snowflake cursor
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  # Update conn_id if needed
    conn = hook.get_conn()
    return conn, conn.cursor()

# Extract Task
@task
def extract_power_outage_data():
    OUTAGE_URL = (
        "https://services.arcgis.com/BLN4oKB0N1YSgvY8/ArcGIS/"
        "rest/services/Power_Outages_%28View%29/FeatureServer/0/query"
    )

    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json"
    }

    try:
        response = requests.get(OUTAGE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        transformer = Transformer.from_crs("epsg:3857", "epsg:4326", always_xy=True)

        records = []
        for feature in features:
            record = feature.get("attributes", {})
            geometry = feature.get("geometry", {})

            # Convert x/y to lon/lat
            x = geometry.get("x") or record.get("x")
            y = geometry.get("y") or record.get("y")

            if x is not None and y is not None:
                try:
                    lon, lat = transformer.transform(x, y)
                    record["longitude"] = lon
                    record["latitude"] = lat
                except Exception as conv_err:
                    print(f"⚠️ Coordinate conversion failed for x={x}, y={y}: {conv_err}")
                    record["longitude"] = None
                    record["latitude"] = None
            else:
                record["longitude"] = None
                record["latitude"] = None

            records.append(record)

        print(f"✅ Extracted {len(records)} records from ArcGIS API with lat/lon.")
        return records

    except Exception as e:
        print("❌ Failed to extract data:", e)
        return []

@task
def transform_data(records):
    if not records:
        print("⚠️ No records to transform.")
        return []

    df = pd.DataFrame(records)
    print("📌 Raw columns:", df.columns.tolist())

    # Define mapping from ArcGIS to internal schema
    column_mapping = {
        'OBJECTID': 'objectid',
        'IncidentId': 'incident_id',
        'UtilityCompany': 'utility_company',
        'StartDate': 'start_time',
        'EstimatedRestoreDate': 'end_time',
        'Cause': 'cause',
        'ImpactedCustomers': 'customers_affected',
        'County': 'county',
        'OutageStatus': 'status',
        'OutageType': 'outage_type',
        'longitude': 'longitude',
        'latitude': 'latitude'
    }

    df = df[[col for col in column_mapping if col in df.columns]]
    df = df.rename(columns=column_mapping)

    # Convert timestamps
    df['start_time'] = pd.to_datetime(df['start_time'], unit='ms')
    df['end_time'] = pd.to_datetime(df['end_time'], unit='ms')

    df['start_time'] = df['start_time'].dt.floor('S')
    df['end_time'] = df['end_time'].dt.floor('S')

    # Clean and fill null values
    string_cols = ['utility_company', 'cause', 'county', 'status']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Convert numeric columns safely
    df['customers_affected'] = pd.to_numeric(df['customers_affected'], errors='coerce').fillna(0).astype(int)

    # Print example
    print("✅ Transformed sample record:\n", df.head(1).to_dict(orient='records'))

    # Convert datetime columns to string for XCom compatibility
    if 'start_time' in df.columns:
        df['start_time'] = df['start_time'].astype(str)
    if 'end_time' in df.columns:
        df['end_time'] = df['end_time'].astype(str)

    df.columns = [col.upper() for col in df.columns]  # for Snowflake compatibility
    print(df.head(1).to_dict(orient='records'))  # Print first record for debugging
    return df.to_dict(orient='records')

# Load Task
@task
def load_to_snowflake(records):
    if not records:
        print("⚠️ No records to load.")
        return

    print("🔍 Keys in first record:")
    print(records[0].keys() if records else "No records returned.")

    print("🔍 First record received for loading:")
    print(records[0] if records else "No records!")


    df = pd.DataFrame(records)
    
    conn, cursor = return_snowflake_conn()  # unified session
    print("🔗 Connected to Snowflake.")

    staging_table = "USER_DB_BOA.RAW.OUTAGE_STAGE"
    target_table = "USER_DB_BOA.RAW.OUTAGE_DATA"

    try:
        cursor.execute("BEGIN")

        # cursor.execute("CREATE DATABASE IF NOT EXISTS USER_DB_BOA;")

        # Use the database before creating schemas
        cursor.execute("USE DATABASE USER_DB_BOA;")
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        # cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

        # Step 1: Create staging table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {staging_table} (
                OBJECTID             NUMBER,
                INCIDENT_ID          STRING,
                UTILITY_COMPANY      STRING,
                START_TIME           DATETIME,
                END_TIME             DATETIME,
                CAUSE                STRING,
                CUSTOMERS_AFFECTED   STRING,
                COUNTY               STRING,
                STATUS               STRING,
                OUTAGE_TYPE          STRING,
                LATITUDE             FLOAT,
                LONGITUDE            FLOAT
            );
        """)

        # Step 2: Truncate before reload
        cursor.execute(f"TRUNCATE TABLE {staging_table}")

        # Step 3: Load into staging
        # Fix datetime issues
        for time_col in ['START_TIME', 'END_TIME']:
            if time_col in df.columns:
                df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
                df[time_col] = df[time_col].dt.strftime('%Y-%m-%d %H:%M:%S')  # convert to proper string
                df[time_col] = df[time_col].where(df[time_col].notnull(), None)

        success, nchunks, nrows, _ = write_pandas(conn, df, table_name=staging_table.split('.')[-1], schema="RAW")
        print(f"✅ Loaded {nrows} records into staging table.")

        # Step 4: Create target if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                OBJECTID             NUMBER PRIMARY KEY,
                INCIDENT_ID          STRING,
                UTILITY_COMPANY      STRING,
                START_TIME           DATETIME,
                END_TIME             DATETIME,
                CAUSE                STRING,
                CUSTOMERS_AFFECTED   STRING,
                COUNTY               STRING,
                STATUS               STRING,
                OUTAGE_TYPE          STRING,
                LATITUDE             FLOAT,
                LONGITUDE            FLOAT
            );
        """)

        # Step 5: MERGE into final table
        cursor.execute(f"""
            MERGE INTO {target_table} tgt
            USING {staging_table} src
            ON tgt.OBJECTID = src.OBJECTID
            WHEN MATCHED THEN UPDATE SET
                INCIDENT_ID = src.INCIDENT_ID,
                UTILITY_COMPANY = src.UTILITY_COMPANY,
                START_TIME = src.START_TIME,
                END_TIME = src.END_TIME,
                CAUSE = src.CAUSE,
                CUSTOMERS_AFFECTED = src.CUSTOMERS_AFFECTED,
                COUNTY = src.COUNTY,
                STATUS = src.STATUS,
                OUTAGE_TYPE = src.OUTAGE_TYPE,
                LATITUDE = src.LATITUDE,
                LONGITUDE = src.LONGITUDE
            WHEN NOT MATCHED THEN INSERT (
                OBJECTID, INCIDENT_ID, UTILITY_COMPANY,
                START_TIME, END_TIME, CAUSE, CUSTOMERS_AFFECTED,
                COUNTY, STATUS, OUTAGE_TYPE, LATITUDE, LONGITUDE
            )
            VALUES (
                src.OBJECTID, src.INCIDENT_ID, src.UTILITY_COMPANY,
                src.START_TIME, src.END_TIME, src.CAUSE, src.CUSTOMERS_AFFECTED,
                src.COUNTY, src.STATUS, src.OUTAGE_TYPE, src.LATITUDE, src.LONGITUDE
            );
        """)

        cursor.execute("COMMIT")
        print(f"✅ Merge complete. {nrows} records upserted into final table.")

    except Exception as e:
        cursor.execute("ROLLBACK")
        print("❌ Merge failed:", e)
        raise e
    finally:
        cursor.close()

# DAG Definition
with DAG(
    dag_id='California_Power_Outage_ETL_PowerOutage',
    start_date=datetime(2025, 4, 28),
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['ETL', 'PowerOutage']
) as dag:

    raw_data = extract_power_outage_data()
    transformed_data = transform_data(raw_data)
    load_to_snowflake(transformed_data)
