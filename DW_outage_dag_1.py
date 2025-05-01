from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import pandas as pd
import datetime as dt
from snowflake.connector.pandas_tools import write_pandas

# ─── CONFIG ───
SNOWFLAKE_CONN_ID = "snowflake_connection"
SF_DATABASE       = "USER_DB_CHIPMUNK"
SF_SCHEMA         = "RAW"
SF_TABLE          = "POWER_OUTAGE_TABLE"
SCHEDULE_CRON     = "*/15 * * * *"

OUTAGE_URL = (
    "https://services.arcgis.com/BLN4oKB0N1YSgvY8/ArcGIS/"
    "rest/services/Power_Outages_%28View%29/FeatureServer/0/query"
)

# ─── Helper to fetch lat/lon by county ───
def get_latlon_from_county(county_name):
    try:
        response = requests.get(
            f"https://nominatim.openstreetmap.org/search?county={county_name}&state=California&country=USA&format=json",
            headers={"User-Agent": "forecastblackout-etl"}
        )
        data = response.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            return lat, lon
    except Exception as e:
        print(f"Error fetching lat/lon for county {county_name}: {e}")
    return None, None

# ─── Extract ───
def extract_power_outages(**context):
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json"
    }
    data = requests.get(OUTAGE_URL, params=params, timeout=30).json()
    context["ti"].xcom_push(key="raw_features", value=data.get("features", []))

# ─── Transform ───
def transform_to_dataframe(**context):
    feats = context["ti"].xcom_pull(key="raw_features", task_ids="extract")
    snap_ts = pd.Timestamp.utcnow()
    rows = []
    for f in feats:
        a = f.get("attributes", {})
        county = a.get("County")
        lat, lon = get_latlon_from_county(county)
        if lat is None or lon is None:
            continue

        start_ms = a.get("StartDate")
        restore_ms = a.get("EstimatedRestoreDate")

        start_time = pd.to_datetime(start_ms, unit="ms", utc=True) if isinstance(start_ms, (int, float)) and start_ms > 946684800000 else pd.NaT
        restore_time = pd.to_datetime(restore_ms, unit="ms", utc=True) if isinstance(restore_ms, (int, float)) and restore_ms > 946684800000 else pd.NaT

        rows.append({
            "SNAPSHOT_TIME_UTC": snap_ts,
            "INCIDENT_ID":       a.get("OBJECTID"),
            "UTILITY":           a.get("UtilityCompany"),
            "COUNTY":            county,
            "CUSTOMERS_OUT":     a.get("ImpactedCustomers"),
            "OUTAGE_STATUS":     a.get("OutageStatus"),
            "OUTAGE_TYPE":       a.get("OutageType"),
            "START_TIME":        start_time,
            "EST_RESTORE":       restore_time,
            "LAT":               lat,
            "LON":               lon
        })

    df = pd.DataFrame(rows)
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].astype(str)
    context["ti"].xcom_push(key="clean_df", value=df.to_dict(orient="records"))

# ─── Load ───
def load_to_snowflake(**context):
    records = context["ti"].xcom_pull(key="clean_df", task_ids="transform")
    df = pd.DataFrame(records)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with hook.get_conn() as conn:
        success, _, _, _ = write_pandas(
            conn,
            df,
            table_name=SF_TABLE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            auto_create_table=True,
            overwrite=False
        )
        if not success:
            raise RuntimeError("write_pandas failed")

# ─── DAG ───
default_args = {"retries": 2, "retry_delay": dt.timedelta(minutes=5)}

with DAG(
    dag_id="forecastblackout_outages",
    description="Fetch live outage incidents every 15 min",
    start_date=days_ago(1),
    schedule_interval=SCHEDULE_CRON,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "outages", "forecastblackout"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_power_outages)
    transform = PythonOperator(task_id="transform", python_callable=transform_to_dataframe)
    load = PythonOperator(task_id="load", python_callable=load_to_snowflake)

    extract >> transform >> load
