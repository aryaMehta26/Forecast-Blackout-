from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests, time, json, pandas as pd
import datetime as dt
from snowflake.connector.pandas_tools import write_pandas
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── CONFIG ─────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_connection"
DB, SCHEMA = "USER_DB_CHIPMUNK", "RAW"
OUTAGE_TABLE = "POWER_OUTAGE_TABLE"
WEATHER_TABLE = "WEATHER_AT_OUTAGE_TIME"

HOURLY_VARS = (
    "temperature_2m,wind_speed_10m,precipitation,"
    "apparent_temperature,relative_humidity_2m,surface_pressure"
)
UNITS = "&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=UTC"

# ─── Session with retry strategy ────────────────────────────────
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

# ─── TASK 1: Fetch Points ───────────────────────────────────────
def fetch_points(**context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    cur = hook.get_conn().cursor()
    cur.execute(f"""
        SELECT DISTINCT INCIDENT_ID, LAT, LON, START_TIME
        FROM {DB}.{SCHEMA}.{OUTAGE_TABLE}
        WHERE LAT IS NOT NULL AND LON IS NOT NULL AND START_TIME IS NOT NULL
        ORDER BY START_TIME DESC
        LIMIT 100
    """)
    rows = cur.fetchall()
    cur.close()
    context["ti"].xcom_push(key="outage_points", value=json.dumps(rows))

# ─── TASK 2: Fetch Weather ──────────────────────────────────────
def fetch_weather(**context):
    points = json.loads(context["ti"].xcom_pull(key="outage_points", task_ids="get_points"))
    records = []

    for incident_id, lat, lon, start_time in points:
        try:
            outage_time = pd.to_datetime(start_time)
            if pd.isna(outage_time):
                continue

            date_str = outage_time.strftime('%Y-%m-%d')
            hour_str = outage_time.strftime('%Y-%m-%dT%H:00')

            url = (
                f"https://archive-api.open-meteo.com/v1/archive"
                f"?latitude={lat}&longitude={lon}"
                f"&start_date={date_str}&end_date={date_str}"
                f"&hourly={HOURLY_VARS}{UNITS}"
            )

            response = session.get(url, timeout=10)
            data = response.json()
            if "hourly" not in data:
                continue

            df = pd.DataFrame(data["hourly"])
            df["time"] = pd.to_datetime(df["time"])
            row = df[df["time"] == pd.Timestamp(hour_str)]

            if row.empty:
                continue

            r = row.iloc[0]
            records.append((
                str(incident_id),
                float(lat), float(lon),
                outage_time.strftime('%Y-%m-%d %H:%M:%S'),
                float(r["temperature_2m"]),
                float(r["wind_speed_10m"]),
                float(r["precipitation"]),
                float(r["apparent_temperature"]),
                float(r["relative_humidity_2m"]),
                float(r["surface_pressure"])
            ))

            time.sleep(0.5)  # prevent hammering the API

        except Exception as e:
            print(f"⚠️ Skipped {incident_id} due to: {e}")
            continue

    context["ti"].xcom_push(key="weather_rows", value=json.dumps(records))

# ─── TASK 3: Load into Snowflake ─────────────────────────────────
def load_weather(**context):
    rows = json.loads(context["ti"].xcom_pull(key="weather_rows", task_ids="get_weather"))
    if not rows:
        print("❌ No rows to load.")
        return

    df = pd.DataFrame(rows, columns=[
        "INCIDENT_ID", "LAT", "LON", "OUTAGE_HOUR",
        "TEMP_2M_F", "WIND_SPEED_10M_MPH", "PRECIP_MM",
        "APPARENT_TEMP_F", "HUMIDITY_PCT", "PRESSURE_HPA"
    ])

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with hook.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.{WEATHER_TABLE} (
                INCIDENT_ID STRING,
                LAT FLOAT,
                LON FLOAT,
                OUTAGE_HOUR TIMESTAMP_NTZ,
                TEMP_2M_F FLOAT,
                WIND_SPEED_10M_MPH FLOAT,
                PRECIP_MM FLOAT,
                APPARENT_TEMP_F FLOAT,
                HUMIDITY_PCT FLOAT,
                PRESSURE_HPA FLOAT
            )
        """)
        success, _, _, _ = write_pandas(
            conn, df,
            table_name=WEATHER_TABLE,
            schema=SCHEMA,
            database=DB,
            auto_create_table=False,
            overwrite=False
        )
        if not success:
            raise RuntimeError("write_pandas failed")

# ─── DAG Definition ─────────────────────────────────────────────
default_args = {"retries": 1, "retry_delay": dt.timedelta(minutes=10)}

with DAG(
    dag_id="weather_at_outage_time",
    description="Enrich outage points with archived weather from Open-Meteo",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # hourly
    catchup=False,
    default_args=default_args,
    tags=["forecastblackout", "weather", "archive"],
) as dag:

    get_points = PythonOperator(task_id="get_points", python_callable=fetch_points)
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=fetch_weather,
        execution_timeout=dt.timedelta(minutes=20)
    )
    load = PythonOperator(task_id="load_weather", python_callable=load_weather)

    get_points >> get_weather >> load
