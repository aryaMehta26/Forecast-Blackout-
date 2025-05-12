from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

with DAG(
    dag_id="county_weather_persistence_forecast",
    start_date=datetime(2025, 5, 2),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather", "persistence", "forecast"],
) as dag:

    @task
    def extract_weather_data() -> pd.DataFrame:
        """Extract historical weather data from Snowflake"""
        try:
            hook = SnowflakeHook(snowflake_conn_id="Snowflake_Connect")
            sql = """
                SELECT
                  START_TIME as ds,
                  COUNTY,
                  TEMPERATURE_C,
                  WINDSPEED_KPH,
                  PRECIP_MM,
                  APPARENT_TEMP_C,
                  HUMIDITY_PCT,
                  PRESSURE_HPA
                FROM user_db_chipmunk.analytics.int_outage_weather_joined
                WHERE START_TIME IS NOT NULL
            """
            df = hook.get_pandas_df(sql)
            df.columns = df.columns.str.lower()
            df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
            return df.dropna(subset=['ds'])
        except Exception as e:
            raise ValueError(f"Data extraction failed: {str(e)}")

    @task
    def preprocess_data(df: pd.DataFrame) -> dict:
        """Prepare data for persistence model"""
        try:
            county_data = {}
            for county, group in df.groupby('county'):
                # Resample to hourly and forward fill
                resampled = (
                    group.set_index('ds')
                    .resample('1H')
                    .first()
                    .ffill()
                )
                county_data[county] = resampled.reset_index()
            return county_data
        except Exception as e:
            raise ValueError(f"Preprocessing failed: {str(e)}")

    @task
    def generate_persistence_forecast(county_data: dict) -> pd.DataFrame:
        """Generate forecasts using persistence model (last observation carried forward)"""
        try:
            all_forecasts = []
            forecast_hours = 24  # 24-hour forecast
            
            for county, data in county_data.items():
                if data.empty:
                    continue
                    
                last_record = data.iloc[-1]
                forecast_dates = pd.date_range(
                    start=last_record['ds'] + timedelta(hours=1),
                    periods=forecast_hours,
                    freq='H'
                )
                
                forecast_df = pd.DataFrame({
                    'ds': forecast_dates,
                    'county': county,
                    'temperature_c': last_record['temperature_c'],
                    'windspeed_kph': last_record['windspeed_kph'],
                    'precip_mm': last_record['precip_mm'],
                    'apparent_temp_c': last_record['apparent_temp_c'],
                    'humidity_pct': last_record['humidity_pct'],
                    'pressure_hpa': last_record['pressure_hpa']
                })
                all_forecasts.append(forecast_df)
                
            return pd.concat(all_forecasts, ignore_index=True)
        except Exception as e:
            raise ValueError(f"Forecast generation failed: {str(e)}")

    @task
    def calculate_outage_risk(forecast_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate outage risk flags"""
        try:
            forecast_df['forecast_outage'] = (
                (forecast_df['windspeed_kph'] > 35) |
                (forecast_df['precip_mm'] > 0.5) |
                ((forecast_df['temperature_c'] < 0) | (forecast_df['temperature_c'] > 30)) |
                ((forecast_df['humidity_pct'] < 30) | (forecast_df['humidity_pct'] > 70)) |
                ((forecast_df['pressure_hpa'] < 1000) | (forecast_df['pressure_hpa'] > 1010))
            )
            return forecast_df
        except Exception as e:
            raise ValueError(f"Risk calculation failed: {str(e)}")

    @task
    def load_results(df: pd.DataFrame):
        """Load results to Snowflake"""
        try:
            # Convert datetime to string for Snowflake
            df['ds'] = df['ds'].astype(str)
            
            hook = SnowflakeHook(snowflake_conn_id="Snowflake_Connect")
            conn = hook.get_conn()
            
            write_pandas(
                conn,
                df,
                table_name="COUNTY_WEATHER_FORECASTS",
                schema="ANALYTICS",
                database="USER_DB_CHIPMUNK",
                auto_create_table=True,
                overwrite=True
            )
        except Exception as e:
            raise ValueError(f"Data load failed: {str(e)}")

    # DAG workflow
    raw_data = extract_weather_data()
    processed_data = preprocess_data(raw_data)
    forecasts = generate_persistence_forecast(processed_data)
    risk_assessment = calculate_outage_risk(forecasts)
    load_results(risk_assessment)