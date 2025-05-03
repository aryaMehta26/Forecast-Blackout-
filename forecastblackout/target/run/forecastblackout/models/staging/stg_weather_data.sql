
  create or replace   view USER_DB_CHIPMUNK.analytics.stg_weather_data
  
   as (
    with base as (
    select
        INCIDENT_ID,
        LATITUDE,
        LONGITUDE,
        TEMPERATURE_C,
        WINDSPEED_KPH,
        PRECIP_MM,
        APPARENT_TEMP_C,
        HUMIDITY_PCT,
        PRESSURE_HPA
    from USER_DB_CHIPMUNK.RAW.weather_data
)

select * from base
  );

