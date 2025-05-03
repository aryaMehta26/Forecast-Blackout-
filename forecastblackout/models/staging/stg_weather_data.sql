with base as (
    select
        INCIDENT_ID,
        LAT,
        LON,
        TEMPERATURE_C,
        WINDSPEED_KPH,
        PRECIP_MM,
        APPARENT_TEMP_C,
        HUMIDITY_PCT,
        PRESSURE_HPA
    from {{ source('user_db_chipmunk', 'weather_data') }}
)

select * from base
