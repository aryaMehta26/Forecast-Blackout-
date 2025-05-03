with outage as (
    select * from {{ ref('stg_outage_data') }}
),

weather as (
    select * from {{ ref('stg_weather_data') }}
),

joined as (
    select
        o.INCIDENT_ID,
        o.START_TIME,
        o.COUNTY,
        o.LATITUDE,
        o.LONGITUDE,
        o.UTILITY,
        o.CUSTOMERS_OUT,
        o.OUTAGE_TYPE,
        o.OUTAGE_STATUS,
        w.TEMPERATURE_C,
        w.WINDSPEED_KPH,
        w.PRECIP_MM,
        w.APPARENT_TEMP_C,
        w.HUMIDITY_PCT,
        w.PRESSURE_HPA
    from outage o
    left join weather w
        on o.INCIDENT_ID = w.INCIDENT_ID
        and o.LATITUDE = w.LATITUDE
        and o.LONGITUDE = w.LONGITUDE
)

select * from joined
