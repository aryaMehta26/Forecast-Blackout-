-- Join outages with weather using INCIDENT_ID
select
    o.INCIDENT_ID,
    o.LATITUDE,
    o.LONGITUDE,
    o.OUTAGE_TYPE,
    o.START_TIME,
    o.COUNTY,
    o.UTILITY_COMPANY,
    o.CUSTOMERS_AFFECTED,
    w.TEMPERATURE_C,
    w.WINDSPEED_KPH,
    w.PRECIP_MM,
    w.APPARENT_TEMP_C,
    w.HUMIDITY_PCT,
    w.PRESSURE_HPA
from {{ ref('stg_outage_data') }} o
left join {{ ref('stg_weather_data') }} w
  on o.INCIDENT_ID = w.INCIDENT_ID
