with base as (
    select
        INCIDENT_ID,
        START_TIME,
        COUNTY,
        LAT,
        LON,
        UTILITY,
        CUSTOMERS_OUT,
        OUTAGE_TYPE,
        OUTAGE_STATUS
    from {{ source('user_db_boa', 'outage_data') }}
    where OUTAGE_TYPE != 'Planned'
)

select * from base
