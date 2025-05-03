with base as (
    select
        INCIDENT_ID,
        START_TIME,
        COUNTY,
        LATITUDE,
        LONGITUDE,
        UTILITY_COMPANY,
        CUSTOMERS_AFFECTED,
        OUTAGE_TYPE,
        STATUS
    from {{ source('user_db_boa', 'outage_data') }}
    where OUTAGE_TYPE != 'Planned'
)

select * from base
