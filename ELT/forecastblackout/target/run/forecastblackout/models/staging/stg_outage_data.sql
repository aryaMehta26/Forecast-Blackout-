
  create or replace   view USER_DB_CHIPMUNK.analytics.stg_outage_data
  
   as (
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
    from USER_DB_BOA.RAW.outage_data
    where OUTAGE_TYPE != 'Planned'
)

select * from base
  );

