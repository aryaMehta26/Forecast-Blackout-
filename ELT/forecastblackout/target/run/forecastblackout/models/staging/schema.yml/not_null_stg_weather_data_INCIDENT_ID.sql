select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select INCIDENT_ID
from USER_DB_CHIPMUNK.analytics.stg_weather_data
where INCIDENT_ID is null



      
    ) dbt_internal_test