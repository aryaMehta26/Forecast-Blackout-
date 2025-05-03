select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    INCIDENT_ID as unique_field,
    count(*) as n_records

from USER_DB_CHIPMUNK.analytics.stg_outage_data
where INCIDENT_ID is not null
group by INCIDENT_ID
having count(*) > 1



      
    ) dbt_internal_test