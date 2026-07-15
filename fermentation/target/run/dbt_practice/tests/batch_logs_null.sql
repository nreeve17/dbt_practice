
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    *
FROM
    "nicome"."dbt_nicoreeve"."batch_log_metrics"
where
    percent_phb_at_harvest is NULL and percent_phb_at_nd is NULL
  
  
      
    ) dbt_internal_test