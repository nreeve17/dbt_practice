
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select 
    *
FROM
    "nicome"."dbt_nicoreeve"."genomic_metadata_passing"
where
    seq_count < 5000
  
  
      
    ) dbt_internal_test