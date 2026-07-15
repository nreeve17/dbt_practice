select 
    *
FROM
    "nicome"."dbt_nicoreeve"."genomic_metadata_passing"
where
    seq_count < 5000