


with gm_seq_count as (

    select
     gm.*, sum(gc.count) as seq_count
    from genomic_metadata gm 
    JOIN "nicome"."dbt_nicoreeve"."genomic_counts_dereplicated" gc on gm.sequenceuuid = gc.sequenceuuid
    GROUP by gm.sequenceuuid
)
select *
 from gm_seq_count
 where seq_count >= 5000