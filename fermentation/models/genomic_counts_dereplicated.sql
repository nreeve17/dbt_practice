{{
    config(
        materialized = 'incremental'
    )
}}

with counts_dereplicated as (
    select
        parent_sequenceuuid as sequenceuuid, 
        feature_id,
        round(avg(count))::int as count
    from genomic_counts_tech_replicates
    group by parent_sequenceuuid, feature_id
)
select 
    *
from
    counts_dereplicated