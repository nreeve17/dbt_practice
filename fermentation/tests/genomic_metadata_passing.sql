select 
    *
FROM
    {{ ref('genomic_metadata_passing')}}
where
    seq_count < 5000