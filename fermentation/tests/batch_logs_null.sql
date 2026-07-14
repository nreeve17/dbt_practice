select
    *
FROM
    {{ ref('batch_log_metrics') }}
where
    percent_phb_at_harvest is NULL and percent_phb_at_nd is NULL