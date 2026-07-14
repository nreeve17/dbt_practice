

with harvest_samples as (
	SELECT 
		run_id,
		sample_type as sample_type_harvest,
		harvest_sample,
		percent_phb_peak_area as percent_phb_at_harvest,
		dcw_by_ma as dcw_by_ma_at_harvest
		
	from process_data 
	WHERE sample_type='harvest'
),
nd_samples as (
	SELECT 
		run_id,
		sample_type as sample_type_nd,
		harvest_sample,
		percent_phb_peak_area as percent_phb_at_nd,
		dcw_by_ma as dcw_by_ma_at_nd
	from process_data 
	WHERE sample_type='nd'
)

SELECT 
	h.run_id,
	h.percent_phb_at_harvest,
	h.dcw_by_ma_at_harvest,
	n.percent_phb_at_nd,
	n.dcw_by_ma_at_nd
from harvest_samples h
JOIN nd_samples n 
on h.run_id=n.run_id