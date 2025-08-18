SELECT 
	part_i_d
	, UDF_NEMO_GET_PART_IS_SINGLE_SOURCED(part_i_d, CURRENT_DATE)
FROM 
	mig."pa_export" 
WHERE part_type = 10
GROUP BY 
	part_i_d