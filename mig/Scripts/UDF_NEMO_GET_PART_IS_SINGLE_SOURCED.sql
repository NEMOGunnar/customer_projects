CREATE FUNCTION UDF_NEMO_GET_PART_IS_SINGLE_SOURCED (input_part_i_d VARCHAR(128), input_process_date DATE)
RETURNS part_is_single_sourced BOOLEAN
AS
BEGIN
	DECLARE supplier_count INT;
	IF input_part_i_d IS NOT NULL AND LENGTH(TRIM(input_part_i_d)) > 0 THEN
        SELECT COALESCE(COUNT(DISTINCT supplier_i_d), 0)
        INTO supplier_count
        FROM MIG."pa_export"
        WHERE part_i_d = input_part_i_d
        AND process_date >= ADD_YEARS(input_process_date,-1);
   	ELSE 
   		supplier_count := 0;
    END IF;
	IF supplier_count = 0 THEN
		part_is_single_sourced = NULL;
	ELSEIF supplier_count = 1 THEN
		part_is_single_sourced = TRUE;
	ELSE
		part_is_single_sourced = FALSE;
	END IF;
END;