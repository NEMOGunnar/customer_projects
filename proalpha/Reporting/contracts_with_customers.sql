WITH CTEContracts AS (
	SELECT * FROM nemo."VH0017_15_.XVH0017_snrAuftragsuebersichtNEMO15"
	UNION ALL
	SELECT * FROM nemo."VH0017_18_.XVH0017_snrAuftragsuebersichtNEMO18"
	UNION ALL
	SELECT * FROM nemo."VH0017_21_.XVH0017_snrAuftragsuebersichtNEMO21"
	UNION ALL
	SELECT * FROM nemo."VH0017_30_.XVH0017_snrAuftragsuebersichtNEMO30"
)
,RankedData AS (
    SELECT
          *
        , DENSE_RANK () OVER (PARTITION BY FIRMA_VERTRAG, KUNDE ORDER BY X_INDEX DESC) AS RowNum
    FROM CTEContracts
)
, customers AS (
	SELECT DISTINCT 
		company
		, customer_i_d
		, CUSTOMER_SINCE
		, X_XBESTANDSKUNDE_SEIT
	FROM 
		NEMO."pa_export"
)
SELECT 
	rd.*
	, pe.CUSTOMER_SINCE
	, pe.X_XBESTANDSKUNDE_SEIT 
FROM 
 	RankedData rd
LEFT JOIN customers pe 
	ON pe.COMPANY  = rd.FIRMA_VERTRAG 
	AND pe.CUSTOMER_I_D = rd.Kunde
WHERE 
    rd.RowNum = 1 	