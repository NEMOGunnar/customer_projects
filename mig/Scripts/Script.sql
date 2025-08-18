WITH CTEDefMining AS (
    SELECT
        s_kunde_kunde__001_,
		s_adresse_name1__003_,
		s_adresse_staat__004_,
		s_adresse_ort__005_,
		s_adresse_suchbegriff__006_,
		s_adresse_selektion__007_,
		s_adresse_name2__012_,
		s_adresse_name3__013_,
		s_adresse_plz__014_,
		s_adresse_strasse__018_,
		s_adresse_hausnummer__020_,
		s_adresse_plz_postfach__022_,
		s_adresse_postfach__023_,
		s_adresse_email__025_,
		s_adresse_homepage__026_,
		s_adresse_handy__027_,
		s_adresse_telefon__028_,
		s_adresse_longitude__033_,
		s_adresse_latitude__034_,
		s_kunde_suchbegriff__035_,
		s_kunde_branche__038_
        ,LTRIM(CASE
		WHEN s_kunde_kunde__001_ IS NULL OR s_kunde_kunde__001_ = ''
		THEN  CHAR(10) || 'S_Kunde.Kunde (001) is mandatory and must not be empty'
		ELSE ''
	END ||
	CASE
		WHEN INSTR(s_kunde_kunde__001_, '-') > 0
		THEN  CHAR(10) || 'S_Kunde.Kunde (001) must not contain a minus sign (expected format: zzzzzzzz)'
		ELSE ''
	END ||
	CASE
		WHEN INSTR(REPLACE(s_kunde_kunde__001_, '-', ''), ',') > 0
		THEN  CHAR(10) || 'S_Kunde.Kunde (001) must not contain decimal places (expected format: zzzzzzzz)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_kunde_kunde__001_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_kunde_kunde__001_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_kunde_kunde__001_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_kunde_kunde__001_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 8
		THEN  CHAR(10) || 'S_Kunde.Kunde (001) has too many digits before the decimal point (maximum 8 allowed; expected format: zzzzzzzz)'
		ELSE ''
	END ||
	CASE
		WHEN NOT REPLACE(s_kunde_kunde__001_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$')
		THEN  CHAR(10) || 'S_Kunde.Kunde (001) is not a valid number (expected German format, e.g. 1.234,56; format: zzzzzzzz)'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_name1__003_ IS NULL OR s_adresse_name1__003_ = ''
		THEN  CHAR(10) || 'S_Adresse.Name1 (003) is mandatory and must not be empty'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_name1__003_) > 80
		THEN  CHAR(10) || 'S_Adresse.Name1 (003) exceeds field length (max 80 digits)'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_staat__004_ IS NULL OR s_adresse_staat__004_ = ''
		THEN  CHAR(10) || 'S_Adresse.Staat (004) is mandatory and must not be empty'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_staat__004_) > 3
		THEN  CHAR(10) || 'S_Adresse.Staat (004) exceeds field length (max 3 digits)'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_ort__005_ IS NULL OR s_adresse_ort__005_ = ''
		THEN  CHAR(10) || 'S_Adresse.Ort (005) is mandatory and must not be empty'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_ort__005_) > 30
		THEN  CHAR(10) || 'S_Adresse.Ort (005) exceeds field length (max 30 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_suchbegriff__006_) > 12
		THEN  CHAR(10) || 'S_Adresse.Suchbegriff (006) exceeds field length (max 12 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_selektion__007_) > 20
		THEN  CHAR(10) || 'S_Adresse.Selektion (007) exceeds field length (max 20 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_name2__012_) > 80
		THEN  CHAR(10) || 'S_Adresse.Name2 (012) exceeds field length (max 80 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_name3__013_) > 80
		THEN  CHAR(10) || 'S_Adresse.Name3 (013) exceeds field length (max 80 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_plz__014_) > 10
		THEN  CHAR(10) || 'S_Adresse.PLZ (014) exceeds field length (max 10 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_strasse__018_) > 50
		THEN  CHAR(10) || 'S_Adresse.Strasse (018) exceeds field length (max 50 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_hausnummer__020_) > 12
		THEN  CHAR(10) || 'S_Adresse.Hausnummer (020) exceeds field length (max 12 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_plz_postfach__022_) > 10
		THEN  CHAR(10) || 'S_Adresse.PLZ_Postfach (022) exceeds field length (max 10 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_postfach__023_) > 30
		THEN  CHAR(10) || 'S_Adresse.Postfach (023) exceeds field length (max 30 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_email__025_) > 254
		THEN  CHAR(10) || 'S_Adresse.EMail (025) exceeds field length (max 254 digits)'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_email__025_ LIKE_REGEXPR '[ \t\n\r\[\],;:\\()]'
		THEN  CHAR(10) || 'S_Adresse.EMail (025) contains invalid characters (e.g., space, brackets, semicolon, colon)'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_email__025_ LIKE_REGEXPR '\.\.'
		THEN  CHAR(10) || 'S_Adresse.EMail (025) contains consecutive dots'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_email__025_ LIKE '.%'
		THEN  CHAR(10) || 'S_Adresse.EMail (025) starts with a dot'
		ELSE ''
	END ||
	CASE
		WHEN s_adresse_email__025_ LIKE '%.'
		THEN  CHAR(10) || 'S_Adresse.EMail (025) ends with a dot'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_email__025_) - LENGTH(REPLACE(s_adresse_email__025_, '@', '')) <> 1
		THEN  CHAR(10) || 'S_Adresse.EMail (025) must contain exactly one @'
		ELSE ''
	END ||
	CASE
		WHEN INSTR(SUBSTRING(s_adresse_email__025_, INSTR(s_adresse_email__025_, '@') + 1), '.') = 0
		THEN  CHAR(10) || 'S_Adresse.EMail (025) domain must contain at least one dot'
		ELSE ''
	END ||
	CASE
		WHEN NOT s_adresse_email__025_ LIKE_REGEXPR '^[^\s@]+@[^\s@]+\.[^\s@]+$'
		THEN  CHAR(10) || 'S_Adresse.EMail (025) does not match the general email format'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_homepage__026_) > 60
		THEN  CHAR(10) || 'S_Adresse.HomePage (026) exceeds field length (max 60 digits)'
		ELSE ''
	END ||
	CASE
		WHEN (genius_s_adresse_homepage__026_.STATUS IS NOT NULL AND genius_s_adresse_homepage__026_.STATUS = 'check')
		THEN  CHAR(10) || 'genius analysis: ' || genius_s_adresse_homepage__026_.STATUS_MESSAGE || ''
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_handy__027_) > 25
		THEN  CHAR(10) || 'S_Adresse.Handy (027) exceeds field length (max 25 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_adresse_telefon__028_) > 25
		THEN  CHAR(10) || 'S_Adresse.Telefon (028) exceeds field length (max 25 digits)'
		ELSE ''
	END ||
	CASE
		WHEN (INSTR(s_adresse_longitude__033_, '-') > 0 AND LEFT(s_adresse_longitude__033_, 1) != '-')
		THEN  CHAR(10) || 'S_Adresse.Longitude (033) must have minus sign only at the beginning if present (expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) > 0 AND 
                            LENGTH(RIGHT(
                                REPLACE(s_adresse_longitude__033_, '-', ''),
                                LENGTH(REPLACE(s_adresse_longitude__033_, '-', '')) - LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', ''))
                            )) > 10
		THEN  CHAR(10) || 'S_Adresse.Longitude (033) has too many decimal places (maximum 10 allowed; expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_longitude__033_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_longitude__033_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 3
		THEN  CHAR(10) || 'S_Adresse.Longitude (033) has too many digits before the decimal point (maximum 3 allowed; expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN NOT REPLACE(s_adresse_longitude__033_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$')
		THEN  CHAR(10) || 'S_Adresse.Longitude (033) is not a valid number (expected German format, e.g. 1.234,56; format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN (INSTR(s_adresse_latitude__034_, '-') > 0 AND LEFT(s_adresse_latitude__034_, 1) != '-')
		THEN  CHAR(10) || 'S_Adresse.Latitude (034) must have minus sign only at the beginning if present (expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) > 0 AND 
                            LENGTH(RIGHT(
                                REPLACE(s_adresse_latitude__034_, '-', ''),
                                LENGTH(REPLACE(s_adresse_latitude__034_, '-', '')) - LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', ''))
                            )) > 10
		THEN  CHAR(10) || 'S_Adresse.Latitude (034) has too many decimal places (maximum 10 allowed; expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_latitude__034_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_latitude__034_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 3
		THEN  CHAR(10) || 'S_Adresse.Latitude (034) has too many digits before the decimal point (maximum 3 allowed; expected format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN NOT REPLACE(s_adresse_latitude__034_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$')
		THEN  CHAR(10) || 'S_Adresse.Latitude (034) is not a valid number (expected German format, e.g. 1.234,56; format: -zz9.9999999999)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_kunde_suchbegriff__035_) > 12
		THEN  CHAR(10) || 'S_Kunde.Suchbegriff (035) exceeds field length (max 12 digits)'
		ELSE ''
	END ||
	CASE
		WHEN LENGTH(s_kunde_branche__038_) > 6
		THEN  CHAR(10) || 'S_Kunde.Branche (038) exceeds field length (max 6 digits)'
		ELSE ''
	END,CHAR(10)) AS DEFICIENCY_MINING_MESSAGE
        ,CASE 
            WHEN s_kunde_kunde__001_ IS NULL OR s_kunde_kunde__001_ = '' OR INSTR(s_kunde_kunde__001_, '-') > 0 OR INSTR(REPLACE(s_kunde_kunde__001_, '-', ''), ',') > 0 OR LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_kunde_kunde__001_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_kunde_kunde__001_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_kunde_kunde__001_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_kunde_kunde__001_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 8 OR NOT REPLACE(s_kunde_kunde__001_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$') OR s_adresse_name1__003_ IS NULL OR s_adresse_name1__003_ = '' OR LENGTH(s_adresse_name1__003_) > 80 OR s_adresse_staat__004_ IS NULL OR s_adresse_staat__004_ = '' OR LENGTH(s_adresse_staat__004_) > 3 OR s_adresse_ort__005_ IS NULL OR s_adresse_ort__005_ = '' OR LENGTH(s_adresse_ort__005_) > 30 OR LENGTH(s_adresse_suchbegriff__006_) > 12 OR LENGTH(s_adresse_selektion__007_) > 20 OR LENGTH(s_adresse_name2__012_) > 80 OR LENGTH(s_adresse_name3__013_) > 80 OR LENGTH(s_adresse_plz__014_) > 10 OR LENGTH(s_adresse_strasse__018_) > 50 OR LENGTH(s_adresse_hausnummer__020_) > 12 OR LENGTH(s_adresse_plz_postfach__022_) > 10 OR LENGTH(s_adresse_postfach__023_) > 30 OR LENGTH(s_adresse_email__025_) > 254 OR s_adresse_email__025_ LIKE_REGEXPR '[ \t\n\r\[\],;:\\()]' OR s_adresse_email__025_ LIKE_REGEXPR '\.\.' OR s_adresse_email__025_ LIKE '.%' OR s_adresse_email__025_ LIKE '%.' OR LENGTH(s_adresse_email__025_) - LENGTH(REPLACE(s_adresse_email__025_, '@', '')) <> 1 OR INSTR(SUBSTRING(s_adresse_email__025_, INSTR(s_adresse_email__025_, '@') + 1), '.') = 0 OR NOT s_adresse_email__025_ LIKE_REGEXPR '^[^\s@]+@[^\s@]+\.[^\s@]+$' OR LENGTH(s_adresse_homepage__026_) > 60 OR (genius_s_adresse_homepage__026_.STATUS IS NOT NULL AND genius_s_adresse_homepage__026_.STATUS = 'check') OR LENGTH(s_adresse_handy__027_) > 25 OR LENGTH(s_adresse_telefon__028_) > 25 OR (INSTR(s_adresse_longitude__033_, '-') > 0 AND LEFT(s_adresse_longitude__033_, 1) != '-') OR LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) > 0 AND 
                            LENGTH(RIGHT(
                                REPLACE(s_adresse_longitude__033_, '-', ''),
                                LENGTH(REPLACE(s_adresse_longitude__033_, '-', '')) - LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', ''))
                            )) > 10 OR LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_longitude__033_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_adresse_longitude__033_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_longitude__033_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 3 OR NOT REPLACE(s_adresse_longitude__033_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$') OR (INSTR(s_adresse_latitude__034_, '-') > 0 AND LEFT(s_adresse_latitude__034_, 1) != '-') OR LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) > 0 AND 
                            LENGTH(RIGHT(
                                REPLACE(s_adresse_latitude__034_, '-', ''),
                                LENGTH(REPLACE(s_adresse_latitude__034_, '-', '')) - LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', ''))
                            )) > 10 OR LENGTH(
                        CASE 
                            WHEN LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) > 0 
                            THEN LEFT(REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_latitude__034_, '-', ''), ' ', ''), '.', ''), ',', ''), LOCATE(',', REPLACE(s_adresse_latitude__034_, '-', '')) - 1)
                            ELSE REPLACE(REPLACE(REPLACE(REPLACE(s_adresse_latitude__034_, '-', ''), ' ', ''), '.', ''), ',', '')
                        END
                    ) > 3 OR NOT REPLACE(s_adresse_latitude__034_, '-', '') 
                    LIKE_REGEXPR('^[-]?([[:digit:]]+|[[:digit:]]{1,3}(\.[[:digit:]]{3})*)(,[[:digit:]]+)?$') OR LENGTH(s_kunde_suchbegriff__035_) > 12 OR LENGTH(s_kunde_branche__038_) > 6 THEN 'check'
            ELSE 'ok'
        END AS STATUS
    FROM
       MIG."PROJECT_CUSTOMERS"
LEFT JOIN
	MIG.SHARED_NAIGENT genius_s_adresse_homepage__026_
ON  
	    genius_s_adresse_homepage__026_.CLASSIFICATION = 'URL'
	AND genius_s_adresse_homepage__026_.VALUE          = s_adresse_homepage__026_       
)
SELECT
    DEFICIENCY_MINING_MESSAGE,
    s_kunde_kunde__001_ as "S_Kunde.Kunde",
	s_adresse_name1__003_ as "S_Adresse.Name1",
	s_adresse_staat__004_ as "S_Adresse.Staat",
	s_adresse_ort__005_ as "S_Adresse.Ort",
	s_adresse_suchbegriff__006_ as "S_Adresse.Suchbegriff",
	s_adresse_selektion__007_ as "S_Adresse.Selektion",
	s_adresse_name2__012_ as "S_Adresse.Name2",
	s_adresse_name3__013_ as "S_Adresse.Name3",
	s_adresse_plz__014_ as "S_Adresse.PLZ",
	s_adresse_strasse__018_ as "S_Adresse.Strasse",
	s_adresse_hausnummer__020_ as "S_Adresse.Hausnummer",
	s_adresse_plz_postfach__022_ as "S_Adresse.PLZ_Postfach",
	s_adresse_postfach__023_ as "S_Adresse.Postfach",
	s_adresse_email__025_ as "S_Adresse.EMail",
	s_adresse_homepage__026_ as "S_Adresse.HomePage",
	s_adresse_handy__027_ as "S_Adresse.Handy",
	s_adresse_telefon__028_ as "S_Adresse.Telefon",
	s_adresse_longitude__033_ as "S_Adresse.Longitude",
	s_adresse_latitude__034_ as "S_Adresse.Latitude",
	s_kunde_suchbegriff__035_ as "S_Kunde.Suchbegriff",
	s_kunde_branche__038_ as "S_Kunde.Branche"
FROM 
    CTEDefMining
WHERE
    STATUS <> 'ok'
