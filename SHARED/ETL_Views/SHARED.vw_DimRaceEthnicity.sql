/***************************************************
	Builds a view of Race/Ethnicity
	Codes/Descriptions from the common Lookup table. 

	Specifies how PIMS race codes map to other values, e.g., IPEDS
		Add other mappings as necessary

	TODO: Add mappings to NCES / EdFacts race code values
***************************************************/

DROP VIEW IF EXISTS SHARED.vw_DimRaceEthnicityCode
GO

CREATE VIEW SHARED.vw_DimRaceEthnicityCode AS
	SELECT DISTINCT 
			L.LookupCd [RaceCd]
			, L.LookupDesc [RaceDesc]
			, CASE L.LookupCd
					WHEN 1 THEN 'AIAN'
					WHEN 2 THEN 'API'
					WHEN 3 THEN 'BKAFAM'
					WHEN 4 THEN 'HISP'
					WHEN 5 THEN 'WH'
					WHEN 6 THEN 'MR'
					WHEN 7 THEN 'WH'
					WHEN 9 THEN 'ASN'
					WHEN 10 THEN 'NHOPI'
					ELSE 'NA'
				END [IPEDSRaceCd]
	FROM [SHARED].[tblLookupImport] L
		INNER JOIN (
						SELECT MAX(LL.[SourceYear]) LATEST_LKP_SY, LL.LookupCd 
						FROM [SHARED].[tblLookupImport] LL 
						WHERE UPPER(LL.LookupName) =  'RACE OR ETHNICITY' 
							--AND LL.LookupCd IN ('YES','NO')
						GROUP BY LL.LookupCd
					) LATEST ON L.[SourceYear] = LATEST.LATEST_LKP_SY AND LATEST.LookupCd = L.LookupCd
	WHERE UPPER(L.LookupName) =  'RACE OR ETHNICITY'

	/*ADD NA values and IPEDS Values to view manually */
	UNION SELECT 'NA','NA','NA'
	UNION SELECT 'UNK','Race or Ethnicity Unknown','UNK'
	UNION SELECT 'NRAL','Non-Resident Alien','NRAL'
GO

 /*
    ETL into the RPT Database
 */
    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimRaceEthnicityCode
    SELECT * INTO OSDS_RPT.SHARED.tblDimRaceEthnicityCode FROM OSDS_ETL.SHARED.vw_DimRaceEthnicityCode
    CREATE CLUSTERED COLUMNSTORE INDEX IX_RaceEthnicity_Colstore ON OSDS_RPT.SHARED.tblDimRaceEthnicityCode
