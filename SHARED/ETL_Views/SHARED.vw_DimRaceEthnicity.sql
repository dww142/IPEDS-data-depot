/***************************************************
	Builds a view of Race/Ethnicity
	Codes/Descriptions from the common Lookup table. 

	Specifies how PIMS race codes map to other values, e.g., IPEDS
		Add other mappings as necessary

	TODO: Add mappings to NCES / EdFacts race code values

map: 
CENSUS_RACE_CODES = {
    'A' : 'WHITE ALONE',
    'B' : 'BLACK OR AFRICAN AMERICAN ALONE',
    'C' : 'AMERICAN INDIAN AND ALASKA NATIVE ALONE',
    'D' : 'ASIAN ALONE',
    'E' : 'NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER ALONE',
    'F' : 'SOME OTHER RACE ALONE',
    'G' : 'TWO OR MORE RACES',
    'H' : 'WHITE ALONE, NOT HISPANIC OR LATINO',
    'I' : 'HISPANIC OR LATINO',


****** iMPORTANT NOTE: 
	- Each set of codes should be unique; i.e., multiple IPEDS codes should not map to the same RaceCd, 
	multiple Census Codes should not map to the same IPEDS code, etc. 
	The DESCRIPTIVE fields can be the same, but the join fields should be different or risk duplication in joining
***************************************************/
USE OSDS_ETL;

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
					WHEN 9 THEN 'ASN'
					WHEN 10 THEN 'NHOPI'
					ELSE NULL
				END [IPEDSRaceCd]

			, CASE L.LookupCd
					WHEN 1 THEN 'C'
					WHEN 2 THEN 'F'
					WHEN 3 THEN 'B'
					WHEN 4 THEN 'I'
					WHEN 5 THEN 'H'
					WHEN 6 THEN 'G'
					WHEN 9 THEN 'D'
					WHEN 10 THEN 'E'
					ELSE NULL
				END [CensusRaceCd]
	FROM [SHARED].[tblLookupImport] L
		INNER JOIN (
						SELECT MAX(LL.[SourceYear]) LATEST_LKP_SY, LL.LookupCd 
						FROM [SHARED].[tblLookupImport] LL 
						WHERE UPPER(LL.LookupName) =  'RACE OR ETHNICITY' 
							--AND LL.LookupCd IN ('YES','NO')
						GROUP BY LL.LookupCd
					) LATEST ON L.[SourceYear] = LATEST.LATEST_LKP_SY AND LATEST.LookupCd = L.LookupCd
	WHERE UPPER(L.LookupName) =  'RACE OR ETHNICITY'

	/*ADD NA values and IPEDS Values to view manually 
        RaceCd, RaceDesc, IPEDSRaceCd, CensusRaceCd
    */
	UNION SELECT 'NA','NA','NA','NA'
	UNION SELECT 'UNK','Race or Ethnicity Unknown','UNK','UNK'
	UNION SELECT 'NRAL','Non-Resident Alien','NRAL','NA_C' /*IPEDS Only Code*/
    UNION SELECT 'WH_ONLY','White Alone (Includes Hispanic)','WH_ONLY','A' /*Census Only Code*/

--ORDER BY IPEDSRaceCd	
GO

 /*
    ETL into the RPT Database
 */
    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimRaceEthnicityCode
    SELECT * INTO OSDS_RPT.SHARED.tblDimRaceEthnicityCode FROM OSDS_ETL.SHARED.vw_DimRaceEthnicityCode
    CREATE CLUSTERED COLUMNSTORE INDEX IX_RaceEthnicity_Colstore ON OSDS_RPT.SHARED.tblDimRaceEthnicityCode



