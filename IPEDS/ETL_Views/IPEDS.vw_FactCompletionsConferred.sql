/*
	View to pivot the completions (conferred) data 
	into a tall table with Gender and Race Codes. 

	Verifies validity of CIP Code, Institution UnitID
    
    (57457558 rows affected) 
	Total execution time: 00:09:31.284
*/

USE OSDS_ETL;
go
DROP VIEW IPEDS.vw_FactCompletionsConferred
go
CREATE VIEW IPEDS.vw_FactCompletionsConferred AS
	SELECT 
		CAST(BASE.SURVEY_YEAR AS INT) [AcademicYr]
		, CAST(BASE.UNITID AS INT) [UnitID]
		, BASE.CountyFIPSCd
		, BASE.StateFIPSCd
		, BASE.CIPCODE [CIPCdFK]
		, CAST(BASE.AWLEVEL AS SMALLINT) [AwardLevelFK]
		, L.LookupCategory1 [GenderCd]
		, L.LookupCategory2 [IPEDSRaceCd]
		, CAST(BASE.MAJORNUM AS SMALLINT) [MajorNbr]
		, CAST(BASE.CDISTEDP AS SMALLINT) [DistanceEdPgm]
		, sum(BASE.COMPLETIONS) [Completions]

	FROM (
				SELECT DISTINCT 
					SURVEY_YEAR
					, UNITID
					, CountyFIPSCd
					, StateFIPSCd
					, CIPCODE
					, AWLEVEL
					, MAJORNUM
					, CDISTEDP
					, CA_CODE
					, cast(COMPLETIONS as bigint) COMPLETIONS
				FROM (
						SELECT DISTINCT 
								CA.SURVEY_YEAR
								, COALESCE(I.StateFIPSCd,'00') StateFIPSCd
								, I.CountyFIPSCd  /*Unknown county AND unknown state */
								, CA.UNITID
								, COALESCE(CIP.CIPCodePK,'98') [CIPCODE]
								, AWLEVEL
								, MAJORNUM
								, CDISTEDP
									, CAIANM --American Indian or Alaska Native men - DETAIL
									, CRACE05 --American Indian or Alaska Native men  - DETAIL
									, DVCAIM --American Indian or Alaska Native men  - DETAIL
									, DVCAIW --American Indian or Alaska Native women  - DETAIL
									, CRACE06 --American Indian or Alaska Native women  - DETAIL
									, CAIANW --American Indian or Alaska Native women - DETAIL
									, CASIAM --Asian men - DETAIL
									, CRACE07 --Asian or Pacific Islander men  - DETAIL
									, CRACE08 --Asian or Pacific Islander women  - DETAIL
									, CASIAW --Asian women - DETAIL
									, DVCAPM --Asian/Native Hawaiian/Other Pacific Islander men  - DETAIL
									, DVCAPW --Asian/Native Hawaiian/Other Pacific Islander women  - DETAIL
									, CRACE03 --Black non-Hispanic men  - DETAIL
									, CRACE04 --Black non-Hispanic women  - DETAIL
									, CBKAAM --Black or African American men - DETAIL
									, CBKAAW --Black or African American women - DETAIL
									, DVCBKM --Black or African American/Black non-Hispanic men  - DETAIL
									, DVCBKW --Black or African American/Black non-Hispanic women  - DETAIL
									, CHISPM --Hispanic men - DETAIL
									, CRACE09 --Hispanic men  - DETAIL
									, DVCHSM --Hispanic or Latino/Hispanic  men  - DETAIL
									, DVCHSW --Hispanic or Latino/Hispanic  women  - DETAIL
									, CRACE10 --Hispanic women  - DETAIL
									, CHISPW --Hispanic women - DETAIL
									, CNHPIM --Native Hawaiian or Other Pacific Islander men - DETAIL
									, CNHPIW --Native Hawaiian or Other Pacific Islander women - DETAIL
									, CNRALM --Nonresident alien men - DETAIL
									, CRACE01 --Nonresident alien men  - DETAIL
									, CRACE02 --Nonresident alien women - DETAIL
									, CNRALW --Nonresident alien women - DETAIL
									, CRACE13 --Race/ethnicity unknown men - DETAIL
									, CUNKNM --Race/ethnicity unknown men - DETAIL
									, CUNKNW --Race/ethnicity unknown women - DETAIL
									, CRACE14 --Race/ethnicity unknown women - DETAIL
									, C2MORM --Two or more races men - DETAIL
									, C2MORW --Two or more races women - DETAIL
									, CWHITM --White men - DETAIL
									, CRACE11 --White non-Hispanic men  - DETAIL
									, CRACE12 --White non-Hispanic women  - DETAIL
									, CWHITW --White women - DETAIL
									, DVCWHM --White/White non-Hispanic men  - DETAIL
									, DVCWHW --White/White non-Hispanic women  - DETAIL
							/****** EXCLUDE TOTALS AND Grand Totals */
									--, DVCAIT --American Indian or Alaska Native total  - TOTAL
									--, CRACE19 --American Indian or Alaska Native total - TOTAL
									--, CAIANT --American Indian or Alaska Native total - TOTAL
									--, CRACE20 --Asian or Pacific Islander total   - TOTAL
									--, CASIAT --Asian total - TOTAL
									--, DVCAPT --Asian/Native Hawaiian/Other Pacific Islander total  - TOTAL
									--, CRACE18 --Black non-Hispanic  total  - TOTAL
									--, CBKAAT --Black or African American total - TOTAL
									--, DVCBKT --Black or African American/Black non-Hispanic total  - TOTAL
									--, DVCHST --Hispanic or Latino/Hispanic  total  - TOTAL
									--, CRACE21 --Hispanic total  - TOTAL
									--, CHISPT --Hispanic total - TOTAL
									--, CNHPIT --Native Hawaiian or Other Pacific Islander total - TOTAL
									--, CNRALT --Nonresident alien total - TOTAL
									--, CRACE17 --Nonresident alien total - TOTAL
									--, CUNKNT --Race/ethnicity unknown total - TOTAL
									--, CRACE23 --Race/ethnicity unknown total  - TOTAL
									--, C2MORT --Two or more races total - TOTAL
									--, CRACE22 --White non-Hispanic total  - TOTAL
									--, CWHITT --White total - TOTAL
									--, DVCWHT --White/White non-Hispanic total  - TOTAL
									
									--, CRACE15 --Total men  - TOTAL
									--, CRACE16 --Total women  - TOTAL
									--, CRACE24 --Grand total  - TOTAL
									--, CTOTALT --Grand total - TOTAL
									--, CTOTALM --Grand total men - TOTAL
									--, CTOTALW --Grand total women - TOTAL
						FROM IPEDS.tblCA CA
							LEFT JOIN IPEDS.vw_DimCIPCodes CIP ON CA.CIPCODE = CIP.CIPCodePK
							INNER JOIN IPEDS.vw_DimInstitution I ON CA.UNITID = I.UnitID
							
						WHERE CA.CIPCODE <> '99'
--AND CA.UNITID = 214777 -- TEST
						
					) CA_BASE
					UNPIVOT ( 
								COMPLETIONS 
								FOR CA_CODE IN 
								(
									  CAIANM --American Indian or Alaska Native men - DETAIL
									, CRACE05 --American Indian or Alaska Native men  - DETAIL
									, DVCAIM --American Indian or Alaska Native men  - DETAIL
									, DVCAIW --American Indian or Alaska Native women  - DETAIL
									, CRACE06 --American Indian or Alaska Native women  - DETAIL
									, CAIANW --American Indian or Alaska Native women - DETAIL
									, CASIAM --Asian men - DETAIL
									, CRACE07 --Asian or Pacific Islander men  - DETAIL
									, CRACE08 --Asian or Pacific Islander women  - DETAIL
									, CASIAW --Asian women - DETAIL
									, DVCAPM --Asian/Native Hawaiian/Other Pacific Islander men  - DETAIL
									, DVCAPW --Asian/Native Hawaiian/Other Pacific Islander women  - DETAIL
									, CRACE03 --Black non-Hispanic men  - DETAIL
									, CRACE04 --Black non-Hispanic women  - DETAIL
									, CBKAAM --Black or African American men - DETAIL
									, CBKAAW --Black or African American women - DETAIL
									, DVCBKM --Black or African American/Black non-Hispanic men  - DETAIL
									, DVCBKW --Black or African American/Black non-Hispanic women  - DETAIL
									, CHISPM --Hispanic men - DETAIL
									, CRACE09 --Hispanic men  - DETAIL
									, DVCHSM --Hispanic or Latino/Hispanic  men  - DETAIL
									, DVCHSW --Hispanic or Latino/Hispanic  women  - DETAIL
									, CRACE10 --Hispanic women  - DETAIL
									, CHISPW --Hispanic women - DETAIL
									, CNHPIM --Native Hawaiian or Other Pacific Islander men - DETAIL
									, CNHPIW --Native Hawaiian or Other Pacific Islander women - DETAIL
									, CNRALM --Nonresident alien men - DETAIL
									, CRACE01 --Nonresident alien men  - DETAIL
									, CRACE02 --Nonresident alien women - DETAIL
									, CNRALW --Nonresident alien women - DETAIL
									, CRACE13 --Race/ethnicity unknown men - DETAIL
									, CUNKNM --Race/ethnicity unknown men - DETAIL
									, CUNKNW --Race/ethnicity unknown women - DETAIL
									, CRACE14 --Race/ethnicity unknown women - DETAIL
									, C2MORM --Two or more races men - DETAIL
									, C2MORW --Two or more races women - DETAIL
									, CWHITM --White men - DETAIL
									, CRACE11 --White non-Hispanic men  - DETAIL
									, CRACE12 --White non-Hispanic women  - DETAIL
									, CWHITW --White women - DETAIL
									, DVCWHM --White/White non-Hispanic men  - DETAIL
									, DVCWHW --White/White non-Hispanic women  - DETAIL
						/****** EXCLUDE TOTALS */
									--, DVCAIT --American Indian or Alaska Native total  - TOTAL
									--, CRACE19 --American Indian or Alaska Native total - TOTAL
									--, CAIANT --American Indian or Alaska Native total - TOTAL
									--, CRACE20 --Asian or Pacific Islander total   - TOTAL
									--, CASIAT --Asian total - TOTAL
									--, DVCAPT --Asian/Native Hawaiian/Other Pacific Islander total  - TOTAL
									--, CRACE18 --Black non-Hispanic  total  - TOTAL
									--, CBKAAT --Black or African American total - TOTAL
									--, DVCBKT --Black or African American/Black non-Hispanic total  - TOTAL
									--, CRACE24 --Grand total  - TOTAL
									--, CTOTALT --Grand total - TOTAL
									--, CTOTALM --Grand total men - TOTAL
									--, CTOTALW --Grand total women - TOTAL
									--, DVCHST --Hispanic or Latino/Hispanic  total  - TOTAL
									--, CRACE21 --Hispanic total  - TOTAL
									--, CHISPT --Hispanic total - TOTAL
									--, CNHPIT --Native Hawaiian or Other Pacific Islander total - TOTAL
									--, CNRALT --Nonresident alien total - TOTAL
									--, CRACE17 --Nonresident alien total - TOTAL
									--, CUNKNT --Race/ethnicity unknown total - TOTAL
									--, CRACE23 --Race/ethnicity unknown total  - TOTAL
									--, CRACE15 --Total men  - TOTAL
									--, CRACE16 --Total women  - TOTAL
									--, C2MORT --Two or more races total - TOTAL
									--, CRACE22 --White non-Hispanic total  - TOTAL
									--, CWHITT --White total - TOTAL
									--, DVCWHT --White/White non-Hispanic total  - TOTAL
								)
						) CA_UNPIVOT

			) BASE
		INNER JOIN (	SELECT L.LookupCd, L.LookupCategory1, L.LookupCategory2
						FROM SHARED.tblLookupImport L 
						WHERE UPPER(L.LookupName) = 'CA_CODES'
							AND UPPER(L.LookupCategory1) <> 'T'
							AND UPPER(L.LookupCategory2) <> 'T'
			) L ON BASE.CA_CODE = L.LookupCd
		
WHERE 
/*
	This logic ensures no duplication between OLD race/gender codes, Derived race/gender codes and New codes 
	- it forces the use of old/derived codes through 2010, and only introduces new codes from 2011 onward when they were required
*/
	(SURVEY_YEAR <= 2007
		AND UPPER(BASE.CA_CODE) LIKE 'CRACE%'
	)
	OR
	(SURVEY_YEAR BETWEEN 2008 AND 2010
		AND (
				UPPER(BASE.CA_CODE) LIKE 'DV%'
				OR UPPER(BASE.CA_CODE) IN ('C2MORM','C2MORW','CUNKNM','CUNKNW') /*Non-derived totals during these years*/
			)
	)
	OR
	(SURVEY_YEAR >= 2011
		AND UPPER(BASE.CA_CODE) LIKE 'C%' 
		AND UPPER(BASE.CA_CODE) NOT LIKE 'CRACE%'
	)
group by 
		CAST(BASE.SURVEY_YEAR AS INT) 
		, CAST(BASE.UNITID AS INT) 
		, CAST(BASE.CountyFIPSCd AS CHAR(5)) 
		, BASE.StateFIPSCd
		, BASE.CIPCODE 
		, CAST(BASE.AWLEVEL AS SMALLINT) 
		, L.LookupCategory1 
		, L.LookupCategory2 
		, CAST(BASE.MAJORNUM AS SMALLINT) 
		, CAST(BASE.CDISTEDP AS SMALLINT)

--ORDER BY AcademicYr, AwardLevelFK, GenderCd, IPEDSRaceCd, MajorNbr, DistanceEdPgm
GO


/*move data*/
	DROP TABLE OSDS_RPT.IPEDS.tblFactCompletionsConferred
	SELECT * INTO OSDS_RPT.IPEDS.tblFactCompletionsConferred FROM OSDS_ETL.IPEDS.vw_FactCompletionsConferred

/*create clustered colstore on physical table*/
	CREATE CLUSTERED COLUMNSTORE INDEX IX_CompletionsClusteredColStore on OSDS_RPT.IPEDS.tblFactCompletionsConferred
