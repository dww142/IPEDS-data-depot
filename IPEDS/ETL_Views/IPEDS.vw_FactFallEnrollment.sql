/*
	View to pivot the Fall Enrollment data 
	into a tall table with Enrollment Level,
	Attendnace Status, Gender, and Race Codes. 

	Excludes codes that represents totals and grand totals
	- totals should be calculated by summarizing the enrollment
		for each detail code

last run: 
	(8482228 rows affected) 
	Total execution time: 00:08:55.282
*/
USE OSDS_ETL;
GO
 DROP  VIEW IPEDS.vw_FactFallEnrollment;
go
CREATE VIEW IPEDS.vw_FactFallEnrollment AS
SELECT 
	CAST(BASE.SURVEY_YEAR AS INT) +1 [AcademicYr]
	, I.StateFIPSCd
	, I.[CountyFIPSCd]
	, CAST(BASE.UNITID AS INT) [UnitID]
	, CAST(BASE.EFALEVEL AS SMALLINT) [FallEnrollDetailFK]
	, CAST(BASE.SECTION AS SMALLINT) [FallAttendanceStatusFK]
	, L.LookupCategory1 [GenderCd]
	, L.LookupCategory2 [IPEDSRaceCd]
	, R.RaceCd

	--, BASE.EFA_CODE
	--, BASE.EFALEVEL

	, sum(ENROLLMENT) [EnrollmentTotal]
FROM (
		SELECT 
			SURVEY_YEAR
			, UNITID
			, EFALEVEL
			, SECTION
			, EFA_CODE
			, CAST(ENROLLMENT AS BIGINT) ENROLLMENT
		FROM 
			(
				SELECT  
					EFA.SURVEY_YEAR
					, EFA.UNITID
					, EFALEVEL
					, SECTION
	
					, DVEFAIM --Detail - American Indian or Alaska Native men 
					, EFAIANM --Detail - American Indian or Alaska Native men
					, EFRACE05 --Detail - American Indian or Alaska Native men 
					, DVEFAIW --Detail - American Indian or Alaska Native women 
					, EFAIANW --Detail - American Indian or Alaska Native women
					, EFRACE06 --Detail - American Indian or Alaska Native women 
					, EFASIAM --Detail - Asian men
					, EFRACE07 --Detail - Asian or Pacific Islander men 
					, EFRACE08 --Detail - Asian or Pacific Islander women 
					, EFASIAW --Detail - Asian women
					, DVEFAPM --Detail - Asian/Native Hawaiian/Other Pacific Islander men 
					, DVEFAPW --Detail - Asian/Native Hawaiian/Other Pacific Islander women 
					, EFRACE03 --Detail - Black non-Hispanic men 
					, EFRACE04 --Detail - Black non-Hispanic women 
					, EFBKAAM --Detail - Black or African American men
					, EFBKAAW --Detail - Black or African American women
					, DVEFBKM --Detail - Black or African American/Black non-Hispanic men 
					, DVEFBKW --Detail - Black or African American/Black non-Hispanic women 
					, EFHISPM --Detail - Hispanic men
					, EFRACE09 --Detail - Hispanic men 
					, DVEFHSM --Detail - Hispanic or Latino/Hispanic  men 
					, DVEFHSW --Detail - Hispanic or Latino/Hispanic  women 
					, EFHISPW --Detail - Hispanic women
					, EFRACE10 --Detail - Hispanic women 
					, EFNHPIM --Detail - Native Hawaiian or Other Pacific Islander men
					, EFNHPIW --Detail - Native Hawaiian or Other Pacific Islander women
					, EFNRALM --Detail - Nonresident alien men
					, EFRACE01 --Detail - Nonresident alien men 
					, EFNRALW --Detail - Nonresident alien women
					, EFRACE02 --Detail - Nonresident alien women
					, EFRACE13 --Detail - Race/ethnicity unknown men
					, EFUNKNM --Detail - Race/ethnicity unknown men
					, EFRACE14 --Detail - Race/ethnicity unknown women
					, EFUNKNW --Detail - Race/ethnicity unknown women
					, EF2MORM --Detail - Two or more races men
					, EF2MORW --Detail - Two or more races women
					, EFWHITM --Detail - White men
					, EFRACE11 --Detail - White non-Hispanic men 
					, EFRACE12 --Detail - White non-Hispanic women 
					, EFWHITW --Detail - White women
					, DVEFWHM --Detail - White/White non-Hispanic men 
					, DVEFWHW --Detail - White/White non-Hispanic women 
					
					--, DVEFAIT --Total - American Indian or Alaska Native total 
					--, EFAIANT --Total - American Indian or Alaska Native total
					--, EFRACE19 --Total - American Indian or Alaska Native total
					--, EFRACE20 --Total - Asian or Pacific Islander total  
					--, EFASIAT --Total - Asian total
					--, DVEFAPT --Total - Asian/Native Hawaiian/Other Pacific Islander total 
					--, EFRACE18 --Total - Black non-Hispanic  total 
					--, EFBKAAT --Total - Black or African American total
					--, DVEFBKT --Total - Black or African American/Black non-Hispanic total 
					--, EFRACE24 --Total - Grand total 
					--, EFTOTLT --Total - Grand total
					--, EFTOTLM --Total - Grand total men
					--, EFTOTLW --Total - Grand total women
					--, DVEFHST --Total - Hispanic or Latino/Hispanic  total 
					--, EFHISPT --Total - Hispanic total
					--, EFRACE21 --Total - Hispanic total 
					--, EFNHPIT --Total - Native Hawaiian or Other Pacific Islander total
					--, EFNRALT --Total - Nonresident alien total
					--, EFRACE17 --Total - Nonresident alien total
					--, EFRACE23 --Total - Race/ethnicity unknown total 
					--, EFUNKNT --Total - Race/ethnicity unknown total
					--, EFRACE15 --Total - Total men 
					--, EFRACE16 --Total - Total women 
					--, EF2MORT --Total - Two or more races total
					--, EFRACE22 --Total - White non-Hispanic total 
					--, EFWHITT --Total - White total
					--, DVEFWHT --Total - White/White non-Hispanic total 

				FROM IPEDS.tblEFA EFA
				WHERE  --EFA.UNITID = 214777 and -----------------testing
					(
						CAST(RTRIM(LTRIM(EFA.EFALEVEL)) AS INT) IN (36, 32, 24, 31, 40, 39, 56, 52, 44, 51, 60, 59)
						OR (
								cast(EFA.SURVEY_YEAR as int) < 2006 /*academic year 2007*/
								AND CAST(RTRIM(LTRIM(EFA.EFALEVEL)) AS INT) IN (25, 45)

								/*	25 = SUM OF 39 AND 40 PRIOR TO 2007; 
									45 = SUM OF 59 AND 60 PRIOR TO 2007*/
							) 
					)
			/*Exclude generated totals....*/
				--CAST(EFA.EFALEVEL AS INT) NOT IN (1,2,3,4,5,11,12,19,20) --ALL Students totals
				--	AND CAST(EFA.EFALEVEL AS INT) NOT IN (21, 22, 41, 42) -- ft/pt totals
				--	AND CAST(EFA.EFALEVEL AS INT) NOT IN (23, 43) -- FT/PT UG DS TOTAL 
				--  AND CAST(EFA.EFALEVEL AS INT) NOT IN (25, 45) -- OTHer degree seek total (combination of transfer in + continuing) 


			)EFA_BASE
			UNPIVOT (
					ENROLLMENT
					FOR EFA_CODE IN 
					(
							DVEFAIM --Detail - American Indian or Alaska Native men 
							, EFAIANM --Detail - American Indian or Alaska Native men
							, EFRACE05 --Detail - American Indian or Alaska Native men 
							, DVEFAIW --Detail - American Indian or Alaska Native women 
							, EFAIANW --Detail - American Indian or Alaska Native women
							, EFRACE06 --Detail - American Indian or Alaska Native women 
							, EFASIAM --Detail - Asian men
							, EFRACE07 --Detail - Asian or Pacific Islander men 
							, EFRACE08 --Detail - Asian or Pacific Islander women 
							, EFASIAW --Detail - Asian women
							, DVEFAPM --Detail - Asian/Native Hawaiian/Other Pacific Islander men 
							, DVEFAPW --Detail - Asian/Native Hawaiian/Other Pacific Islander women 
							, EFRACE03 --Detail - Black non-Hispanic men 
							, EFRACE04 --Detail - Black non-Hispanic women 
							, EFBKAAM --Detail - Black or African American men
							, EFBKAAW --Detail - Black or African American women
							, DVEFBKM --Detail - Black or African American/Black non-Hispanic men 
							, DVEFBKW --Detail - Black or African American/Black non-Hispanic women 
							, EFHISPM --Detail - Hispanic men
							, EFRACE09 --Detail - Hispanic men 
							, DVEFHSM --Detail - Hispanic or Latino/Hispanic  men 
							, DVEFHSW --Detail - Hispanic or Latino/Hispanic  women 
							, EFHISPW --Detail - Hispanic women
							, EFRACE10 --Detail - Hispanic women 
							, EFNHPIM --Detail - Native Hawaiian or Other Pacific Islander men
							, EFNHPIW --Detail - Native Hawaiian or Other Pacific Islander women
							, EFNRALM --Detail - Nonresident alien men
							, EFRACE01 --Detail - Nonresident alien men 
							, EFNRALW --Detail - Nonresident alien women
							, EFRACE02 --Detail - Nonresident alien women
							, EFRACE13 --Detail - Race/ethnicity unknown men
							, EFUNKNM --Detail - Race/ethnicity unknown men
							, EFRACE14 --Detail - Race/ethnicity unknown women
							, EFUNKNW --Detail - Race/ethnicity unknown women
							, EF2MORM --Detail - Two or more races men
							, EF2MORW --Detail - Two or more races women
							, EFWHITM --Detail - White men
							, EFRACE11 --Detail - White non-Hispanic men 
							, EFRACE12 --Detail - White non-Hispanic women 
							, EFWHITW --Detail - White women
							, DVEFWHM --Detail - White/White non-Hispanic men 
							, DVEFWHW --Detail - White/White non-Hispanic women 
					
					) 
				) EFA_UNPIVOT
	) BASE
	INNER JOIN IPEDS.vw_DimInstitution I ON BASE.UNITID = I.UnitID
	INNER JOIN SHARED.tblLookupImport L ON LTRIM(RTRIM(BASE.EFA_CODE)) = L.LookupCd
										AND UPPER(L.LookupName) = 'EFA_CODES'
										AND UPPER(L.LookupCategory1) <> 'T'
										AND UPPER(L.LookupCategory2) <> 'T'

	LEFT JOIN SHARED.vw_DimRaceEthnicityCode R ON L.LookupCategory2 = R.IPEDSRaceCd /*Join IPEDS Race code to race dim to get race FK value*/
WHERE 
	(	BASE.SURVEY_YEAR <= 2007
		AND UPPER(BASE.EFA_CODE) LIKE 'EFRACE%'
	)
	OR
	(	BASE.SURVEY_YEAR BETWEEN 2008 AND 2009
		AND (UPPER(BASE.EFA_CODE) LIKE 'DV%' 
				OR UPPER(BASE.EFA_CODE) IN ('EFUNKNM','EFUNKNW') /*Unk Race and 2 or more not included in derived variables*/
				OR UPPER(BASE.EFA_CODE) IN ('EF2MORM','EF2MORW') /*Unk Race and 2 or more not included in derived variables*/
			)
	)
	OR
	(	BASE.SURVEY_YEAR >= 2010
		AND UPPER(BASE.EFA_CODE) LIKE 'EF%' 
		AND UPPER(BASE.EFA_CODE) NOT LIKE 'EFRACE%'
	)

GROUP BY 
	BASE.SURVEY_YEAR
	, I.StateFIPSCd
	, I.CountyFIPSCd
	, BASE.UNITID
	, BASE.EFALEVEL 
	, BASE.SECTION 
	, L.LookupCategory1 
	, L.LookupCategory2
	, R.RaceCd
	--	, BASE.EFA_CODE
	--, BASE.EFALEVEL

	GO

/*
	Move view data into physical table
*/
	DROP TABLE OSDS_RPT.IPEDS.tblFactFallEnrollment
	SELECT * INTO OSDS_RPT.IPEDS.tblFactFallEnrollment FROM OSDS_ETL.IPEDS.vw_FactFallEnrollment
	CREATE CLUSTERED COLUMNSTORE INDEX IX_IPEDSFallEnrollment on OSDS_RPT.IPEDS.tblFactFallEnrollment
