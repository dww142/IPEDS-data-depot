
/*
**********************
This needs cleaned up; 
field names, 
identifying totals and subtotals better; 
document relationships between chrtstat values

	CHRTSTAT Value Relationships
	- 22 (100% normal time) = sum of 23 (less than 2 year pgms) and 24 (2 yr pgms)  -- in 2 year institutions
	- 16 (150% normal time) = sum of 17 (4yr) + 18 (5yr) + 19 (6yr) --in 4 year institutitons


RACIAL BREAKDown
- only not available for the 2 year institution 100% completion
-- available for 4 year, 100%-150% and 2 year 150% data

WHEN 2 YR 100% COMPLETION VALUES; GET TOTALS; ELSE GET RACIAL/GENDER SUBTOTALS
	COHORT = 4 AND CHRTSTAT IN (22, 23, 24) THEN 'T'
	ELSE NOT 'T'

LAST RUN: 
	(1718603 rows affected) 
	Total execution time: 00:04:41.763
*/

USE OSDS_ETL;
GO

DROP VIEW IF EXISTS IPEDS.vw_FactGraduationRate
GO
CREATE VIEW IPEDS.vw_FactGraduationRate AS

SELECT 
	GR_UNPIVOT.SURVEY_YEAR AcademicYr
	, GR_UNPIVOT.UNITID UnitID
	, GR_UNPIVOT.StateFIPSCd
	, GR_UNPIVOT.CountyFIPSCd
	, GR_UNPIVOT.COHORT [CohortCd]
	, GR_UNPIVOT.[COHORT DESC] [CohortDesc]
	--, GR_UNPIVOT.CHRTSTAT [CohortStatusCd]
	--, GR_UNPIVOT.[CHRTSTAT DESC] [CohortStatusDesc]
	, CAST(REPLACE(L.LookupCategory1,'T','NA') AS VARCHAR(20)) [GenderCd]
	, CAST(REPLACE(L.LookupCategory2,'T','NA') AS VARCHAR(20)) [IPEDSRaceCd]
	--, L.LookupDesc

     , SUM(CASE CHRTSTAT WHEN '10' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Revised Cohort]
     , SUM(CASE CHRTSTAT WHEN '12' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Adjusted Cohort (Revised Cohort Minus Exclusions)]

     , SUM(CASE CHRTSTAT WHEN '13' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Within 150% Of Normal Time]
     , SUM(CASE CHRTSTAT WHEN '14' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Programs Of Less Than 2 Years (150% Of Normal Time)]
     , SUM(CASE CHRTSTAT WHEN '15' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Programs Of 2 But Less Than 4 Years (150% Of Normal Time)]

     , SUM(CASE CHRTSTAT WHEN '22' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Programs Within 100% Of Normal Time Total]
     , SUM(CASE CHRTSTAT WHEN '23' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Programs Of < 2 Yrs Within 100% Of Normal Time (not Available By Race Or Gender)]
     , SUM(CASE CHRTSTAT WHEN '24' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Programs Of 2 But < 4 Yrs Within 100% Of Normal Time (not Available By Race Or Gender)]
     
	 /*16 = SUM OF 17,18,19*/
	 , SUM(CASE CHRTSTAT WHEN '16' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Bachelor's Or Equivalent Degrees (150% Of Normal Time)]
		 , SUM(CASE CHRTSTAT WHEN '17' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Bachelor's Or Equivalent Degrees In 4 Years Or Less]
		 , SUM(CASE CHRTSTAT WHEN '18' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Bachelor's Or Equivalent Degrees In 5 Years]
		 , SUM(CASE CHRTSTAT WHEN '19' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Completers Of Bachelor's Or Equivalent Degrees In 6 Years]

     , SUM(CASE CHRTSTAT WHEN '20' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Transfer-Out Students]
     , SUM(CASE CHRTSTAT WHEN '31' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Noncompleters, Still Enrolled]
     , SUM(CASE CHRTSTAT WHEN '32' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Noncompleters, No Longer Enrolled]
     , SUM(CASE CHRTSTAT WHEN '11' THEN CAST(GRTOTAL AS BIGINT) ELSE NULL END) [Exclusions]


	--, CAST(GR_UNPIVOT.GRTOTAL AS BIGINT) GRTOTAL
FROM 

	(/*GR data pull*/
		SELECT 
			cast(GR.SURVEY_YEAR as int) SURVEY_YEAR
			, CAST(GR.UNITID AS INT) UNITID
			, I.StateFIPSCd
			, I.CountyFIPSCd
			, CAST(GR.COHORT AS VARCHAR(20)) COHORT
			, CAST(COHORT.LookupDesc AS VARCHAR(100)) [COHORT DESC]	
			, CAST(GR.CHRTSTAT AS VARCHAR(20)) CHRTSTAT
			, CAST(CHRTSTAT.LookupDesc AS VARCHAR(20)) [CHRTSTAT DESC]
			, GR.DVGRAIM --Detail - American Indian or Alaska Native men 
			, GR.GRAIANM --Detail - American Indian or Alaska Native men
			, GR.GRRACE05 --Detail - American Indian or Alaska Native men 
			, GR.DVGRAIW --Detail - American Indian or Alaska Native women 
			, GR.GRAIANW --Detail - American Indian or Alaska Native women
			, GR.GRRACE06 --Detail - American Indian or Alaska Native women 
			, GR.GRASIAM --Detail - Asian men
			, GR.GRRACE07 --Detail - Asian or Pacific Islander men 
			, GR.GRRACE08 --Detail - Asian or Pacific Islander women 
			, GR.GRASIAW --Detail - Asian women
			, GR.DVGRAPM --Detail - Asian/Native Hawaiian/Other Pacific Islander men 
			, GR.DVGRAPW --Detail - Asian/Native Hawaiian/Other Pacific Islander women 
			, GR.GRRACE03 --Detail - Black non-Hispanic men 
			, GR.GRRACE04 --Detail - Black non-Hispanic women 
			, GR.GRBKAAM --Detail - Black or African American men
			, GR.GRBKAAW --Detail - Black or African American women
			, GR.DVGRBKM --Detail - Black or African American/Black non-Hispanic men 
			, GR.DVGRBKW --Detail - Black or African American/Black non-Hispanic women 
			, GR.GRHISPM --Detail - Hispanic men
			, GR.GRRACE09 --Detail - Hispanic men 
			, GR.DVGRHSM --Detail - Hispanic or Latino/Hispanic  men 
			, GR.DVGRHSW --Detail - Hispanic or Latino/Hispanic  women 
			, GR.GRHISPW --Detail - Hispanic women
			, GR.GRRACE10 --Detail - Hispanic women 
			, GR.GRNHPIM --Detail - Native Hawaiian or Other Pacific Islander men
			, GR.GRNHPIW --Detail - Native Hawaiian or Other Pacific Islander women
			, GR.GRNRALM --Detail - Nonresident alien men
			, GR.GRRACE01 --Detail - Nonresident alien men 
			, GR.GRNRALW --Detail - Nonresident alien women
			, GR.GRRACE02 --Detail - Nonresident alien women
			, GR.GRRACE13 --Detail - Race/ethnicity unknown men
			, GR.GRUNKNM --Detail - Race/ethnicity unknown men
			, GR.GRRACE14 --Detail - Race/ethnicity unknown women
			, GR.GRUNKNW --Detail - Race/ethnicity unknown women
			, GR.GR2MORM --Detail - Two or more races men
			, GR.GR2MORW --Detail - Two or more races women
			, GR.GRWHITM --Detail - White men
			, GR.GRRACE11 --Detail - White non-Hispanic men 
			, GR.GRRACE12 --Detail - White non-Hispanic women 
			, GR.GRWHITW --Detail - White women
			, GR.DVGRWHM --Detail - White/White non-Hispanic men 
			, GR.DVGRWHW --Detail - White/White non-Hispanic women 
		/*Not all Grad Rate data is availalbe by race & gender; need totals for those data points*/
			, GRRACE24 --Total - Grand total 
			, GRTOTLT --Total - Grand total
			, GRTOTLM --Total - Grand total men
			, GRTOTLW --Total - Grand total women
			, GRRACE15 --Total - Total men 
			, GRRACE16 --Total - Total women 
		FROM IPEDS.tblGR150 GR
			INNER JOIN IPEDS.vw_DimInstitution I ON GR.UNITID = I.UnitID
			LEFT JOIN SHARED.vw_UniqueLookupList COHORT ON COALESCE(GR.COHORT,'-2') = COHORT.LookupCd AND UPPER(COHORT.LookupName)='COHORT' AND COHORT.Source = 'IPEDS_GR150'
			LEFT JOIN SHARED.vw_UniqueLookupList CHRTSTAT ON COALESCE(GR.CHRTSTAT,'-2') = CHRTSTAT.LookupCd AND UPPER(CHRTSTAT.LookupName)='CHRTSTAT' AND CHRTSTAT.Source = 'IPEDS_GR150'

		--WHERE HD.CONTROL = 1 AND HD.STABBR = 'PA' 
		--	--AND GR.UNITID IN (214777, 212878, 216010) --PSU, HACC, SHIPP
		--	AND GR.SURVEY_YEAR = 2016
			
	) GRBASE
	UNPIVOT (
			GRTOTAL
			FOR GR_CODE IN (
						DVGRAIM --Detail - American Indian or Alaska Native men 
						, GRAIANM --Detail - American Indian or Alaska Native men
						, GRRACE05 --Detail - American Indian or Alaska Native men 
						, DVGRAIW --Detail - American Indian or Alaska Native women 
						, GRAIANW --Detail - American Indian or Alaska Native women
						, GRRACE06 --Detail - American Indian or Alaska Native women 
						, GRASIAM --Detail - Asian men
						, GRRACE07 --Detail - Asian or Pacific Islander men 
						, GRRACE08 --Detail - Asian or Pacific Islander women 
						, GRASIAW --Detail - Asian women
						, DVGRAPM --Detail - Asian/Native Hawaiian/Other Pacific Islander men 
						, DVGRAPW --Detail - Asian/Native Hawaiian/Other Pacific Islander women 
						, GRRACE03 --Detail - Black non-Hispanic men 
						, GRRACE04 --Detail - Black non-Hispanic women 
						, GRBKAAM --Detail - Black or African American men
						, GRBKAAW --Detail - Black or African American women
						, DVGRBKM --Detail - Black or African American/Black non-Hispanic men 
						, DVGRBKW --Detail - Black or African American/Black non-Hispanic women 
						, GRHISPM --Detail - Hispanic men
						, GRRACE09 --Detail - Hispanic men 
						, DVGRHSM --Detail - Hispanic or Latino/Hispanic  men 
						, DVGRHSW --Detail - Hispanic or Latino/Hispanic  women 
						, GRHISPW --Detail - Hispanic women
						, GRRACE10 --Detail - Hispanic women 
						, GRNHPIM --Detail - Native Hawaiian or Other Pacific Islander men
						, GRNHPIW --Detail - Native Hawaiian or Other Pacific Islander women
						, GRNRALM --Detail - Nonresident alien men
						, GRRACE01 --Detail - Nonresident alien men 
						, GRNRALW --Detail - Nonresident alien women
						, GRRACE02 --Detail - Nonresident alien women
						, GRRACE13 --Detail - Race/ethnicity unknown men
						, GRUNKNM --Detail - Race/ethnicity unknown men
						, GRRACE14 --Detail - Race/ethnicity unknown women
						, GRUNKNW --Detail - Race/ethnicity unknown women
						, GR2MORM --Detail - Two or more races men
						, GR2MORW --Detail - Two or more races women
						, GRWHITM --Detail - White men
						, GRRACE11 --Detail - White non-Hispanic men 
						, GRRACE12 --Detail - White non-Hispanic women 
						, GRWHITW --Detail - White women
						, DVGRWHM --Detail - White/White non-Hispanic men 
						, DVGRWHW --Detail - White/White non-Hispanic women 
			/*Not all Grad Rate data is availalbe by race & gender; need totals for those data points*/
					, GRRACE24 --Total - Grand total 
					, GRTOTLT --Total - Grand total
					, GRTOTLM --Total - Grand total men
					, GRTOTLW --Total - Grand total women
					, GRRACE15 --Total - Total men 
					, GRRACE16 --Total - Total women 
			) 
		) GR_UNPIVOT
	INNER JOIN SHARED.vw_UniqueLookupList L ON LTRIM(RTRIM(UPPER(GR_UNPIVOT.GR_CODE))) = LTRIM(RTRIM(UPPER(L.LookupCd)))
											AND UPPER(L.LookupName) = 'GR150_CODES'
											AND ( /*For 2 year institutions, 100% completions values get Totals (no race/gender breakdown)*/
													(
													GR_UNPIVOT.COHORT = 4 /*2 yr degree seeking*/
													AND GR_UNPIVOT.CHRTSTAT IN (22,23,24) /*100% completion time*/
													AND(
														 UPPER(L.LookupCategory1) = 'T'
														AND UPPER(L.LookupCategory2) = 'T'
														)
													)
													or (/*for all other institutions & cohort statuses; get race/gender breakdowns and exclude totals*/
														UPPER(L.LookupCategory1) <> 'T'
														AND UPPER(L.LookupCategory2) <> 'T'
														)
												)
GROUP BY 
	CAST(GR_UNPIVOT.SURVEY_YEAR AS INT) 
	, CAST(GR_UNPIVOT.UNITID AS INT) 
	, GR_UNPIVOT.StateFIPSCd
	, GR_UNPIVOT.CountyFIPSCd
	, GR_UNPIVOT.COHORT 
	, GR_UNPIVOT.[COHORT DESC] 
	--, GR_UNPIVOT.CHRTSTAT [CohortStatusCd]
	--, GR_UNPIVOT.[CHRTSTAT DESC] [CohortStatusDesc]
	, L.LookupCategory1 
	, L.LookupCategory2 
	--, L.LookupDesc

--ORDER BY UnitID, AcademicYr, CohortCd


GO


DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblFactGraduationRate
SELECT * INTO OSDS_RPT.IPEDS.tblFactGraduationRate FROM OSDS_ETL.IPEDS.vw_FactGraduationRate
CREATE CLUSTERED COLUMNSTORE INDEX IX_tblFactGraduationRates_ClusteredColStore ON OSDS_RPT.IPEDS.tblFactGraduationRate


