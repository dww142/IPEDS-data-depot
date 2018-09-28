/*
	Fall retention survey
	- collections data on two years at a time
	i.e., 2016 survey: collects data on fall 2016 entering cohort
					AND the retained students from the fall 2015 cohort

	this query flattens it to look at data from the perspective of
	- get fall 2015 cohort; get retention from fall 2016 collection
	- compare the cohort reported in prior year to cohort reported in retention year for data quality check
	- 
*/

USE OSDS_ETL;
GO
DROP VIEW IF EXISTS IPEDS.vw_FactFallEnrollmentRetention
GO
CREATE VIEW IPEDS.vw_FactFallEnrollmentRetention AS
	SELECT DISTINCT
		cast(BASEYR.SURVEY_YEAR as int) + 1 [BaseAcademicYr]
		, cast(BASEYR.UNITID as int) UnitID
		, I.StateFIPSCd
		, I.CountyFIPSCd
		, 'Fall ' + CAST(BASEYR.SURVEY_YEAR AS CHAR(4)) [CohortStartingTerm]

		, cast(BASEYR.UGENTERN as bigint) [TotalUndergraduatesEnteringInFall]
		, cast(BASEYR.GRCOHRT as bigint) [FirstTimeFullTimeFallCohort]

		, RETAINYR.SURVEY_YEAR + 1 [RetentionAcademicYr]
		, 'Fall ' + CAST(RETAINYR.SURVEY_YEAR AS CHAR(4)) [CohortRetentionTerm]
		, RETAINYR.[Full-time fall 2015 cohort] [FullTimeCohortReportedInRetentionYear]
		, RETAINYR.[Full-time adjusted fall 2015 cohort] [FullTimeAdjustedCohortInRetentionYear]
		, RETAINYR.[Exclusions from full-time fall 2015 cohort] [FullTimeExclusionsReportedInRetentionYear]
		, RETAINYR.[Inclusions to the full-time fall 2015 cohort] [FullTimeInclusionsReportedInRetentionYear]
		, RETAINYR.[Students from the full-time adjusted fall 2015 cohort enrolled in fall 2016] [FullTimeStudentsEnrolledInRetentionYear]
	
		, RETAINYR.[Part-time fall 2015 cohort] [PartTimeCohortReportedInRetentionYear]
		, RETAINYR.[Part-time adjusted fall 2015 cohort] [PartTimeAdjustedCohortInRetentionYear]
		, RETAINYR.[Exclusions from Part-time fall 2015 cohort] [PartTimeExclusionsReportedInRetentionYear]
		, RETAINYR.[Inclusions to the Part-time fall 2015 cohort] [PartTimeInclusionsReportedInRetentionYear]
		, RETAINYR.[Students from the Part-time adjusted fall 2015 cohort enrolled in fall 2016] [PartTimeStudentsEnrolledInRetentionYear]


	FROM IPEDS.tblEFD BASEYR
		INNER JOIN IPEDS.vw_DimInstitution I ON BASEYR.UNITID = I.UnitID
		LEFT JOIN (
				SELECT DISTINCT EFD.SURVEY_YEAR
					, EFD.UNITID
					, EFD.RRFTCT
					, CAST(RRFTCT AS BIGINT) [Full-time fall 2015 cohort]
					, CAST(RRFTEX AS BIGINT) [Exclusions from full-time fall 2015 cohort]
					, CAST(RRFTIN AS BIGINT) [Inclusions to the full-time fall 2015 cohort]
					, CAST(RRFTCTA AS BIGINT) [Full-time adjusted fall 2015 cohort]
					, CAST(RET_NMF AS BIGINT) [Students from the full-time adjusted fall 2015 cohort enrolled in fall 2016]
					, CAST(RET_PCF AS BIGINT) [Full-time retention rate, 2016]
					, CAST(RRPTCT AS BIGINT) [Part-time fall 2015 cohort]
					, CAST(RRPTEX AS BIGINT) [Exclusions from part-time fall 2015 cohort]
					, CAST(RRPTIN AS BIGINT) [Inclusions to the part-time fall 2015 cohort]
					, CAST(RRPTCTA AS BIGINT) [Part-time adjusted fall 2015 cohort]
					, CAST(RET_NMP AS BIGINT) [Students from the part-time adjusted fall 2015 cohort enrolled in fall 2016]
					, CAST(RET_PCP AS BIGINT) [Part-time retention rate, 2016]
				FROM IPEDS.tblEFD EFD
			) RETAINYR ON BASEYR.SURVEY_YEAR + 1 = RETAINYR.SURVEY_YEAR
						AND BASEYR.UNITID = RETAINYR.UNITID
	WHERE BASEYR.SURVEY_YEAR > 2006
		--AND I.UnitID LIKE 214777
	GO

DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblFactFallEnrollmentRetention
SELECT * INTO OSDS_RPT.IPEDS.tblFactFallEnrollmentRetention FROM OSDS_ETL.IPEDS.vw_FactFallEnrollmentRetention
CREATE CLUSTERED COLUMNSTORE INDEX IX_FallEnrollmentRetention ON OSDS_RPT.IPEDS.tblFactFallEnrollmentRetention