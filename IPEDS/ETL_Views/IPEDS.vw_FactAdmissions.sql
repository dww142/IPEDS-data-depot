/*************************************
	Combination of Admission data from Admissions survey (2014 onward)
		and IC survey (prev years). 

	Unions together 2 queries pulling applicants/admissions/enrollments from IC
	and ADM survey files from appropriate years;. 

LAST RUN: 
(92047 rows affected) 
Total execution time: 00:00:03.693
*************************************/

use OSDS_ETL;

GO
DROP VIEW IPEDS.vw_FactAdmissions
GO

CREATE VIEW IPEDS.vw_FactAdmissions as

	SELECT DISTINCT
		cast(ADM.UnitID	as int) UnitID
		, cast(ADM.Survey_Year as int) + 1 [AcademicYr]
		, COALESCE(CAST(S.StateFIPSCode AS CHAR(2)),'00') StateFIPSCd
		--, CASE 
		--		WHEN LTRIM(RTRIM(HD.COUNTYCD)) = '-2' THEN '00000' 
		--		ELSE COALESCE(RIGHT('00000' + NULLIF(HD.COUNTYCD,''), 5), '00000') 
		--	END CountyFIPSCd
		, CAST(COALESCE( RIGHT('00000' + NULLIF(NULLIF(HD.COUNTYCD,''),'-2'), 5) /*Formatted county code*/
					, CAST(S.StateFIPSCode AS CHAR(2)) + '000' /*Formatted State FIPS + '000' for state unknown*/
					, '00000')AS VARCHAR(5)) [CountyFIPSCd]  /*Unknown county AND unknown state */
		--, CAST(coalesce(ADMCON1,-1) AS SMALLINT) [SecondarySchoolGpaReqFK]	--Secondary school GPA (used as admission consideration)
		--, CAST(coalesce(ADMCON2,-1) AS SMALLINT) [SecondarySchoolRankReqFK]	--Secondary school rank
		--, CAST(coalesce(ADMCON3,-1) AS SMALLINT) [SecondarySchoolRecordReqFK]	--Secondary school record
		--, CAST(coalesce(ADMCON4,-1) AS SMALLINT) [CompletionOfCollegePreparatoryProgramReqFK]	--Completion of college-preparatory program
		--, CAST(coalesce(ADMCON5,-1) AS SMALLINT) [RecommendationsReqFK]	--Recommendations
		--, CAST(coalesce(ADMCON6,-1) AS SMALLINT) [FormalDemonstrationOfCompetenciesReqFK]	--Formal demonstration of competencies
		--, CAST(coalesce(ADMCON7,-1) AS SMALLINT) [AdmissionTestScoresReqFK]	--Admission test scores
		--, CAST(coalesce(ADMCON8,-1) AS SMALLINT) [TOEFLReqFK]	--TOEFL (Test of English as a Foreign Language
		--, CAST(coalesce(ADMCON9,-1) AS SMALLINT) [OtherTestReqFK]	--Other Test (Wonderlic, WISC-III, etc.)

		, cast(APPLCN as bigint) [ApplicantsTotal]  --Applicants total
		, cast(APPLCNM as bigint) [ApplicantsMen]   --Applicants men
		, cast(APPLCNW as bigint) [ApplicantsWomen] --Applicants women

		, cast(ADMSSN as bigint) [AdmissionsTotal]  --Admissions total
		, cast(ADMSSNM as bigint) [AdmissionsMen]   --Admissions men
		, cast(ADMSSNW as bigint) [AdmissionsWomen] --Admissions women

		, cast(ENRLT as bigint) [EnrolledTotal] --Enrolled total
		, cast(ENRLM as bigint) [EnrolledMen]   --Enrolled  men
		, cast(ENRLW as bigint) [EnrolledWomen] --Enrolled  women

		, cast(ENRLFT as bigint) [EnrolledFullTimeTotal]    --Enrolled full time total
		, cast(ENRLFTM as bigint) [EnrolledFullTimeMen] --Enrolled full time men
		, cast(ENRLFTW as bigint) [EnrolledFullTimeWomen]   --Enrolled full time women

		, cast(ENRLPT as bigint) [EnrolledPartTimeTotal]    --Enrolled part time total
		, cast(ENRLPTM as bigint) [EnrolledPartTimeMen] --Enrolled part time men
		, cast(ENRLPTW as bigint) [EnrolledPartTimeWomen]   --Enrolled part time women
	FROM IPEDS.tblADM ADM
		INNER JOIN IPEDS.tblHD HD ON ADM.UNITID = HD.UNITID AND ADM.SURVEY_YEAR = HD.SURVEY_YEAR
		INNER JOIN SHARED.tblStateImport S ON HD.STABBR = S.StatePostalCode

UNION
/*Prior to 2015 admissions data was collected in IC survey*/

	SELECT DISTINCT
		CAST(IC.UnitID AS INT) UnitID
		, cast(IC.Survey_Year as int) + 1 [AcademicYr]
		, COALESCE(CAST(S.StateFIPSCode AS CHAR(2)),'00') StateFIPSCd
		--, CASE 
		--		WHEN LTRIM(RTRIM(HD.COUNTYCD)) = '-2' THEN '-2' 
		--		ELSE COALESCE(RIGHT('00000' + NULLIF(HD.COUNTYCD,''), 5), '-2') 
		--	END CountyFIPSCd
		, COALESCE( RIGHT('00000' + NULLIF(NULLIF(HD.COUNTYCD,''),'-2'), 5) /*Formatted county code*/
					, CAST(S.StateFIPSCode AS CHAR(2)) + '000' /*Formatted State FIPS + '000' for state unknown*/
					, '00000') [CountyFIPSCd] /*Unknown county AND unknown state */
		--, CAST(coalesce(ADMCON1,-1) AS SMALLINT) [SecondarySchoolGpaReqFK]	--Secondary school GPA (used as admission consideration)
		--, CAST(coalesce(ADMCON2,-1) AS SMALLINT) [SecondarySchoolRankReqFK]	--Secondary school rank
		--, CAST(coalesce(ADMCON3,-1) AS SMALLINT) [SecondarySchoolRecordReqFK]	--Secondary school record
		--, CAST(coalesce(ADMCON4,-1) AS SMALLINT) [CompletionOfCollegePreparatoryProgramReqFK]	--Completion of college-preparatory program
		--, CAST(coalesce(ADMCON5,-1) AS SMALLINT) [RecommendationsReqFK]	--Recommendations
		--, CAST(coalesce(ADMCON6,-1) AS SMALLINT) [FormalDemonstrationOfCompetenciesReqFK]	--Formal demonstration of competencies
		--, CAST(coalesce(ADMCON7,-1) AS SMALLINT) [AdmissionTestScoresReqFK]	--Admission test scores
		--, CAST(coalesce(ADMCON8,-1) AS SMALLINT) [TOEFLReqFK]	--TOEFL (Test of English as a Foreign Language
		--, CAST(coalesce(ADMCON9,-1) AS SMALLINT) [OtherTestReqFK]	--Other Test (Wonderlic, WISC-III, etc.)

		, cast(APPLCN as bigint) [ApplicantsTotal]  --Applicants total
		, cast(APPLCNM as bigint) [ApplicantsMen]   --Applicants men
		, cast(APPLCNW as bigint) [ApplicantsWomen] --Applicants women

		, cast(ADMSSN as bigint) [AdmissionsTotal]  --Admissions total
		, cast(ADMSSNM as bigint) [AdmissionsMen]   --Admissions men
		, cast(ADMSSNW as bigint) [AdmissionsWomen] --Admissions women

		, cast(ENRLT as bigint) [EnrolledTotal] --Enrolled total
		, cast(ENRLM as bigint) [EnrolledMen]   --Enrolled  men
		, cast(ENRLW as bigint) [EnrolledWomen] --Enrolled  women

		, cast(ENRLFT as bigint) [EnrolledFullTimeTotal]    --Enrolled full time total
		, cast(ENRLFTM as bigint) [EnrolledFullTimeMen] --Enrolled full time men
		, cast(ENRLFTW as bigint) [EnrolledFullTimeWomen]   --Enrolled full time women

		, cast(ENRLPT as bigint) [EnrolledPartTimeTotal]    --Enrolled part time total
		, cast(ENRLPTM as bigint) [EnrolledPartTimeMen] --Enrolled part time men
		, cast(ENRLPTW as bigint) [EnrolledPartTimeWomen]   --Enrolled part time women
	FROM IPEDS.tblIC IC
		INNER JOIN IPEDS.tblHD HD ON IC.UNITID = HD.UNITID AND IC.SURVEY_YEAR = HD.SURVEY_YEAR
		INNER JOIN SHARED.tblStateImport S ON HD.STABBR = S.StatePostalCode
	WHERE IC.SURVEY_YEAR + 1 < 2015

go

DROP TABLE OSDS_RPT.IPEDS.tblFactAdmissions
SELECT * INTO OSDS_RPT.IPEDS.tblFactAdmissions FROM OSDS_ETL.IPEDS.vw_FactAdmissions
CREATE CLUSTERED COLUMNSTORE INDEX IX_Admissions ON OSDS_RPT.IPEDS.tblFactAdmissions