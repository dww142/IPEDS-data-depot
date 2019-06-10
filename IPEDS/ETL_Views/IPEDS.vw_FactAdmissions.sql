/*************************************
	Combination of Admission data from Admissions survey (2014 onward)
		and IC survey (prev years). 

	Unions together 2 queries pulling applicants/admissions/enrollments from IC
	and ADM survey files from appropriate years;. 

*************************************/

use OSDS_ETL;

GO
DROP VIEW IF EXISTS IPEDS.vw_FactAdmissions
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
		
		, CASE CAST(coalesce(ADMCON1,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolGpaRequired]
		, CASE CAST(coalesce(ADMCON2,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolRankRequired]
		, CASE CAST(coalesce(ADMCON3,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolRecordRequired]
		, CASE CAST(coalesce(ADMCON4,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [CompletionOfCollegePreparatoryProgramRequired]
		, CASE CAST(coalesce(ADMCON5,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [RecommendationsRequired]
		, CASE CAST(coalesce(ADMCON6,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [FormalDemonstrationOfCompetenciesRequired]
		, CASE CAST(coalesce(ADMCON7,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [AdmissionTestScoresRequired]
		, CASE CAST(coalesce(ADMCON8,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [TOEFLRequired]
		, CASE CAST(coalesce(ADMCON9,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [OtherTestRequired]

		, CASE WHEN ADMCON1 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 = '1' THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsRequired]
		, CASE WHEN ADMCON1 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('2','5') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsRecommondedOrConsidered]
		, CASE WHEN ADMCON1 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('3') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotRequiredOrRecommended]
		, CASE WHEN ADMCON1 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('-1','9','') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotReported]
		, CASE WHEN ADMCON1 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('-2') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotApplicable]	

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

		, CAST(SATNUM as int) [Number of first-time degree/certificate-seeking students submitting SAT scores]
		, CAST(SATPCT as int) [Percent of first-time degree/certificate-seeking students submitting SAT scores]
		, CAST(ACTNUM as int) [Number of first-time degree/certificate-seeking students submitting ACT scores]
		, CAST(ACTPCT as int) [Percent of first-time degree/certificate-seeking students submitting ACT scores]
		, CAST(SATVR25 as int) [SAT Critical Reading 25th percentile score]
		, CAST(SATVR75 as int) [SAT Critical Reading 75th percentile score]
		, CAST(SATMT25 as int) [SAT Math 25th percentile score]
		, CAST(SATMT75 as int) [SAT Math 75th percentile score]
		, CAST(ACTCM25 as int) [ACT Composite 25th percentile score]
		, CAST(ACTCM75 as int) [ACT Composite 75th percentile score]
		, CAST(ACTEN25 as int) [ACT English 25th percentile score]
		, CAST(ACTEN75 as int) [ACT English 75th percentile score]
		, CAST(ACTMT25 as int) [ACT Math 25th percentile score]
		, CAST(ACTMT75 as int) [ACT Math 75th percentile score]

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
		, CASE CAST(coalesce(ADMCON1,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolGpaRequired]
		, CASE CAST(coalesce(ADMCON2,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolRankRequired]
		, CASE CAST(coalesce(ADMCON3,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [SecondarySchoolRecordRequired]
		, CASE CAST(coalesce(ADMCON4,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [CompletionOfCollegePreparatoryProgramRequired]
		, CASE CAST(coalesce(ADMCON5,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [RecommendationsRequired]
		, CASE CAST(coalesce(ADMCON6,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [FormalDemonstrationOfCompetenciesRequired]
		, CASE CAST(coalesce(ADMCON7,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [AdmissionTestScoresRequired]
		, CASE CAST(coalesce(ADMCON8,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [TOEFLRequired]
		, CASE CAST(coalesce(ADMCON9,-1) AS SMALLINT)
				WHEN '-2' THEN 'Not applicable'
				WHEN '-1' THEN 'Not reported'
				WHEN '1' THEN 'Required'
				WHEN '2' THEN 'Recommended'
				WHEN '3' THEN 'Neither required nor recommended'
				WHEN '4' THEN 'Do not know'
				WHEN '5' THEN 'Considered but not required'
				WHEN '9' THEN ''
				ELSE ''
			END [OtherTestRequired]

		, CASE WHEN ADMCON1 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 = '1' THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 = '1' THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsRequired]
		, CASE WHEN ADMCON1 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('2','5') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('2','5') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsRecommondedOrConsidered]
		, CASE WHEN ADMCON1 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('3') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('3') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotRequiredOrRecommended]
		, CASE WHEN ADMCON1 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('-1','9','') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('-1','9','') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotReported]
		, CASE WHEN ADMCON1 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON2 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON3 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON4 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON5 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON6 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON7 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON8 in ('-2') THEN 1 ELSE 0 END
				+ CASE WHEN ADMCON9 in ('-2') THEN 1 ELSE 0 END
			[NumberOfAdmissionsConsiderationsNotApplicable]	
				
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

		, CAST(SATNUM as int) [Number of first-time degree/certificate-seeking students submitting SAT scores]
		, CAST(SATPCT as int) [Percent of first-time degree/certificate-seeking students submitting SAT scores]
		, CAST(ACTNUM as int) [Number of first-time degree/certificate-seeking students submitting ACT scores]
		, CAST(ACTPCT as int) [Percent of first-time degree/certificate-seeking students submitting ACT scores]
		, CAST(SATVR25 as int) [SAT Critical Reading 25th percentile score]
		, CAST(SATVR75 as int) [SAT Critical Reading 75th percentile score]
		, CAST(SATMT25 as int) [SAT Math 25th percentile score]
		, CAST(SATMT75 as int) [SAT Math 75th percentile score]
		, CAST(ACTCM25 as int) [ACT Composite 25th percentile score]
		, CAST(ACTCM75 as int) [ACT Composite 75th percentile score]
		, CAST(ACTEN25 as int) [ACT English 25th percentile score]
		, CAST(ACTEN75 as int) [ACT English 75th percentile score]
		, CAST(ACTMT25 as int) [ACT Math 25th percentile score]
		, CAST(ACTMT75 as int) [ACT Math 75th percentile score]

	FROM IPEDS.tblIC IC
		INNER JOIN IPEDS.tblHD HD ON IC.UNITID = HD.UNITID AND IC.SURVEY_YEAR = HD.SURVEY_YEAR
		INNER JOIN SHARED.tblStateImport S ON HD.STABBR = S.StatePostalCode
	WHERE IC.SURVEY_YEAR + 1 < 2015

go

DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblFactAdmissions
SELECT * INTO OSDS_RPT.IPEDS.tblFactAdmissions FROM OSDS_ETL.IPEDS.vw_FactAdmissions
CREATE CLUSTERED COLUMNSTORE INDEX IX_Admissions ON OSDS_RPT.IPEDS.tblFactAdmissions