/*
	EF__CP - semi-annual enrollment by CIP survey

	LSTUDY = combination of LINE and SECTION
	EFCIPLEV = cobmination of LSTUDY and CIPCODE
LSTUDY, LINE, SECTION codes match the EFA file codes for the same fields (all fall enrollment)

Logical Key of source filefile: SURVEY_YEAR, UNITID, EFCIPLEV
Logical Key of View: AcademicYr, UnitID, FallEnrollDetailFK, FallAttendanceStatusFK, Gender, IPEDSRaceCd, CipCdFK

	Only CIP Codes included in this survey are:
		13.0000	Education
		14.0000	Engineering
		26.0000	Biological Sciences/Life Sciences
		27.0000	Mathematics
		40.0000	Physical Sciences
		51.0401	Dentistry
		51.1201	Medicine
		52.0000	Business Management and Administrative Services
		22.0101	Law
		99

*/
USE OSDS_ETL;
GO

DROP VIEW IF EXISTS IPEDS.vw_FactFallEnrollmentByMajorCIP
go
create VIEW IPEDS.vw_FactFallEnrollmentByMajorCIP as 
	SELECT 
		BASE.SURVEY_YEAR + 1 [AcademicYr]
		, BASE.UNITID [UnitID]
		, I.StateFIPSCd
		, I.CountyFIPSCd
		
		, CAST(BASE.CIPCODE AS VARCHAR(10)) [CipCdFK]
	
		, CAST(BASE.LSTUDY AS SMALLINT) [FallEnrollDetailFK]
		, CAST(BASE.SECTION AS SMALLINT) [FallAttendanceStatusFK]

		, L.LookupCategory1 [GenderCd]
		, l.LookupCategory2 [IPEDSRaceCd]

		, SUM(BASE.ENROLLMENT) [Enrollment]
	FROM (
			SELECT DISTINCT
				CAST(EFCP_UNPIVOT.SURVEY_YEAR AS INT) SURVEY_YEAR
				, CAST(EFCP_UNPIVOT.UNITID AS INT) UNITID
				, EFCP_UNPIVOT.EFCIPLEV
				, CAST(EFCP_UNPIVOT.CIPCODE AS VARCHAR(10)) CIPCODE
				, EFCP_UNPIVOT.LINE  LINE
				, EFCP_UNPIVOT.SECTION SECTION
				, EFCP_UNPIVOT.LSTUDY LSTUDY
				, EFCP_CODE
				, CAST(ENROLLMENT AS BIGINT) ENROLLMENT
			FROM (
					SELECT  
						EF.SURVEY_YEAR
						, EF.UNITID
						, EF.EFCIPLEV
						, EF.CIPCODE
						, EF.LINE
						, EF.SECTION
						, EF.LSTUDY

						, DVEFAIM, DVEFAIW, DVEFAPM, DVEFAPW, DVEFBKM, DVEFBKW, DVEFHSM, DVEFHSW, DVEFWHM, DVEFWHW, EF2MORM, EF2MORW, 
						EFAIANM, EFAIANW, EFASIAM, EFASIAW, EFBKAAM, EFBKAAW, EFHISPM, EFHISPW, EFNHPIM, EFNHPIW, EFNRALM, EFNRALW, 
						EFRACE03, EFRACE04, EFRACE05, EFRACE06, EFRACE07, EFRACE08, EFRACE09, EFRACE10, EFRACE11, EFRACE12, EFUNKNM, 
						EFUNKNW, EFWHITM, EFWHITW

					FROM IPEDS.tblEFCP EF
					WHERE --EF.SURVEY_YEAR = '2016' AND EF.UNITID = 214777 AND	------------TEST FILTERS
						EF.LSTUDY IN (
								--1,	  --  All students total
								--2,	  --  All students, Undergraduate total
								--3,	  --  All students, Undergraduate, Degree/certificate-seeking total
								--4,	  --  All students, Undergraduate, Degree/certificate-seeking, First-time
								--5,	  --  All students, Undergraduate, Degree/certificate-seeking, Other degree/certificate-seeking
								--19,     --  All students, Undergraduate, Other degree/certifcate-seeking, Transfer-ins
								--20,     --  All students, Undergraduate, Other degree/certifcate-seeking, Continuing
								--11,     --  All students, Undergraduate, Non-degree/certificate-seeking
								--12,     --  All students, Graduate
								--16,     --  All students, First professional

								--21,     --  Full-time students total
								--22,     --  Full-time students, Undergraduate total
								--23,     --  Full-time students, Undergraduate, Degree/certificate-seeking total

								24,     --  Full-time students, Undergraduate, Degree/certificate-seeking, First-time
								--25,     --  Full-time students, Undergraduate, Degree/certificate-seeking, Other degree/certificate-seeking ------TOTAL OF 39 AND 40
								39,     --  Full-time students, Undergraduate, Other degree/certifcate-seeking, Transfer-ins
								40,     --  Full-time students, Undergraduate, Other degree/certifcate-seeking, Continuing
								31,     --  Full-time students, Undergraduate, Non-degree/certificate-seeking
								32,     --  Full-time students, Graduate
								36,     --  Full-time students, First professional

								--41,     --  Part-time students total
								--42,     --  Part-time students, Undergraduate total
								--43,     --  Part-time students, Undergraduate, Degree/certificate-seeking total

								44,     --  Part-time students, Undergraduate, Degree/certificate-seeking, First-time
								--45,     --  Part-time students, Undergraduate, Degree/certificate-seeking, Other degree/certificate-seeking -------TOTAL OF 59 AND 60
								59,     --  Part-time students, Undergraduate, Other degree/certifcate-seeking, Transfer-ins
								60,     --  Part-time students, Undergraduate, Other degree/certifcate-seeking, Continuing
								51,     --  Part-time students, Undergraduate, Non-degree/certificate-seeking
								52,     --  Part-time students, Graduate
								56      --  Part-time students, First professional
						)
				) EFCP_BASE
				UNPIVOT 
				( ENROLLMENT FOR EFCP_CODE IN
					(DVEFAIM, DVEFAIW, DVEFAPM, DVEFAPW, DVEFBKM, DVEFBKW, DVEFHSM, DVEFHSW, DVEFWHM, DVEFWHW, EF2MORM, EF2MORW, 
					EFAIANM, EFAIANW, EFASIAM, EFASIAW, EFBKAAM, EFBKAAW, EFHISPM, EFHISPW, EFNHPIM, EFNHPIW, EFNRALM, EFNRALW, 
					EFRACE03, EFRACE04, EFRACE05, EFRACE06, EFRACE07, EFRACE08, EFRACE09, EFRACE10, EFRACE11, EFRACE12, EFUNKNM, 
					EFUNKNW, EFWHITM, EFWHITW)
				) EFCP_UNPIVOT
		) BASE
		INNER JOIN IPEDS.vw_DimInstitution I ON BASE.UNITID = I.UnitID
		INNER JOIN SHARED.tblLookupImport L ON LTRIM(RTRIM(BASE.EFCP_CODE)) = L.LookupCd
											AND UPPER(L.LookupName) = 'EFCP_CODES'
											AND UPPER(L.LookupCategory1) <> 'T'
											AND UPPER(L.LookupCategory2) <> 'T'
	WHERE
		(	BASE.SURVEY_YEAR <= 2007
			AND UPPER(BASE.EFCP_CODE) LIKE 'EFRACE%'
		)
		OR
		(	BASE.SURVEY_YEAR BETWEEN 2008 AND 2009
			AND (UPPER(BASE.EFCP_CODE) LIKE 'DV%' 
					OR UPPER(BASE.EFCP_CODE) IN ('EFUNKNM','EFUNKNW') /*Unk Race and 2 or more not included in derived variables*/
					OR UPPER(BASE.EFCP_CODE) IN ('EF2MORM','EF2MORW') /*Unk Race and 2 or more not included in derived variables*/
				)
		)
		OR
		(	BASE.SURVEY_YEAR >= 2010
			AND UPPER(BASE.EFCP_CODE) LIKE 'EF%' 
			AND UPPER(BASE.EFCP_CODE) NOT LIKE 'EFRACE%'
		)
	GROUP BY 
		BASE.SURVEY_YEAR 
		, BASE.UNITID
		, I.StateFIPSCd
		, I.CountyFIPSCd 
		, BASE.CIPCODE 
		, BASE.LSTUDY 
		, BASE.SECTION 
		, L.LookupCategory1 
		, l.LookupCategory2

GO


DROP TABLE  IF EXISTS OSDS_RPT.IPEDS.tblFactFallEnrollmentByMajorCIP
SELECT * INTO OSDS_RPT.IPEDS.tblFactFallEnrollmentByMajorCIP FROM OSDS_ETL.IPEDS.vw_FactFallEnrollmentByMajorCIP
CREATE CLUSTERED COLUMNSTORE INDEX IX_FallEnrollCIP ON OSDS_RPT.IPEDS.tblFactFallEnrollmentByMajorCIP
