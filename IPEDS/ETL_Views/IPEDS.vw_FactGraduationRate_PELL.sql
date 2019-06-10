USE OSDS_ETL;
GO
DROP VIEW IF EXISTS IPEDS.vw_FactGraduationRate_PELL
GO

CREATE VIEW IPEDS.vw_FactGraduationRate_PELL AS

	SELECT
		GP_UP.SURVEY_YEAR [AcademicYr]
		, GP_UP.UNITID [UnitID]
		, GP_UP.PSGRTYPE [CohortTypeCd]
        , CASE GP_UP.PSGRTYPE
                WHEN '1' THEN 'Total cohort (Bachelor''s and other degree/certificate seeking) - four-year institutions'
                WHEN '2' THEN 'Bachelor''s degree seeking cohort - four-year institutions'
                WHEN '3' THEN 'Other degree/certificate seeking cohort - four-year institutions'
                WHEN '4' THEN 'Degree/certificate seeking cohort (less than four-year institutions)'
                ELSE 'n/a'
            END [CohortTypeDesc]
		, L.LookupCategory1 [PellStatus]
		, L.LookupCategory2 [StaffordLoanStatus]
		, max(CASE WHEN L.LookupCd LIKE '%REVCT' THEN GP_UP.GP_VALUE else null end) [RevisedCohort]
		, max(CASE WHEN L.LookupCd LIKE '%EXCLU' THEN GP_UP.GP_VALUE else null end ) [Exclusion]
		, max(CASE WHEN L.LookupCd LIKE '%ADJCT' THEN GP_UP.GP_VALUE else null end) [AdjustedCohort]
		, max(CASE WHEN L.LookupCd LIKE '%MTOT' THEN GP_UP.GP_VALUE else null end) [CompletersIn150PctTime]
		
	FROM ( 
			SELECT
				CAST(GP.SURVEY_YEAR AS INT) SURVEY_YEAR
				, CAST(GP.UNITID AS INT) UNITID 
				, CAST(GP.PSGRTYPE AS SMALLINT) PSGRTYPE 
				, CAST(PGREVCT AS BIGINT) PGREVCT
				, CAST(PGEXCLU AS BIGINT) PGEXCLU
				, CAST(PGADJCT AS BIGINT) PGADJCT
				, CAST(PGCMBAC AS BIGINT) PGCMBAC
				, CAST(PGCMOBA AS BIGINT) PGCMOBA
				, CAST(PGCMTOT AS BIGINT) PGCMTOT
				, CAST(SSREVCT AS BIGINT) SSREVCT
				, CAST(SSEXCLU AS BIGINT) SSEXCLU
				, CAST(SSADJCT AS BIGINT) SSADJCT
				, CAST(SSCMBAC AS BIGINT) SSCMBAC
				, CAST(SSCMOBA AS BIGINT) SSCMOBA
				, CAST(SSCMTOT AS BIGINT) SSCMTOT
				, CAST(NRREVCT AS BIGINT) NRREVCT
				, CAST(NREXCLU AS BIGINT) NREXCLU
				, CAST(NRADJCT AS BIGINT) NRADJCT
				, CAST(NRCMBAC AS BIGINT) NRCMBAC
				, CAST(NRCMOBA AS BIGINT) NRCMOBA
				, CAST(NRCMTOT AS BIGINT) NRCMTOT
				, CAST(TTREVCT AS BIGINT) TTREVCT
				, CAST(TTEXCLU AS BIGINT) TTEXCLU
				, CAST(TTADJCT AS BIGINT) TTADJCT
				, CAST(TTCMBAC AS BIGINT) TTCMBAC
				, CAST(TTCMOBA AS BIGINT) TTCMOBA
				, CAST(TTCMTOT AS BIGINT) TTCMTOT
			FROM IPEDS.tblGR_PELL_SSL GP
		 ) GP_BASE
		 UNPIVOT (
			GP_VALUE
			FOR GP_CODE IN 
						(
						PGREVCT, PGEXCLU, PGADJCT, PGCMBAC, PGCMOBA, PGCMTOT
						, SSREVCT, SSEXCLU, SSADJCT, SSCMBAC, SSCMOBA, SSCMTOT
						, NRREVCT, NREXCLU, NRADJCT, NRCMBAC, NRCMOBA, NRCMTOT
						, TTREVCT, TTEXCLU, TTADJCT, TTCMBAC, TTCMOBA, TTCMTOT
					)
			) GP_UP
		INNER JOIN SHARED.tblLookupImport L ON LTRIM(RTRIM(GP_UP.GP_CODE)) = L.LookupCd 
											AND L.Source = 'IPEDS_GR_PELL'
											AND L.LookupName = 'GR_PELL_CODES'
											AND L.LookupCategory1 <> 'NA' AND L.LookupCategory2 <> 'NA' --REMOVE GRAND TOTAL ROW

	GROUP BY GP_UP.SURVEY_YEAR 
		, GP_UP.UNITID 
		, GP_UP.PSGRTYPE 
		, L.LookupCategory1 
		, L.LookupCategory2
GO


DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblFactGraduationRate_PELL
SELECT * INTO OSDS_RPT.IPEDS.tblFactGraduationRate_PELL FROM OSDS_ETL.IPEDS.vw_FactGraduationRate_PELL
CREATE CLUSTERED COLUMNSTORE INDEX IX_GradRate_Pell on OSDS_RPT.IPEDS.tblFactGraduationRate_PELL
