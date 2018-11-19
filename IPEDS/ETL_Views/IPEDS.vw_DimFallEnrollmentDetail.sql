/*
	Dimension lookup for IPEDS Fall Enrollment detailed categories
		codes are part of the unique key of the EFA fall enrollment data set
		Exclude the codes representing totals - 
			only include the most granular data elements and allow BI solution (SSAS cubes to calculate totals as sums)
	
    (14 rows affected) 
	Total execution time: 00:00:00.222
*/
USE OSDS_ETL;
DROP VIEW IPEDS.vw_DimFallEnrollmentDetail
GO
CREATE VIEW IPEDS.vw_DimFallEnrollmentDetail AS
	SELECT DISTINCT	
		cast(L.LookupCd as smallint) FallEnrollDetailPK
		, CAST(L.LookupDesc AS VARCHAR(100)) FallEnrollDetailDesc
		, case WHEN cast(L.LookupCd as smallint) IN (32,36,52,56) THEN 'Graduate'
			WHEN cast(L.LookupCd as smallint) IN (24,25,31,39,40,44,45,51,59,60) THEN 'Undergraduate'
			else 'NA'
			end [FallEnrollStudentLevel]

	FROM SHARED.tblLookupImport L
	WHERE upper(L.LookupName)=upper('EFALEVEL')
		AND 
		(
			cast(L.LookupCd as smallint) IN (24,25,31,39,40,44,45,51,59,60) --UNDERGRADUATES - ELIMINATE derived totals from IPEDS, let cube calculate totals
			OR cast(L.LookupCd as smallint) IN (32,36,52,56) --GRADUATES - ELIMINATE derived totals from IPEDS, let cube calculate totals
		)
	GO
	
DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblDimFallEnrollmentDetail
SELECT * INTO OSDS_RPT.IPEDS.tblDimFallEnrollmentDetail	FROM OSDS_ETL.IPEDS.vw_DimFallEnrollmentDetail
CREATE CLUSTERED COLUMNSTORE INDEX IX_FallEnrollDetail ON OSDS_RPT.IPEDS.tblDimFallEnrollmentDetail