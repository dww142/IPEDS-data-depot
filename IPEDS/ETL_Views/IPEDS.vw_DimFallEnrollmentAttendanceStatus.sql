/*
	IPEDS Fall Enrollment Attendance Status values
	excludes - totals (i.e., code set includes values for full time, part time and total - exclude total from view)

	Unique constraint on primary key value
*/
USE OSDS_ETL;
DROP VIEW IPEDS.vw_DimFallEnrollmentAttendanceStatus 
GO
CREATE VIEW IPEDS.vw_DimFallEnrollmentAttendanceStatus AS
	SELECT DISTINCT	
		cast(L.LookupCd as smallint) FallAttendanceStatusPK
		, CAST(L.LookupDesc AS VARCHAR(100)) FallAttendanceStatusDesc

	FROM SHARED.tblLookupImport L
	WHERE upper(L.LookupName)=upper('SECTION') 
		and RIGHT(Source,3)='EFA' AND L.LookupCd <>'3'

	GO

DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblDimFallEnrollmentAttendanceStatus
SELECT * INTO OSDS_RPT.IPEDS.tblDimFallEnrollmentAttendanceStatus FROM OSDS_ETL.IPEDS.vw_DimFallEnrollmentAttendanceStatus
CREATE CLUSTERED COLUMNSTORE INDEX IX_FallEnrollStatus ON OSDS_RPT.IPEDS.tblDimFallEnrollmentAttendanceStatus