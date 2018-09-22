/***************************

	Fact view of FTE Instructional Activity from the IPEDS
		Unduplicated full academic year enrollment survey. 

    (105363 rows affected) 
	Total execution time: 00:00:10.709
***************************/
USE OSDS_ETL;

GO
DROP VIEW IPEDS.vw_FactFTEInstructionalActivity
GO
CREATE VIEW IPEDS.vw_FactFTEInstructionalActivity AS

	SELECT 
		CAST(E.SURVEY_YEAR AS INT) AcademicYr
		, cast(E.UnitID as int) UnitID

		, I.StateFIPSCd
		, I.CountyFIPSCd

		, CAST(ACTTYPE AS INT) [IsInstructionalActivityBasedOnCreditOrContactHours]	--Is instructional activity based on credit or contact hours

		, CAST(CDACTUA AS BIGINT) [12MonthInstructionalActivityCreditHoursUndergraduates]	--12-month instructional activity credit hours: undergraduates
		, CAST(CNACTUA AS BIGINT) [12MonthInstructionalActivityContactHoursUndergraduates]	--12-month instructional activity contact hours: undergraduates
		, CAST(CDACTGA AS bigint) [12MonthInstructionalActivityCreditHoursGraduates]	--12-month instructional activity credit hours: graduates

		, CAST(EFTEUG AS BIGINT) [EstimatedFullTimeEquivalentFTEUndergraduateEnrollment]	--Estimated full-time equivalent (FTE) undergraduate enrollment, 2013-14
		, CAST(EFTEGD AS BIGINT) [EstimatedFullTimeEquivalentFTEGraduateEnrollment]	--Estimated full-time equivalent (FTE) graduate enrollment, 2013-14
	
		, CAST(FTEUG AS BIGINT) [ReportedFullTimeEquivalentFTEUndergraduateEnrollment]	--Reported full-time equivalent (FTE) undergraduate enrollment, 2013-14
		, CAST(FTEGD AS BIGINT) [ReportedFullTimeEquivalentFTEGraduateEnrollment]	--Reported full-time equivalent (FTE) graduate enrollment, 2013-14
		, CAST(FTEDPP AS BIGINT) [ReportedFullTimeEquivalentFTEDoctorsProfessionalPractice]	--Reported full-time equivalent (FTE) doctors professional practice, 2013-14
	FROM IPEDS.tblEFIA E
		INNER JOIN IPEDS.vw_DimInstitution I ON E.UNITID = I.UnitID

	GO


DROP TABLE OSDS_RPT.IPEDS.tblFactFTEInstructionalActivity

SELECT * INTO OSDS_RPT.IPEDS.tblFactFTEInstructionalActivity FROM OSDS_ETL.IPEDS.vw_FactFTEInstructionalActivity

CREATE CLUSTERED COLUMNSTORE INDEX IX_FTEInstrAct_ClusteredColStore ON OSDS_RPT.IPEDS.tblFactFTEInstructionalActivity