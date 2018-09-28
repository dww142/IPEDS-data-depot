/**
	First: Create view for Fall Mobility Fact
	
	TODO:::add field - if year is even - required; else optional 
		allows filtering for required/optional submission years ... ? 

LAST RUN: 
    (729550 rows affected) 
	Total execution time: 00:00:25.213
**/
USE OSDS_ETL;
go
DROP VIEW IPEDS.vw_FactFallMobility
go
CREATE VIEW IPEDS.vw_FactFallMobility AS
	SELECT 
		cast(FM.UNITID as int) [UnitID]
		, cast(FM.SURVEY_YEAR as int) + 1 [AcademicYr]

		, I.StateFIPSCd 
		, I.[CountyFIPSCd]  

		
		, CAST(RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) AS VARCHAR(2)) [ResidenceStateFIPSCd]
		, CASE RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) 
				WHEN '57' THEN 'Residence Unknown' 
				WHEN '90' THEN 'Foreign Countries'
				WHEN '98' THEN 'Residence Unknown' /*'Residence Not Reported'*/
				else RESST.StateName 
			end [ResidenceStateName]
 				, CASE RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) 
				WHEN '57' THEN 'NA' 
				WHEN '90' THEN 'NA'
				WHEN '98' THEN 'NA' /*'Residence Not Reported'*/
				else RESST.StatePostalCd 
			end [ResidenceStatePostalCd]

		, CASE RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) 
				WHEN '57' THEN 'NA' 
				WHEN '90' THEN 'NA'
				WHEN '98' THEN 'NA' /*'Residence Not Reported'*/
				else RESST.PABorderStatus 
			END [ResidenceStatePABorderStatus]

		, CASE RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) 
				WHEN '57' THEN 'NA' 
				WHEN '90' THEN 'NA'
				WHEN '98' THEN 'NA' /*'Residence Not Reported'*/
				else RESST.Contiguous48Status 
			END [ResidenceStateContiguous48Status]
		, CASE RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2) 
				WHEN '57' THEN 'NA' 
				WHEN '90' THEN 'NA'
				WHEN '98' THEN 'NA' /*'Residence Not Reported'*/
				else RESST.StateOrDCStatus 
			END [ResidenceStateOrDCStatus]



		, CAST(REPLACE(FM.EFRES01,'.','') AS BIGINT) [FirstTimeFullTimeDegreeSeekingUndergraduateEnrollment]-- [FTDSUGStudentTotal]
		, CAST(REPLACE(FM.EFRES02,'.','') AS BIGINT) [FirstTimeFullTimeDegreeSeekingUndergraduateEnrollmentGraduatedHSInLast12Months]


	FROM IPEDS.tblEFC FM
		INNER JOIN IPEDS.vw_DimInstitution I ON FM.UNITID = I.UnitID
		LEFT JOIN SHARED.vw_DimState RESST ON RESST.StateFIPSCd = RIGHT('00' + LTRIM(RTRIM(FM.EFCSTATE)),2)


	WHERE CAST(LTRIM(RTRIM(FM.LINE)) AS CHAR(3)) not in ('999','99')

GO

DROP TABLE OSDS_RPT.IPEDS.tblFactFallMobility 
SELECT * INTO OSDS_RPT.IPEDS.tblFactFallMobility FROM OSDS_ETL.IPEDS.vw_FactFallMobility
CREATE CLUSTERED COLUMNSTORE INDEX IX_FallMobility ON OSDS_RPT.IPEDS.tblFactFallMobility
