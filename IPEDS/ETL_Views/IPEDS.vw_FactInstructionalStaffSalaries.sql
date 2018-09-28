/*
	MonthsCoveredBySalaryOutlays = (9 month staff * 9) + (10 month staff * 10) + (11 month staff * 11) + (12 month staff * 12) 

	two queries - one for male staff, one for female - union together; 
		eliminate totals; 
		eliminate all staff academic rank records

	Example use for average monthly salary (For any selected gender and/or academic rank): 
		sum([Total Salary Outlays])
		/
		sum([Number Months Covered By Salary Outlays])

TODO: source academic rank from a lookup rather than a CASE statement
*/
USE OSDS_ETL;
GO
DROP VIEW IF EXISTS IPEDS.vw_FactInstructionalStaffSalaries
GO

CREATE VIEW IPEDS.vw_FactInstructionalStaffSalaries AS

SELECT
/*key*/
	cast(SAL.SURVEY_YEAR as int) [AcademicYr]
	, cast(SAL.UNITID as int) UnitID
	, CASE CAST(SAL.ARANK AS SMALLINT)
			WHEN 7 THEN 'All instructional staff total'
			WHEN 1 THEN 'Professor'
			WHEN 2 THEN 'Associate professor'
			WHEN 3 THEN 'Assistant professor'
			WHEN 4 THEN 'Instructor'
			WHEN 5 THEN 'Lecturer'
			WHEN 6 THEN 'No academic rank'
			ELSE 'Other'
		end [AcademicRank]
	, 'M' GenderCd
/*end key*/
	, COALESCE(
				CAST(SAINSTM AS BIGINT)
				, COALESCE(CAST(SA_9MCM AS BIGINT),0)
					+ COALESCE(CAST(SA09MCM AS BIGINT),0)
					+ COALESCE(CAST(SA10MCM AS BIGINT),0)
					+ COALESCE(CAST(SA11MCM AS BIGINT),0)				
					+ COALESCE(CAST(SA12MCM AS BIGINT),0)
			) InstructionalStaffTotal

	, COALESCE(CAST(SA_9MCM AS BIGINT),0) InstructionalStaffTotal_LessThan9MonthContract
	, COALESCE(CAST(SA09MCM AS BIGINT),0) InstructionalStaffTotal_9MonthContract
	, COALESCE(CAST(SA10MCM AS BIGINT),0) InstructionalStaffTotal_10MonthContract
	, COALESCE(CAST(SA11MCM AS BIGINT),0) InstructionalStaffTotal_11MonthContract
	, COALESCE(CAST(SA12MCM AS BIGINT),0) InstructionalStaffTotal_12MonthContract

	, CAST(SAMNTHM AS BIGINT) [NumberMonthsCoveredBySalaryOutlays]

	, COALESCE(	CAST(SAOUTLM AS BIGINT)
				, COALESCE(CAST(SAEQ9OM AS BIGINT),0)
					+ COALESCE(CAST(SA09MOM AS BIGINT),0)
					+ COALESCE(CAST(SA10MOM AS BIGINT),0)
					+ COALESCE(CAST(SA11MOM AS BIGINT),0)
					+ COALESCE(CAST(SA12MOM AS BIGINT),0)
				, 0
			) TotalSalaryOutlays

	--, COALESCE(CAST(SAEQ9OT AS BIGINT),0) SalaryOutlaysForStaffEquatedTo9MonthContract
	, COALESCE(CAST(SA09MOM AS BIGINT),0) SalaryOutlaysForStaff9MonthContract
	, COALESCE(CAST(SA10MOM AS BIGINT),0) SalaryOutlaysForStaff10MonthContract
	, COALESCE(CAST(SA11MOM AS BIGINT),0) SalaryOutlaysForStaff11MonthContract
	, COALESCE(CAST(SA12MOM AS BIGINT),0) SalaryOutlaysForStaff12MonthContract


FROM IPEDS.tblSAL_IS SAL
WHERE ARANK <> 7
	--AND SAL.UNITID = 214777

UNION


SELECT
/*key*/
	cast(SAL.SURVEY_YEAR as int) [AcademicYr]
	, cast(SAL.UNITID as int) UnitID
	, CASE CAST(SAL.ARANK AS SMALLINT)
			WHEN 7 THEN 'All instructional staff total'
			WHEN 1 THEN 'Professor'
			WHEN 2 THEN 'Associate professor'
			WHEN 3 THEN 'Assistant professor'
			WHEN 4 THEN 'Instructor'
			WHEN 5 THEN 'Lecturer'
			WHEN 6 THEN 'No academic rank'
			ELSE 'Other'
		end [AcademicRank]
	, 'F' GenderCd
/*end key*/
	, COALESCE(
				CAST(SAINSTW AS BIGINT)
				, COALESCE(CAST(SA_9MCW AS BIGINT),0)
					+ COALESCE(CAST(SA09MCW AS BIGINT),0)
					+ COALESCE(CAST(SA10MCW AS BIGINT),0)
					+ COALESCE(CAST(SA11MCW AS BIGINT),0)				
					+ COALESCE(CAST(SA12MCW AS BIGINT),0)
			) InstructionalStaffTotal

	, COALESCE(CAST(SA_9MCW AS BIGINT),0) InstructionalStaffTotal_LessThan9MonthContract
	, COALESCE(CAST(SA09MCW AS BIGINT),0) InstructionalStaffTotal_9MonthContract
	, COALESCE(CAST(SA10MCW AS BIGINT),0) InstructionalStaffTotal_10MonthContract
	, COALESCE(CAST(SA11MCW AS BIGINT),0) InstructionalStaffTotal_11MonthContract
	, COALESCE(CAST(SA12MCW AS BIGINT),0) InstructionalStaffTotal_12MonthContract

	, CAST(SAMNTHW AS BIGINT) [NumberMonthsCoveredBySalaryOutlays]

	, COALESCE(	CAST(SAOUTLW AS BIGINT)
				, COALESCE(CAST(SAEQ9OW AS BIGINT),0)
					+ COALESCE(CAST(SA09MOW AS BIGINT),0)
					+ COALESCE(CAST(SA10MOW AS BIGINT),0)
					+ COALESCE(CAST(SA11MOW AS BIGINT),0)
					+ COALESCE(CAST(SA12MOW AS BIGINT),0)
				, 0
			) TotalSalaryOutlays

	--, COALESCE(CAST(SAEQ9OT AS BIGINT),0) SalaryOutlaysForStaffEquatedTo9MonthContract
	, COALESCE(CAST(SA09MOW AS BIGINT),0) SalaryOutlaysForStaff9MonthContract
	, COALESCE(CAST(SA10MOW AS BIGINT),0) SalaryOutlaysForStaff10MonthContract
	, COALESCE(CAST(SA11MOW AS BIGINT),0) SalaryOutlaysForStaff11MonthContract
	, COALESCE(CAST(SA12MOW AS BIGINT),0) SalaryOutlaysForStaff12MonthContract


FROM IPEDS.tblSAL_IS SAL
WHERE ARANK <> 7
	--AND SAL.UNITID = 214777


GO

DROP TABLE IF EXISTS OSDS_RPT.IPEDS.tblFactInstructionalStaffSalaries 
SELECT * INTO OSDS_RPT.IPEDS.tblFactInstructionalStaffSalaries FROM OSDS_ETL.IPEDS.vw_FactInstructionalStaffSalaries
CREATE CLUSTERED COLUMNSTORE INDEX IX_InstrStaffSalaries ON OSDS_RPT.IPEDS.tblFactInstructionalStaffSalaries 
