/*
	Annualized Fact data from the Institution Characteristics IC survey on cost data
    
    (114408 rows affected) 
	Total execution time: 00:00:18.516
*/
USE OSDS_ETL;
GO
DROP VIEW IPEDS.vw_FactInstitutionCharacteristics
GO
CREATE VIEW IPEDS.vw_FactInstitutionCharacteristics AS
SELECT 
	cast(IC.SURVEY_YEAR as int) + 1 [AcademicYr]
	, CAST(IC.UNITID AS INT) UnitID
	, I.StateFIPSCd
	, I.CountyFIPSCd

	, CAST(IC.ROOMAMT AS BIGINT) [RoomCost]
	, CAST(IC.BOARDAMT AS BIGINT) [BoardCost]

	, CAST(IC.APPLFEEU AS BIGINT) [UndergraduateApplicationFee]
	, CAST(IC.APPLFEEG AS BIGINT) [GraduateApplicationFee]
	, CASE cast(SLO8 as varchar(100)) WHEN '1' THEN 'Offers Teacher Certification Degree' ELSE 'Does Not Offer Teacher Certification Degree' end [OffersTeacherCertificationDegree]
	, CASE cast(CREDITS1 as varchar(100)) WHEN '1' THEN 'Accepts Dual Credits' ELSE 'Does Not Accept Dual Credits' END [DualCredit]
	, CASE cast(CREDITS2 as varchar(100))  WHEN '1' THEN 'Offers Credit for Life Experiences' ELSE 'Does Not Offer Credit For Life Experiences' END [Credit for life experiences]
	, CASE cast(CREDITS3 as varchar(100)) WHEN '1' THEN 'Accepts Advanced Placement Credits' ELSE 'Does Not Accept Advanced Placement Credits' END [AcceptsAdvancedPlacementCredits]
	, CAST(CALSYS.LookupDesc AS VARCHAR(100)) [CalendarSystem]

	, CAST(ICAY.TUITION1 AS BIGINT) [In-DistrictAverageTuitionForFull-TimeUndergraduates]
	, CAST(ICAY.FEE1 AS BIGINT) [In-DistrictRequiredFeesForFull-TimeUndergraduates]
	, CAST(ICAY.HRCHG1 AS BIGINT) [In-DistrictPerCreditHourChargeForPart-TimeUndergraduates]

	, CAST(ICAY.TUITION2 AS BIGINT) [In-StateAverageTuitionForFull-TimeUndergraduates]
	, CAST(ICAY.FEE2 AS BIGINT) [In-StateRequiredFeesForFull-TimeUndergraduates]
	, CAST(ICAY.HRCHG2 AS BIGINT) [In-StatePerCreditHourChargeForPart-TimeUndergraduates]

	, CAST(ICAY.TUITION3 AS BIGINT) [Out-Of-StateAverageTuitionForFull-TimeUndergraduates]
	, CAST(ICAY.FEE3 AS BIGINT) [Out-Of-StateRequiredFeesForFull-TimeUndergraduates]
	, CAST(ICAY.HRCHG3 AS BIGINT) [Out-Of-StatePerCreditHourChargeForPart-TimeUndergraduates]

	, CAST(ICAY.CHG1AT3 AS BIGINT) [PublishedIn-DistrictTuition]
	, CAST(ICAY.CHG1AF3 AS BIGINT) [PublishedIn-DistrictFees]
	, CAST(ICAY.CHG2AT3 AS BIGINT) [PublishedIn-StateTuition]
	, CAST(ICAY.CHG2AF3 AS BIGINT) [PublishedIn-StateFees]
	, CAST(ICAY.CHG3AT3 AS BIGINT) [PublishedOut-Of-StateTuition]
	, CAST(ICAY.CHG3AF3 AS BIGINT) [PublishedOut-Of-StateFees]

	, CAST(ICAY.CHG4AY3 AS BIGINT) [BooksAndSupplies]
	, CAST(ICAY.CHG5AY3 AS BIGINT) [OnCampusRoomAndBoard]
	, CAST(ICAY.CHG6AY3 AS BIGINT) [OnCampusOtherExpenses]
	, CAST(ICAY.CHG7AY3 AS BIGINT) [OffCampusRoomAndBoard(not with family)]
	, CAST(ICAY.CHG8AY3 AS BIGINT) [OffCampusOtherExpenses(not with family)]

	--, ICPY.CIPCODE1
	--, CIP.[2DigitCIPFamily] LargestProgram2DigitCIPFamily
	--, CIP.[4DigitCIPCode] LargestProgram4dDigitCIPCode
	--, CIP.[6DigitCIPCode] LargestProgram6DigitCIPCode
	--, CIP.STEMStatusAnytime

FROM IPEDS.tblIC IC
	INNER JOIN IPEDS.vw_DimInstitution I ON IC.UNITID = I.UnitID 
	LEFT JOIN SHARED.vw_UniqueLookupList CALSYS ON COALESCE(IC.CALSYS,'-2') = CALSYS.LookupCd AND UPPER(CALSYS.LookupName)='CALSYS'
	LEFT JOIN IPEDS.tblIC_AY ICAY ON IC.UNITID = ICAY.UNITID AND IC.SURVEY_YEAR = ICAY.SURVEY_YEAR
	LEFT JOIN IPEDS.tblIC_PY ICPY ON IC.UNITID = ICPY.UNITID AND IC.SURVEY_YEAR = ICPY.SURVEY_YEAR
	--LEFT JOIN IPEDS.vw_DimCIPCodes CIP ON ICPY.CIPCODE1 = CIP.CIPCodePK

--WHERE IC.UNITID IN (214777, 212878, 216010) --PSU, HACC, SHIPP
	
--ORDER BY IC.UNITID, IC.SURVEY_YEAR
GO

drop table OSDS_RPT.IPEDS.tblFactInstitutionCharacteristics
SELECT * INTO OSDS_RPT.IPEDS.tblFactInstitutionCharacteristics FROM OSDS_ETL.IPEDS.vw_FactInstitutionCharacteristics
CREATE CLUSTERED COLUMNSTORE INDEX IX_IC_FactColStore ON OSDS_RPT.IPEDS.tblFactInstitutionCharacteristics


