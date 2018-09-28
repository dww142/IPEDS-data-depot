/*************************************

	Fact view of the consolidated IPEDS Finance Surveys
	Aligns common data elements across the 3 surveys collecting finance data
	using different accounting standards. 

	Field names are copied from dictionaries, need to be standardized yet

last run: 
	(95314 rows affected) 
	Total execution time: 00:00:14.186
*************************************/

USE OSDS_ETL;
GO
DROP VIEW if EXISTS IPEDS.vw_FactConsolidatedFinance
GO
CREATE VIEW IPEDS.vw_FactConsolidatedFinance AS
	SELECT 
		CAST(I.UNITID AS INT) [UnitID]
		, cast(COALESCE(F1.SURVEY_YEAR, F2.SURVEY_YEAR, F3.SURVEY_YEAR) as int) [AcademicYr]

		, I.StateFIPSCd
		, I.[CountyFIPSCd]  /*Unknown county AND unknown state */

		, CASE 
			WHEN F1.UNITID IS NOT NULL THEN 'GASB'
			WHEN F2.UNITID IS NOT NULL THEN 'FASB'
			WHEN F3.UNITID IS NOT NULL THEN 'PFP'
			ELSE 'NA'
		END [FinanceReportingSurvey]
	--, cast(COALESCE(F1FHA, F2FHA) as SMALLINT) [Does this institution or any of its foundations or other affiliated organizations own endowment assets ?]
	, CASE cast(COALESCE(F1FHA, F2FHA,'-2') as SMALLINT) 
		WHEN 1 THEN 'Owns Endowment Assets'
		WHEN 2 THEN 'No Endowment Assets'
		ELSE 'Not Applicable'
		END [OwnsEndowmentAssets]

	/*Finance consolidation - if GASB null, use FASB, if FASB null use PFP*/
		, cast(COALESCE(F1A06, F2A02, F3A01) as bigint) [TotalAssets]

	/*REVENUES*/
		, cast(COALESCE(F1D01, F2D16, F3D09) as bigint) [TotalRevenuesAndInvestmentReturn] /*F3B01 - SELF REPORTED; F3D09 - CALCULATED TOTAL*/
		, cast(COALESCE(F1B10, F2D02, F3D02A) as bigint) [FederalAppropriations]
		, cast(COALESCE(F1B11, F2D03, F3D03A) as bigint) [StateAppropriations]
		, cast(COALESCE(F1B12, F2D04, F3D03C) as bigint) [LocalGovernmentAppropriations]
		, cast(COALESCE(F1B01, F2D01, F3D01) as bigint) [TuitionAndFees]
		, cast(COALESCE(F1B02, F2D05, F3D02B) as bigint) [FederalGrantsAndContracts]
		, cast(COALESCE(F1E03, F2C03, F3C03A) as bigint) [StateGrants]
		, cast(COALESCE(F1E02, F2C02, F3C02) as bigint) [OtherFederalGrants]
		, cast(COALESCE(F1E01, F2C01, F3C01) as bigint) [PellGrants]
		, cast(COALESCE(F1B26, F2D11, F3D06) as bigint) [SalesAndServicesOfEducationalActivities]
		, cast(COALESCE(F1B05, F2D12, F3D07) as bigint) [SalesAndServicesOfAuxiliaryEnterprises]

	/*EXPENSES*/
		, cast(COALESCE(F1C191, F2B02, F3B02) as bigint) [TotalExpenses]
		, cast(COALESCE(F1E08, F2C08, F3C06) as bigint) [AllowancesAppliedToTuitionAndFees]
		, cast(COALESCE(F1C051, F2E041, F3E03A1) as bigint) [AcademicSupportTotalAmount]
		, cast(COALESCE(F1C111, F2E071, F3E041) as bigint) [AuxiliaryEnterprisesTotalAmount]
		, cast(COALESCE(F1C011, F2E011, F3E011) as bigint) [InstructionTotalAmount]
		, cast(COALESCE(F1C071, F2E061, F3E03C1) as bigint) [InstitutionalSupportTotalAmount]
		, cast(COALESCE(F1C121, F2E091, F3E101) as bigint) [HospitalServicesTotalAmount]
		, cast(COALESCE(F1C031, F2E031, F3E02B1) as bigint) [PublicServiceTotalAmount]
		, cast(COALESCE(F1C021, F2E021, F3E02A1) as bigint) [ResearchTotalAmount]
		, cast(COALESCE(F1C061, F2E051, F3E03B1) as bigint) [StudentServiceTotalAmount]
		, cast(COALESCE(F1C081, F2E111, F3E111) as bigint) [OperationAndMaintenanceOfPlantTotalAmount]
		, cast(COALESCE(F1C141, F2E121, F3E061) as bigint) [OtherExpensesTotalAmount]
		, cast(COALESCE(F1C192, F2E132, F3E072) as bigint) [TotalExpenses-Salaries and wages]
		, cast(COALESCE(F1C193, F2E133, F3E073) as bigint) [TotalExpensesBenefits]
		, cast(COALESCE(F1C194, F2E135, F3E075) as bigint) [TotalExpensesDepreciation]
		, cast(COALESCE(F1C197, F2E136, F3E076) as bigint) [TotalExpensesInterest]
		, cast(COALESCE(F1C195, F2E137, F3E077) as bigint) [TotalExpensesAllOther]


		, cast(COALESCE(F1A13, F2A03, F3A02) as bigint) [TotalLiabilities]

		, cast(COALESCE(F1H01, F2H01) as bigint) [EndowmentValue_BeginningOfYear]
		, cast(COALESCE(F1H02, F2H02) as bigint) [EndowmentValue_EndOfYear]


	FROM IPEDS.vw_DimInstitution I
/*outer join all 3 finance surveys; coalesce values into a single consistent structure for comparisons*/
		LEFT JOIN IPEDS.tblF1_GASB F1 ON I.UnitID = F1.UNITID
		LEFT JOIN IPEDS.tblF2_FASB F2 ON I.UnitID = F2.UNITID
		LEFT JOIN IPEDS.tblF3_PFP F3 ON I.UnitID = F3.UNITID 


WHERE COALESCE(F1.UNITID, F2.UNITID, F3.UNITID) IS NOT NULL
	--AND I.UNITID IN (214777, 216010, 215062, 210483) --(PSU, SHIPP, UPENN, KEYSTONE TECH - FASB, GASB, FASB, PFP)
	--ORDER BY 2, 1 DESC

go

DROP TABLE if EXISTS OSDS_RPT.IPEDS.tblFactConsolidatedFinance
SELECT * INTO OSDS_RPT.IPEDS.tblFactConsolidatedFinance FROM OSDS_ETL.IPEDS.vw_FactConsolidatedFinance 
CREATE CLUSTERED COLUMNSTORE INDEX IX_ConsolidatedFinance_Colstore ON OSDS_RPT.IPEDS.tblFactConsolidatedFinance 

