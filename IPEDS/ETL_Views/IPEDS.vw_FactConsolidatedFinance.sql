/*************************************

	Fact view of the consolidated IPEDS Finance Surveys
	Aligns common data elements across the 3 surveys collecting finance data
	using different accounting standards. 


	REVENUE only as of 2018-10-9
	- [TotalAllRevenuesAndOtherAdditions_Reported] - this is total revenue as reported to IPEDS
		and downloaded from the file; prior to 2009, the sum of all sources of revenue is less than reported
		total revenue; from 2010 onward (GASB) and 2008 onward (PFP/FASB) the totals match

(95314 rows affected) 
Total execution time: 00:00:07.798

*************************************/

USE OSDS_ETL;
GO
DROP VIEW IPEDS.vw_FactConsolidatedFinance
GO
CREATE VIEW IPEDS.vw_FactConsolidatedFinance AS
	SELECT 
		CAST(I.UNITID AS INT) [UnitID]
		, cast(COALESCE(F1.SURVEY_YEAR, F2.SURVEY_YEAR, F3.SURVEY_YEAR) as int) [AcademicYr]

		, I.StateFIPSCd
		, I.[CountyFIPSCd]  

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

	/*REVENUES*/
		, CAST(COALESCE(F1B01, F2D01, F3D01) AS BIGINT) [Tuition and Fees]
		, CAST(COALESCE(
			CAST(F1B02 AS BIGINT) + CAST(F1B13 AS BIGINT) /*GASB fed GRANTS & CONTRAOCTS OPERATING + NONOPERATING*/
			, F2D05, F3D02B) AS BIGINT) [FederalGrantsAndContracts]
		, CAST(COALESCE(
			CAST(F1B03 AS BIGINT) + CAST(F1B14 AS BIGINT) /*GASB state GRANTS & CONTRAOCTS OPERATING + NONOPERATING*/
			, F2D06, F3D03B) AS BIGINT) [StateGrantsAndContracts]
		, CAST(COALESCE(
			CAST(F1B04A AS BIGINT) + CAST(F1B15 AS BIGINT) /*GASB local GRANTS & CONTRAOCTS OPERATING + NONOPERATING*/
			, F2D07, F3D03D) AS BIGINT) [LocalGrantsAndContracts]

		, CAST(COALESCE(
			CAST(F1B04B AS BIGINT) + CAST(F1B16 AS BIGINT) /*GASB - Private Grants, Contracts, Gifts*/
			, CAST(F2D08B AS BIGINT) + CAST(F2D08A AS BIGINT) + CAST(F2D09 AS BIGINT) /*FASB - Private Grants, Contracts, Gifts, & Contributions from affiliated entities*/
			, F3D04) AS BIGINT) [PrivateGrants-Contracts-Gifts-ContributionsFromAffiliatedEntities]		

		, CAST(COALESCE(F1B05, F2D11, F3D07) AS BIGINT)	SalesAndServicesOfAuxiliaryEnterprises
		, CAST(COALESCE(F1B06, F2D13, F3D12) AS BIGINT)	HospitalRevenue
		, CAST(COALESCE(F1B26, F2D12, F3D06) AS BIGINT)	SalesAndServicesOfEducationalActivities
		, CAST(COALESCE(F1B07, F2D14) AS BIGINT) IndependentOperations
		, CAST(COALESCE(F1B10, F2D02, F3D02A) AS BIGINT) FederalAppropriations
		, CAST(COALESCE(F1B11, F2D03, F3D03A) AS BIGINT) StateAppropriations
		, CAST(COALESCE(F1B12, F2D04, F3D03C) AS BIGINT) LocalAppropriations
		, CAST(COALESCE(F1B17, F2D10, F3D05) AS BIGINT) InvestmentReturn
		, CAST(COALESCE(F1B20,0) AS BIGINT) CapitalAppropriations
		, CAST(COALESCE(F1B21,0) AS BIGINT) CapitalGrantsAndGifts
		, CAST(COALESCE(F1B22,0) AS BIGINT) AdditionsToPermanentEndowments
		, CAST(COALESCE(
			CAST(F1B23 AS BIGINT) + CAST(F1B18 AS BIGINT) + CAST(F1B08 AS BIGINT) /*GASB Other revenue & sources - operating and non-operating combined*/
			, F2D15, F3D08) AS BIGINT)
			[OtherRevenueSources]
		--, CAST(F2D17 AS BIGINT) [FASB_NetAssetsReleasedFromRestriction]
		, CAST(COALESCE(F1B25, F2D16, F3D09) AS BIGINT) [TotalAllRevenuesAndOtherAdditions_Reported]

/*EXPENSES*/
        , cast(COALESCE(F1C011, F2E011, F3E011)  as bigint) [InstructionExpenses]
        , cast(COALESCE(F1C012, F2E012, F3E012)  as bigint) [InstructionSalariesAndWages]
        , cast(COALESCE(F1C021, F2E021, F3E02A1)  as bigint) [ResearchExpenses]
        , cast(COALESCE(F1C022, F2E022, F3E02A2)  as bigint) [ResearchSalariesAndWages]
        , cast(COALESCE(F1C031, F2E031, F3E02B1)  as bigint) [PublicServiceExpenses]
        , cast(COALESCE(F1C032, F2E032, F3E02B2)  as bigint) [PublicServiceSalariesAndWages]
        , cast(COALESCE(F1C051, F2E041, F3E03A1)  as bigint) [AcademicSupportExpenses]
        , cast(COALESCE(F1C052, F2E042, F3E03A2)  as bigint) [AcademicSupportSalariesAndWages]
        , cast(COALESCE(F1C061, F2E051, F3E03B1)  as bigint) [StudentServiceExpenses]
        , cast(COALESCE(F1C062, F2E052, F3E03B2)  as bigint) [StudentServiceSalariesAndWages]
        , cast(COALESCE(F1C071, F2E061, F3E03C1)  as bigint) [InstitutionalSupportExpenses]
        , cast(COALESCE(F1C072, F2E062, F3E03C2)  as bigint) [InstitutionalSupportSalariesAndWages]
        , cast(COALESCE(F1C111, F2E071, F3E041)  as bigint) [AuxiliaryEnterprisesExpenses]
        , cast(COALESCE(F1C112, F2E072, F3E042)  as bigint) [AuxiliaryEnterprisesSalariesAndWages]
        , cast(COALESCE(F1C121, F2E091, F3E101)  as bigint) [HospitalServicesExpenses]
        , cast(COALESCE(F1C122, F2E092, F3E102)  as bigint) [HospitalServicesSalariesAndWages]
        , cast(COALESCE(F1C131, F2E101)  as bigint) [IndependentOperationsExpenses]
        , cast(COALESCE(F1C132, F2E102)  as bigint) [IndependentOperationsSalariesAndWages]
        , cast(COALESCE(F1C141, F2E121, F3E061)  as bigint) [OtherExpenses]
        , cast(COALESCE(F1C142, F2E122, F3E062)  as bigint) [OtherSalariesAndWages]

        , cast(COALESCE(F1C191, F2E131, F3E071)  as bigint) [TotalExpenses_Reported]
        , cast(COALESCE(F1C192, F2E132, F3E072)  as bigint) [TotalSalariesAndWages_Reported]
        , cast(COALESCE(F1C193, F2E133, F3E073)  as bigint) [TotalBenefitsExpenses]
        , cast(COALESCE(F1C19OM, F2E134, F3E074)  as bigint) [TotalOperationsAndMaintenanceExpenses]
        , cast(COALESCE(F1C19DP, F2E135, F3E075)  as bigint) [TotalDepreciation]
        , cast(COALESCE(F1C19IN, F2E136, F3E076)  as bigint) [TotalInterestExpenses]
        , cast(COALESCE(F1C19OT, F2E137, F3E077)  as bigint) [TotalOtherExpenses]

        /*not in total expenses*/
            , cast(F1C101  as bigint) [ScholarshipAndFellowshipExpenses] /*GASB*/
            , cast(COALESCE(F2E081, F3E051)  as bigint) [NetGrantAidToStudentsExpenses] /*FASB-PFP*/

            , cast(F3F01  as bigint) [FederalIncomeTaxExpenses] /*PFP Only*/
            , cast(F3F02  as bigint) [StateAndLocalIncomeTaxExpenses] /*PFP Only*/
            , F3F03  [TaxPayingDesignee_PFPOnly] /*PFP Only*/



FROM IPEDS.vw_DimInstitution I
/*outer join all 3 finance surveys; coalesce values into a single consistent structure for comparisons*/
		LEFT JOIN IPEDS.tblF1_GASB F1 ON I.UnitID = F1.UNITID
		LEFT JOIN IPEDS.tblF2_FASB F2 ON I.UnitID = F2.UNITID
		LEFT JOIN IPEDS.tblF3_PFP F3 ON I.UnitID = F3.UNITID 


WHERE COALESCE(F1.UNITID, F2.UNITID, F3.UNITID) IS NOT NULL
	-- AND I.UNITID IN (214777, 216010, 215062, 210483) --(PSU, SHIPP, UPENN, KEYSTONE TECH - FASB, GASB, FASB, PFP)
	--ORDER BY 2, 1 DESC
GO


DROP TABLE OSDS_RPT.IPEDS.tblFactConsolidatedFinance
SELECT * INTO OSDS_RPT.IPEDS.tblFactConsolidatedFinance FROM OSDS_ETL.IPEDS.vw_FactConsolidatedFinance 
CREATE CLUSTERED COLUMNSTORE INDEX IX_ConsolidatedFinance_Colstore ON OSDS_RPT.IPEDS.tblFactConsolidatedFinance 



