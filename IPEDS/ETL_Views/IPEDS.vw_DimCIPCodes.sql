/*
	Dimension view for Higher Education CIP Codes
	Referenced by IPEDS Completions and Awards survey (initial - more references to be added)

	CREATES  single record per CIP Code with a unique list of CIP codes used across all available CIP Code sets
	- i.e., if a code exists in 2000 and 2010, it will use the 2010 descriptive values for the code. 
		if it only existed in 2000 - the code will still be included in the final view in the event data sources 
		are still references older code sets
*/
USE OSDS_ETL;

DROP VIEW IPEDS.vw_DimCIPCodes
GO
CREATE VIEW IPEDS.vw_DimCIPCodes AS 
	SELECT DISTINCT	 
		CAST(RANKED.[6-DigitCIPCode] AS VARCHAR(10)) [CIPCodePK]
		, CAST(RANKED.[6-DigitCIPCode] + ' - ' + RANKED.[6-DigitCIPTitle] AS VARCHAR(265)) [6DigitCIPCode]
		, CAST(RANKED.[4-DigitCIPCode] + ' - ' + RANKED.[4-DigitCIPTitle] AS VARCHAR(265)) [4DigitCIPCode]
		, CAST(RANKED.CIPFamily + ' - ' + RANKED.CIPFamilyTitle AS VARCHAR(265)) [2DigitCIPFamily]

		, COALESCE(CAST(STEMSTATUS.STEMStatus2008 AS VARCHAR(50)), 'Non-STEM CIP Code') STEMStatus2008
		, COALESCE(CAST(STEMSTATUS.STEMStatus2011 AS VARCHAR(50)), 'Non-STEM CIP Code') STEMStatus2011
		, COALESCE(CAST(STEMSTATUS.STEMStatus2012 AS VARCHAR(50)), 'Non-STEM CIP Code') STEMStatus2012
		, COALESCE(CAST(STEMSTATUS.STEMStatus2016 AS VARCHAR(50)), 'Non-STEM CIP Code') STEMStatus2016
		, COALESCE(CAST(STEMSTATUS.STEMStatus2008 AS VARCHAR(50)) 
					, CAST(STEMSTATUS.STEMStatus2011 AS VARCHAR(50)) 
					, CAST(STEMSTATUS.STEMStatus2012 AS VARCHAR(50)) 
					, CAST(STEMSTATUS.STEMStatus2016 AS VARCHAR(50)) 
					, 'Non-STEM CIP Code'
				) [STEMStatusAnytime]

		-- , RANKED.CIPEdition
		-- , RANKED.[6-DigitCIPCode] [6DigitCIPCode]
		-- , RANKED.[6-DigitCIPTitle] [6DigitCIPTitle]
		-- , RANKED.[6-DigitCIPDefinition] [6DigitCIPDefinition]
		-- , RANKED.[4-DigitCIPCode] [4DigitCIPCode]
		-- , RANKED.[4-DigitCIPTitle] [4DigitCIPTitle]
		-- , RANKED.[4-DigitCIPDefinition] [4DigitCIPDefinition]
		-- , RANKED.CIPFamily
		-- , RANKED.CIPFamilyTitle
		-- , RANKED.CIPFamilyDescription

	FROM (
			SELECT * , RANK() OVER(PARTITION BY [6-DigitCIPCode] ORDER BY CIPEdition DESC) CIP_EDITION_RANK
			FROM (
				SELECT distinct 
						C2.CIPEdition
						, C2.CIPFamily
						, C4.[4-DigitCIPCode]
						, C6.[6-DigitCIPCode]
		
						, C2.CIPFamilyTitle
						, C2.CIPFamilyDescription
		
						, C4.[4-DigitCIPTitle]
						, C4.[4-DigitCIPDefinition]
		
						, C6.[6-DigitCIPTitle]
						, C6.[6-DigitCIPDefinition]

				FROM
						(SELECT distinct 2000 as CIPEdition
								, c2.CIPFAMILY [CIPFamily]
								, c2.CIPTITLE [CIPFamilyTitle]
								, c2.CIPDESCR [CIPFamilyDescription]
								, RANK() OVER(PARTITION BY C2.CIPFAMILY ORDER BY ID DESC) CODE_RANK
						FROM IPEDS.tblCIP2000 c2
						where c2.CIPFAMILY = left(c2.CIPCode,2)
							and (len(c2.CIPCode) < 5 or CIPCode in ('99','98'))
							and (c2.ACTIONCODE is null or c2.ACTIONCODE <> 'D')
							and (c2.CIPDESCR is null or c2.CIPDESCR not like '%Moved, Report%')
							) 
					C2 INNER JOIN 
						(SELECT distinct 2000 as CIPEdition
								, c4.CIPFAMILY [CIPFamily]
								, c4.CIPCode [4-DigitCIPCode]
								, c4.CIPTITLE [4-DigitCIPTitle]
								, c4.CIPDESCR [4-DigitCIPDefinition]
						FROM IPEDS.tblCIP2000 c4
						where c4.CIPFAMILY = left(c4.CIPCode,2)
							and (len(c4.CIPCode) =5 or CIPCode in ('99','98'))
							and (c4.ACTIONCODE is null or c4.ACTIONCODE <> 'D')
							and (c4.CIPDESCR is null or c4.CIPDESCR not like '%Moved, Report%')
						) 
					C4 ON C2.CIPEdition = C4.CIPEdition AND C2.CIPFamily = C4.CIPFamily
							AND C2.CODE_RANK = 1 /*DEDUPLICATE CIP Family (higher ID # wins)*/
					INNER JOIN 
						(SELECT distinct 2000 as CIPEdition
								, c6.CIPFAMILY [CIPFamily]
								, left(c6.CIPCode,5) [4-DigitCIPCode]
								, c6.CIPCode [6-DigitCIPCode]
								, c6.CIPTITLE [6-DigitCIPTitle]
								, c6.CIPDESCR [6-DigitCIPDefinition]
						FROM IPEDS.tblCIP2000 c6
						where c6.CIPFAMILY = left(c6.CIPCode,2)
							and (len(c6.CIPCode) =7 or CIPCode in ('99','98'))
							and (c6.ACTIONCODE is null or c6.ACTIONCODE <> 'D')
							and (c6.CIPDESCR is null or c6.CIPDESCR not like '%Moved, Report%')
						) C6 ON C4.CIPEdition = C6.CIPEdition AND C4.[4-DigitCIPCode] = C6.[4-DigitCIPCode]


				union

				SELECT distinct 		
						C2.CIPEdition
						, C2.CIPFamily
						, C4.[4-DigitCIPCode]
						, C6.[6-DigitCIPCode]
		
						, C2.CIPFamilyTitle
						, C2.[CIPFamilyDefinition] [CIPFamilyDescription]
		
						, C4.[4-DigitCIPTitle]
						, C4.[4-DigitCIPDefinition]
		
						, C6.[6-DigitCIPTitle]
						, C6.[6-DigitCIPDefinition]

				FROM
					(SELECT 2010 AS CIPEdition, c2.CIPFamily, c2.CIPTitle [CIPFamilyTitle], c2.CIPDefinition [CIPFamilyDefinition]
					FROM IPEDS.tblCIP2010 c2
					WHERE c2.Action <>'Deleted' and c2.Action not like '%Moved from%' and (len(c2.CIPCode) < 5 or CIPCode in ('99','98'))
					) C2 INNER JOIN 
					(SELECT 2010 AS CIPEdition, c4.CIPFamily, c4.CIPCode [4-DigitCIPCode], c4.CIPTitle [4-DigitCIPTitle], c4.CIPDefinition [4-DigitCIPDefinition]
					FROM IPEDS.tblCIP2010 c4
					WHERE c4.Action <>'Deleted' and c4.Action not like '%Moved from%' and (len(c4.CIPCode) =5 or CIPCode in ('99','98'))
					) C4 ON C2.CIPEdition = C4.CIPEdition AND C2.CIPFamily = C4.CIPFamily
					inner join 
					(SELECT 2010 AS CIPEdition, c6.CIPFamily, LEFT(c6.CIPCode,5) [4-DigitCIPCode], c6.CIPCode [6-DigitCIPCode],c6.CIPTitle [6-DigitCIPTitle], c6.CIPDefinition [6-DigitCIPDefinition]
					FROM IPEDS.tblCIP2010 c6
					WHERE c6.Action <>'Deleted' and c6.Action not like '%Moved from%' and (len(c6.CIPCode) =7 or CIPCode in ('99','98'))
					) C6 ON c4.CIPFamily = c6.CIPFamily and c4.[4-DigitCIPCode] = c6.[4-DigitCIPCode]

				 union
				 SELECT '-1', '-1', '-1', '-1', 'N/A','N/A','N/A','N/A','N/A','N/A'
				 UNION
				 SELECT '-2', '-2', '-2', '-2', 'N/A','N/A','N/A','N/A','N/A','N/A'
				 UNION 
				 SELECT '98', '98', '98', '98', 'Unknown','Unknown','Unknown','Unknown','Unknown','Unknown'
				) BASE
		) RANKED
		LEFT JOIN (
				SELECT 
					LTRIM(RTRIM(STEM.CIPCode)) CIPCode
					, max(CASE WHEN YEAR(STEM.STEMDesignationDate) = 2008 THEN 'STEM CIP Code' ELSE NULL END) [STEMStatus2008]
					, max(CASE WHEN YEAR(STEM.STEMDesignationDate) = 2011 THEN 'STEM CIP Code' ELSE NULL END) [STEMStatus2011]
					, max(CASE WHEN YEAR(STEM.STEMDesignationDate) = 2012 THEN 'STEM CIP Code' ELSE NULL END) [STEMStatus2012]
					, max(CASE WHEN YEAR(STEM.STEMDesignationDate) = 2016 THEN 'STEM CIP Code' ELSE NULL END) [STEMStatus2016]
				FROM IPEDS.tblSTEMCIPCodeDesignationImport STEM
				GROUP BY STEM.CIPCode
		) STEMSTATUS ON RANKED.[6-DigitCIPCode] = STEMSTATUS.CIPCode
	WHERE RANKED.CIP_EDITION_RANK=1

GO

    DROP TABLE OSDS_RPT.IPEDS.tblDimCIPCodes
	SELECT * INTO OSDS_RPT.IPEDS.tblDimCIPCodes FROM OSDS_ETL.IPEDS.vw_DimCIPCodes 
	CREATE CLUSTERED COLUMNSTORE INDEX IX_CIPCodeClusteredColStore ON OSDS_RPT.IPEDS.tblDimCIPCodes
