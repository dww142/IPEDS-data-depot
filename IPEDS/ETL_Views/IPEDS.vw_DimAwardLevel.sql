/*
	Dimension view for the IPEDS Award level lookup values
	referenced by the IPEDS Completions/Awards survey data file

*/
USE OSDS_ETL

drop VIEW IPEDS.vw_DimAwardLevel 
go
CREATE VIEW IPEDS.vw_DimAwardLevel as
	SELECT DISTINCT	

		CAST(CASE 
			WHEN L.LookupCd IN (1,2,3,4,5) THEN 'Undergraduate'
			WHEN L.LookupCd IN (6,7,8) THEN 'Graduate'
			WHEN L.LookupCd IN (9,17,18,19) THEN 'Doctorate'
			WHEN L.LookupCd IN (10,11) THEN 'First Professional'
			else 'Other'
		END AS VARCHAR(50)) [AwardLevelCategory]
		, cast(L.LookupCd as smallint) [AwardLevelPK]
		, CAST(LATEST.LatestDesc AS VARCHAR(100)) [AwardLevelDesc]
	FROM SHARED.tblLookupImport L
		INNER JOIN (
				SELECT ML.LookupName, ML.LookupCd, ML.LookupDesc [LatestDesc], ML.SourceYear
					, RANK() OVER(PARTITION BY ML.LookupName, ML.LookupCd ORDER BY ML.SourceYear DESC) LBLRank
				FROM SHARED.tblLookupImport ML
				WHERE upper(ML.LookupName)=upper('AWLevel')
			) LATEST ON L.LookupName = LATEST.LookupName
						AND L.LookupCd = LATEST.LookupCd
						AND LATEST.LBLRank=1
	WHERE upper(L.LookupName)=upper('AWLevel')

GO

DROP TABLE OSDS_RPT.IPEDS.tblDimAwardLevel
SELECT * INTO OSDS_RPT.IPEDS.tblDimAwardLevel FROM OSDS_ETL.IPEDS.vw_DimAwardLevel 
CREATE CLUSTERED COLUMNSTORE INDEX IX_AwardLevelClusteredColStore ON OSDS_RPT.IPEDS.tblDimAwardLevel
