/*********************
	Gender Dimension code table
**********************/

DROP VIEW IF EXISTS SHARED.vw_DimGenderCode
GO
CREATE VIEW SHARED.vw_DimGenderCode AS
	SELECT DISTINCT 
			L.LookupCd [GenderCd]
			, LEFT(L.LookupDesc,100) [GenderDesc]
	FROM [SHARED].[tblLookupImport] L
	INNER JOIN (
					SELECT MAX(LL.SourceYear) LATEST_LKP_SY, LL.LookupCd 
					FROM [SHARED].[tblLookupImport] LL 
					WHERE UPPER(LL.LookupName) =  'GENDER CODE' 
					GROUP BY LL.LookupCd
				) LATEST ON L.SourceYear = LATEST.LATEST_LKP_SY AND LATEST.LookupCd = L.LookupCd
		WHERE UPPER(L.LookupName) =  'GENDER CODE'

GO

 /*
    ETL into the RPT Database
 */
    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimGenderCode
    SELECT * INTO OSDS_RPT.SHARED.tblDimGenderCode FROM OSDS_ETL.SHARED.vw_DimGenderCode
    CREATE CLUSTERED COLUMNSTORE INDEX IX_Gender_Colstore ON OSDS_RPT.SHARED.tblDimGenderCode