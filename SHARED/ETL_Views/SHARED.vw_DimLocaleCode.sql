/*
	Insittuiton locale / metro status
		referenced by institution dimension & and other organization dimensions
		that include a metro status or locale

*/

DROP VIEW IF EXISTS SHARED.vw_DimLocaleCode
GO

CREATE VIEW SHARED.vw_DimLocaleCode AS

	SELECT DISTINCT	
		cast(L.LookupCd as smallint) LocalePK
		, LEFT(replace(REPLACE(L.LookupDesc,'{',''),'}',''),40) LocaleDesc

		, LEFT(CASE 
			WHEN CAST(L.LookupCd AS SMALLINT) in (1,2,11,12,13) then 'Urban'
			WHEN CAST(L.LookupCd as smallint) in (3,4,21,22,23) then 'Suburban'
			WHEN CAST(L.LookupCd as smallint) in (5,6,7,8,31,32,33,41,42,43) then 'Rural'
			ELSE replace(REPLACE(L.LookupDesc, '{', ''), '}', '')
		END,20) [UrbanRuralStatus]
	FROM SHARED.tblLookupImport L
		CROSS APPLY (/*Gets latest description for each lookup code*/
					SELECT LI.LookupName, LI.LookupCd, MAX(LI.SourceYear) [LATEST_YEAR]
					FROM SHARED.tblLookupImport LI
					WHERE LI.LookupName = L.LookupName
							AND LI.LookupCd = L.LookupCd
					GROUP BY LI.LookupName, LI.LookupCd
					HAVING MAX(LI.SourceYear) = L.SourceYear
			) LATEST 
	WHERE upper(L.LookupName) = upper('Locale')

go


    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimLocaleCode
    SELECT * INTO OSDS_RPT.SHARED.tblDimLocaleCode FROM OSDS_ETL.SHARED.vw_DimLocaleCode
    CREATE CLUSTERED COLUMNSTORE INDEX IX_LOCALE ON  OSDS_RPT.SHARED.tblDimLocaleCode