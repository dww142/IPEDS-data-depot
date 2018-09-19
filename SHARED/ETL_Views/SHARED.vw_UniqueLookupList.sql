/****************************************
	Create a unique list of Lookup code-description
		combinations, with the latest available description for each code. 

	other views are dependent on this view, it is used in the view/ETL 
        layer only, this table does not exist in the RPT database;

    this should probably get materialized into a physical table and indexed
        to speed up view performance

****************************************/

DROP VIEW IF EXISTS SHARED.vw_UniqueLookupList 
GO
CREATE VIEW SHARED.vw_UniqueLookupList as
SELECT
	L.Source
	, L.SourceYear
	, L.LookupName
	, L.LookupCd
	, replace(REPLACE(L.LookupDesc,'{',''),'}','') LookupDesc
	, L.LookupCategory1
	, L.LookupCategory2
	, L.LookupCategory3

FROM SHARED.tblLookupImport L
	CROSS APPLY (/*Gets latest description for each lookup code*/
				SELECT LATEST.LookupName, LATEST.LookupCd, MAX(LATEST.SourceYear) [LATEST_YEAR]
				FROM SHARED.tblLookupImport LATEST
				WHERE LATEST.LookupName = L.LookupName
						AND LATEST.LookupCd = L.LookupCd
				GROUP BY LATEST.LookupName, LATEST.LookupCd
				HAVING MAX(LATEST.SourceYear) = L.SourceYear
		) LATEST 
