/**************************************************************************************************
	view to produce the US State dimension for the SLDS Data marts
	Includes calculated fields identifying PA border states and other state attributes

	Base Table : SHARED.tblStateImport 
**************************************************************************************************/

DROP VIEW IF EXISTS SHARED.vw_DimState 
GO
CREATE VIEW SHARED.vw_DimState AS
	SELECT DISTINCT
		S.StateFIPSCode as [StateFIPSCd]
		, S.StatePostalCode as [StatePostalCd]
		, S.StateName
		, cast(CASE WHEN S.StatePostalCode IN ('DE','MD','NJ','NY','OH','WV','PA') 
				THEN 'PA Border State (Or PA)' ELSE 'Non-PA Border State' END as varchar(25)) [PABorderStatus]

		, cast(CASE WHEN S.StatePostalCode IN ('AL','AZ','AR','CA','CO','CT','DE','FL','GA','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY')
				THEN 'Contiguous 48 States' ELSE 'Non-Contiguous State or Territory' END as varchar(40)) [Contiguous48Status]

		, cast(case when s.StatePostalCode in ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY')
				THEN 'U.S. State or D.C.' ELSE 'Territory or other Non-State' END AS VARCHAR(40)) [StateOrDCStatus]

	FROM SHARED.tblStateImport S
	WHERE S.StateFIPSCode NOT IN ('57','98','90')

GO


 /*
    ETL into the RPT Database
 */
    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimState 
    SELECT * INTO OSDS_RPT.SHARED.tblDimState  FROM OSDS_ETL.SHARED.vw_DimState 
    CREATE CLUSTERED COLUMNSTORE INDEX IX_State_Colstore ON OSDS_RPT.SHARED.tblDimState 


