/*
	Transform the EDGE geocoded data on Postsecondary Schools
	from https://nces.ed.gov/programs/edge/Geographic/SchoolLocations
	into a more useful view; to be combined with institution data in the 
	institution dimension
*/
USE OSDS_ETL;

GO
DROP VIEW IPEDS.vw_GeocodedInstitutions
GO
CREATE VIEW IPEDS.vw_GeocodedInstitutions AS
WITH BASE AS (
	SELECT 
		CAST(GEO.UNITID AS INT) UnitID
		, CAST(GEO.SURVYEAR AS INT) + 1 AcademicYear
		, RANK() OVER(PARTITION BY GEO.UNITID ORDER BY GEO.SURVYEAR DESC) AcademicYearDescOrder
		, GEO.LAT Latitude
		, GEO.LON Longitude

		, CAST(GEO.LOCALE AS SMALLINT) LocaleFK
		, CAST(GEO.CNTY AS VARCHAR(5)) CountyFIPSCd
		, CAST(GEO.STFIP AS VARCHAR(2)) StateFIPSCd
		, GEO.CITY [CityName]
		, GEO.ZIP [ZipCd]
		, GEO.STATE [StatePostalCd]
		, CAST(
			COALESCE(GEO.STREET,'') +' '+
			COALESCE(GEO.CITY,'')+', '+
			COALESCE(GEO.STATE,'')+' '+
			COALESCE(GEO.ZIP,'') 
			AS VARCHAR(2000))	[InstitutionAddress]

		, cast(GEO.CD as varchar(5)) CongresionalDistrict
		, CAST(GEO.SLDL AS VARCHAR(5)) StateHouseDistrict
		, CAST(GEO.SLDU AS VARCHAR(5)) StateSenateDistrict

		, cast(GEO.CBSA AS VARCHAR(5)) CoreBasedStatisticalAreaCd	
		, CAST(GEO.NMCBSA AS VARCHAR(200)) CoreBasedStatisticalAreaName
		, CAST(GEO.CBSATYPE AS VARCHAR(2)) CoreBasedStatisticalAreaType
	
		, CAST(GEO.CSA AS VARCHAR(5)) CombinedStatisticalAreaCd
		, CAST(GEO.CSA AS VARCHAR(200)) CombinedStatisticalAreaName

	FROM IPEDS.tblEDGEGeocodePostsecondary GEO
)
SELECT * 
FROM BASE
WHERE BASE.AcademicYearDescOrder = 1 /*Gets the latest academic year data for every UNITID */
go
