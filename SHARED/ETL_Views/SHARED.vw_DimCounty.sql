/**************************************************************************************************
view to produce the county dimension for the SLDS data mart
Includes mapping of PA county codes to FIPS/ANSI standard codes

This view adds an NA record for every state (000) county code (i..e, 42000 = PA-NA county)  

 Base Table : SHARED.tblCountyImport 

**************************************************************************************************/
USE OSDS_ETL;

DROP VIEW IF EXISTS SHARED.vw_DimCounty
GO
CREATE VIEW [SHARED].[vw_DimCounty] AS
    SELECT DISTINCT
        CASE 
            WHEN C.CountyFIPSCode <> -2 THEN C.StateFIPSCode + C.CountyFIPSCode 
            ELSE C.CountyFIPSCode
        END AS [CountyFIPSCd]
        , C.CountyName
        , C.StatePostalCode as [StatePostalCd]
        , C.StateFIPSCode as [StateFIPSCd]
        , CASE
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42001' THEN '01' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42003' THEN '02' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42005' THEN '03' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42007' THEN '04' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42009' THEN '05' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42011' THEN '06' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42013' THEN '07' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42015' THEN '08' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42017' THEN '09' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42019' THEN '10' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42021' THEN '11' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42023' THEN '12' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42025' THEN '13' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42027' THEN '14' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42029' THEN '15' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42031' THEN '16' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42033' THEN '17' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42035' THEN '18' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42037' THEN '19' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42039' THEN '20' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42041' THEN '21' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42043' THEN '22' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42045' THEN '23' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42047' THEN '24' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42049' THEN '25' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42051' THEN '26' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42053' THEN '27' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42055' THEN '28' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42057' THEN '29' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42059' THEN '30' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42061' THEN '31' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42063' THEN '32' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42065' THEN '33' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42067' THEN '34' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42069' THEN '35' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42071' THEN '36' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42073' THEN '37' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42075' THEN '38' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42077' THEN '39' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42079' THEN '40' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42081' THEN '41' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42083' THEN '42' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42085' THEN '43' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42087' THEN '44' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42089' THEN '45' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42091' THEN '46' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42093' THEN '47' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42095' THEN '48' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42097' THEN '49' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42099' THEN '50' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42101' THEN '51' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42103' THEN '52' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42105' THEN '53' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42107' THEN '54' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42109' THEN '55' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42111' THEN '56' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42113' THEN '57' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42115' THEN '58' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42117' THEN '59' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42119' THEN '60' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42121' THEN '61' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42123' THEN '62' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42125' THEN '63' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42127' THEN '64' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42129' THEN '65' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42131' THEN '66' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42133' THEN '67' 
            ELSE '00' /*Outside PA (IN PIMS)*/
        END AS [PACountyCd]
        , COALESCE(MAX(CASE CR.RegionType WHEN 'PA Workforce Development Region' THEN CR.RegionName ELSE NULL END),'NA') [PAWorkforceDevelopmentRegion]
        , COALESCE(MAX(CASE CR.RegionType WHEN 'PA Workforce Region' THEN CR.RegionName ELSE NULL END), 'NA') [PAWorkforceRegion]
        , COALESCE(MAX(CASE CR.RegionType WHEN 'PAEconomicDevelopmentRegion' THEN CR.RegionName ELSE NULL END), 'NA') [PAEconomicDevelopmentRegion]
    FROM (
                SELECT StatePostalCode, StateFIPSCode, CountyFIPSCode, [CountyName], [CountyCensusClass] FROM SHARED.tblCountyImport
                UNION
                /*Create a 000 code for each state as the NA county value for that state*/
                SELECT StatePostalCode, StateFIPSCode, '000' CountyFIPSCode, 'NA' [CountyName], 'NA' [CountyCensusClass] FROM SHARED.tblStateImport
            ) C
        LEFT JOIN SHARED.tblCountyRegionImport CR ON C.StateFIPSCode = CR.StateFIPSCd AND C.CountyFIPSCode = CR.CountyFIPS_3
    --where c.StateFIPSCode = '42'

    GROUP BY 
        CASE 
            WHEN C.CountyFIPSCode <> -2 THEN C.StateFIPSCode + C.CountyFIPSCode 
            ELSE C.CountyFIPSCode
        END
        , C.CountyName
        , C.StatePostalCode
        , C.StateFIPSCode
        , CASE
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42001' THEN '01' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42003' THEN '02' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42005' THEN '03' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42007' THEN '04' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42009' THEN '05' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42011' THEN '06' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42013' THEN '07' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42015' THEN '08' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42017' THEN '09' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42019' THEN '10' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42021' THEN '11' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42023' THEN '12' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42025' THEN '13' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42027' THEN '14' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42029' THEN '15' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42031' THEN '16' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42033' THEN '17' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42035' THEN '18' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42037' THEN '19' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42039' THEN '20' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42041' THEN '21' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42043' THEN '22' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42045' THEN '23' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42047' THEN '24' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42049' THEN '25' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42051' THEN '26' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42053' THEN '27' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42055' THEN '28' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42057' THEN '29' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42059' THEN '30' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42061' THEN '31' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42063' THEN '32' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42065' THEN '33' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42067' THEN '34' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42069' THEN '35' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42071' THEN '36' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42073' THEN '37' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42075' THEN '38' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42077' THEN '39' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42079' THEN '40' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42081' THEN '41' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42083' THEN '42' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42085' THEN '43' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42087' THEN '44' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42089' THEN '45' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42091' THEN '46' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42093' THEN '47' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42095' THEN '48' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42097' THEN '49' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42099' THEN '50' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42101' THEN '51' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42103' THEN '52' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42105' THEN '53' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42107' THEN '54' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42109' THEN '55' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42111' THEN '56' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42113' THEN '57' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42115' THEN '58' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42117' THEN '59' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42119' THEN '60' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42121' THEN '61' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42123' THEN '62' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42125' THEN '63' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42127' THEN '64' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42129' THEN '65' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42131' THEN '66' 
            WHEN C.StateFIPSCode + C.CountyFIPSCode = '42133' THEN '67' 
            ELSE '00' /*Outside PA (IN PIMS)*/
        END 


GO

--NO VIEW INDEX POSSIBLE W/ SUB QUERY union
--CREATE UNIQUE CLUSTERED INDEX IX_County_UNIQ ON SHARED.vw_DimCounty(CountyFIPSCd)


 /*
    ETL into the RPT Database
 */
    DROP TABLE IF EXISTS OSDS_RPT.SHARED.tblDimCounty
    SELECT * INTO OSDS_RPT.SHARED.tblDimCounty FROM OSDS_ETL.SHARED.vw_DimCounty
    CREATE CLUSTERED COLUMNSTORE INDEX IX_County_Colstore ON OSDS_RPT.SHARED.tblDimCounty