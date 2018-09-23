/*
    Create a basic time dimension based on Academic Years in IPEDS. 

    Consumer price index data table populated from:
        https://www.usinflationcalculator.com/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/
    HECA/HEPI/CPI-U Index values copied from SHEEO Paper:
        http://www.sheeo.org/sites/default/files/Technical_Paper_A_HECA_1.pdf
    HECA is controversial: https://www.air.org/edsector-archives/blog/higher-ed-data-central-inflation-adjusted-sheeo-chart
    
    TODO:
    indicator of US House/Senate controlling parties by year, similar to: https://web.education.wisc.edu/nwhillman/index.php/2017/02/01/party-control-in-congress-and-state-legislatures/
        add presendential party by year
        separate annual dimension to do this by state (other state annual indicators?)
*/
USE [OSDS_ETL]
GO

DROP VIEW SHARED.[vw_DimAcademicYear]
GO
CREATE VIEW SHARED.[vw_DimAcademicYear] as
	SELECT DISTINCT 
		CAST(SURVEY_YEAR +1 AS INT) [AcademicYr]
		, CAST(SURVEY_YEAR AS VARCHAR(4))+ ' - ' + CAST(SURVEY_YEAR + 1 AS VARCHAR(4)) [AcademicYrDesc]
, CASE SURVEY_YEAR + 1 /*AcademicYear*/
        WHEN 2003 THEN 75.07
        WHEN 2004 THEN 77.06
        WHEN 2005 THEN 79.68
        WHEN 2006 THEN 82.25
        WHEN 2007 THEN 84.59
        WHEN 2008 THEN 87.84
        WHEN 2009 THEN 87.52
        WHEN 2010 THEN 88.96
        WHEN 2011 THEN 91.77
        WHEN 2012 THEN 93.67
        WHEN 2013 THEN 95.04
        WHEN 2014 THEN 96.58
        WHEN 2015 THEN 96.69
        WHEN 2016 THEN 97.91
        WHEN 2017 THEN 100.00    
     else NULL end [CPI_U]

    , case SURVEY_YEAR + 1
        WHEN 2003 THEN 72.18
        WHEN 2004 THEN 74.62
        WHEN 2005 THEN 77.12
        WHEN 2006 THEN 79.58
        WHEN 2007 THEN 82.41
        WHEN 2008 THEN 84.83
        WHEN 2009 THEN 86.18
        WHEN 2010 THEN 87.46
        WHEN 2011 THEN 89.33
        WHEN 2012 THEN 90.94
        WHEN 2013 THEN 92.57
        WHEN 2014 THEN 94.48
        WHEN 2015 THEN 96.08
        WHEN 2016 THEN 97.82
        WHEN 2017 THEN 100.00
        ELSE NULL END [HECA]

    , CASE SURVEY_YEAR + 1
        WHEN 2003 THEN 67.58
        WHEN 2004 THEN 70.06
        WHEN 2005 THEN 72.82
        WHEN 2006 THEN 76.53
        WHEN 2007 THEN 78.71
        WHEN 2008 THEN 82.61
        WHEN 2009 THEN 84.46
        WHEN 2010 THEN 85.21
        WHEN 2011 THEN 87.21
        WHEN 2012 THEN 88.66
        WHEN 2013 THEN 90.05
        WHEN 2014 THEN 92.74
        WHEN 2015 THEN 94.74
        WHEN 2016 THEN 96.46
        WHEN 2017 THEN 100.00
        ELSE NULL END [HEPI]

		, CPI.[Annual Average] AverageCPI
	FROM IPEDS.tblHD HD
		LEFT JOIN SHARED.tblConsumerPriceIndexImport CPI ON HD.SURVEY_YEAR + 1 = CPI.[Year]

GO

DROP TABLE OSDS_RPT.SHARED.tblDimAcademicYear
SELECT * INTO OSDS_RPT.SHARED.tblDimAcademicYear FROM OSDS_ETL.SHARED.vw_DimAcademicYear 
CREATE CLUSTERED COLUMNSTORE INDEX IX_AcademicYear_ColStore on OSDS_RPT.SHARED.tblDimAcademicYear