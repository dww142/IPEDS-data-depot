/*
    Create a basic time dimension based on Academic Years in IPEDS. 

    Consumer price index data table populated from:
        https://www.usinflationcalculator.com/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/
    HECA/HEPI/CPI-U Index values copied from SHEEO Paper:
        http://www.sheeo.org/sites/default/files/Technical_Paper_A_HECA_1.pdf
    HECA is controversial: https://www.air.org/edsector-archives/blog/higher-ed-data-central-inflation-adjusted-sheeo-chart
    
    SHEEO data adjustment indices published here (2018/2019):
        http://www.sheeo.org/projects/shef-%E2%80%94-state-higher-education-finance
        
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
        WHEN 2003 THEN 0.735738
        WHEN 2004 THEN 0.755331
        WHEN 2005 THEN 0.780921
        WHEN 2006 THEN 0.806113
        WHEN 2007 THEN 0.829072
        WHEN 2008 THEN 0.860905
        WHEN 2009 THEN 0.857842
        WHEN 2010 THEN 0.871913
        WHEN 2011 THEN 0.899435
        WHEN 2012 THEN 0.918049
        WHEN 2013 THEN 0.931496
        WHEN 2014 THEN 0.946607
        WHEN 2015 THEN 0.94773
        WHEN 2016 THEN 0.959686
        WHEN 2017 THEN 0.980131
        WHEN 2018 THEN 1
     else NULL end [CPI_U]

    , case SURVEY_YEAR + 1
        WHEN 2003 THEN 0.70514
        WHEN 2004 THEN 0.728837
        WHEN 2005 THEN 0.75311
        WHEN 2006 THEN 0.777079
        WHEN 2007 THEN 0.804641
        WHEN 2008 THEN 0.828222
        WHEN 2009 THEN 0.841431
        WHEN 2010 THEN 0.854095
        WHEN 2011 THEN 0.872125
        WHEN 2012 THEN 0.888037
        WHEN 2013 THEN 0.904348
        WHEN 2014 THEN 0.923235
        WHEN 2015 THEN 0.938933
        WHEN 2016 THEN 0.9554
        WHEN 2017 THEN 0.97698
        WHEN 2018 THEN 1
        ELSE NULL END [HECA]

    , CASE SURVEY_YEAR + 1
        WHEN 2003 THEN 0.66007
        WHEN 2004 THEN 0.684288
        WHEN 2005 THEN 0.711163
        WHEN 2006 THEN 0.747489
        WHEN 2007 THEN 0.768753
        WHEN 2008 THEN 0.806851
        WHEN 2009 THEN 0.824867
        WHEN 2010 THEN 0.83225
        WHEN 2011 THEN 0.851742
        WHEN 2012 THEN 0.865918
        WHEN 2013 THEN 0.879503
        WHEN 2014 THEN 0.905788
        WHEN 2015 THEN 0.92528
        WHEN 2016 THEN 0.942114
        WHEN 2017 THEN 0.973124
        WHEN 2018 THEN 1
        ELSE NULL END [HEPI]

		, CPI.[Annual Average] AverageCPI
	FROM IPEDS.tblHD HD
		LEFT JOIN SHARED.tblConsumerPriceIndexImport CPI ON HD.SURVEY_YEAR + 1 = CPI.[Year]

GO

DROP TABLE OSDS_RPT.SHARED.tblDimAcademicYear
SELECT * INTO OSDS_RPT.SHARED.tblDimAcademicYear FROM OSDS_ETL.SHARED.vw_DimAcademicYear 
CREATE CLUSTERED COLUMNSTORE INDEX IX_AcademicYear_ColStore on OSDS_RPT.SHARED.tblDimAcademicYear