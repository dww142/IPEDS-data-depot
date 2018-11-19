    /*

    Script Creates the view
    Then loads data from the view into the RPT database

        Dimension view for the unique (single record per Institution) list of institutions in IPEDS

        - this gets the latest available record for the institution and presents that record in the view

        It also joins to the IC survey data for certain elements, and the state dimension to 
        present the ANSI/FIPS state code for the institution's 

    Process description for the CTE by subquery:
        RANKED: Latest record and select attributes for each Unit ID in the institution directory (HD) file
        INSTBASE: Rank the main campus (institution) from the group of campuses based on select attributes
        HIERARCHY: Get the system name, for the main campus, and assign institution attributes to each campus record
        OUTPUT: Get the latest attributes of each campus for each insitution
    result: flattened record with hierarhcy of system > Institution > campus
        with consistent attributes of control/sector/level for all campuses in an institution
    */
    USE OSDS_ETL;

    DROP VIEW IPEDS.vw_DimInstitution
    GO
    CREATE VIEW IPEDS.vw_DimInstitution AS -- WITH SCHEMABINDING AS 

    /*Get Institutions & Systems to establish entity hierarchy: System > Institution > Campus*/
    WITH INSTBASE AS (
    /*Get the latest record present in the directory for every Unit ID by Academic Year*/
        SELECT DISTINCT 
            CAST(HD.SURVEY_YEAR AS INT) + 1 [MostRecentIPEDSAcademicYr]
            , CAST(NULLIF(NULLIF(HD.OPEID,'-2'),'') AS VARCHAR(10)) [OPEID]
            , CAST(LEFT(NULLIF(NULLIF(HD.OPEID,'-2'),''),6) AS VARCHAR(6)) InstitutionOPEID
            , CAST(RIGHT(NULLIF(NULLIF(HD.OPEID,'-2'),''),2) AS VARCHAR(2)) CampusOPEID
            , CAST(HD.UNITID AS INT) [UnitID]
            , CAST(NULLIF(NULLIF(NULLIF(LTRIM(RTRIM(HD.F1SYSNAM)),''),'-2'),'-1') AS VARCHAR(150)) SystemName
            , CAST(HD.INSTNM AS VARCHAR(200)) [InstitutionName]
            , cast(CONTROL.LookupDesc as varchar(50)) [InstitutionControl]
            , cast(INSTCAT.LookupDesc as varchar(100)) [InstitutionCategory]
            , cast(CCBASIC.LookupDesc as varchar(100)) [CarnegieClassification]
            , CAST(NULLIF(HD.DEATHYR,-2) AS VARCHAR(10)) InstitutionCloseYr
            , PDEIC.PDEAgencyTypeCd 
            , PDEIC.PDEAgencyTypeDesc
            , PDEIC.AUNumber
        FROM IPEDS.tblHD HD
            INNER JOIN (
                    SELECT UNITID, max(SURVEY_YEAR) MAX_SY
                        , avg(CAST(LEFT(NULLIF(LONGITUD,''),18) AS NUMERIC(18,4))) [AvgLongitude]
                        , avg(CAST(LEFT(NULLIF(LATITUDE,''),18) AS NUMERIC(18,4))) [AvgLatitude]
                    FROM IPEDS.tblHD
                    GROUP BY UNITID
                ) LATEST ON HD.UNITID = LATEST.UNITID AND HD.SURVEY_YEAR = LATEST.MAX_SY
            LEFT JOIN SHARED.vw_UniqueLookupList INSTCAT ON COALESCE(NULLIF(HD.INSTCAT,'0'),'-2') = INSTCAT.LookupCd AND UPPER(INSTCAT.LookupName)='INSTCAT'
            LEFT JOIN SHARED.vw_UniqueLookupList CCBASIC ON COALESCE(HD.CCBASIC,'0') = CCBASIC.LookupCd AND UPPER(CCBASIC.LookupName)='CCBASIC'
            LEFT JOIN SHARED.vw_UniqueLookupList CONTROL ON COALESCE(HD.CONTROL,'-3') = CONTROL.LookupCd AND UPPER(CONTROL.LookupName)='CONTROL'
            LEFT JOIN PDE.tblPDEInstitutionData PDEIC ON HD.UNITID = PDEIC.NCESIdNumber_UnitID
    ), RANKED AS (
    /*Rank institution names for multi-campus institutions to determine main campus name*/
    SELECT DISTINCT
        I.MostRecentIPEDSAcademicYr
        , I.InstitutionOPEID
        , I.CampusOPEID
        , I.UnitID
        , I.SystemName
        , I.InstitutionName
        , I.PDEAgencyTypeCd
        , I.PDEAgencyTypeDesc
        , I.AUNumber
        , 	RANK() OVER(PARTITION BY InstitutionOPEID
                        ORDER BY CASE WHEN CampusOPEID='00' THEN 1 ELSE 2 end								/*00 campus should rank first*/
                                , MostRecentIPEDSAcademicYr DESC											/*most recent data first if multiple 00 campuses*/
                                , coalesce(InstitutionCloseYr,9999) desc									/*still open, or latest closed (open campus first)*/
                                , CASE WHEN UPPER(CarnegieClassification) LIKE 'NOT%' THEN 2 ELSE 1 END		/*has a valid carnegie class*/
                                , CASE WHEN UPPER(InstitutionCategory) LIKE 'NOT%' THEN 2 ELSE 1 END		/*has a valid category*/
                                , InstitutionName desc														/*rank by name as a tiebreaker (max name wins; only valid in few cases)*/
                ) [INSTITUTION_RANK]
    FROM INSTBASE I
    ), HIERARCHY AS (
    /*Identify system name; main campus name and establish hierarchy with each campus. */
    SELECT DISTINCT
        RANKED.MostRecentIPEDSAcademicYr
        , COALESCE( RANKED.SystemName
                    , MAX(CASE WHEN RANKED.INSTITUTION_RANK = 1 THEN RANKED.InstitutionName ELSE NULL END) OVER(PARTITION BY RANKED.InstitutionOPEID)
                    , RANKED.InstitutionName
                ) SystemName
        , RANKED.InstitutionOPEID
        , MAX(CASE WHEN RANKED.INSTITUTION_RANK = 1 THEN RANKED.InstitutionName ELSE NULL END) OVER(PARTITION BY RANKED.InstitutionOPEID) [InstitutionName (Main Campus)]
        , RANKED.CampusOPEID
        , RANKED.UnitID
        , RANKED.InstitutionName [CampusName]
        , MAX(CASE WHEN RANKED.INSTITUTION_RANK = 1 THEN RANKED.PDEAgencyTypeDesc ELSE NULL END) OVER(PARTITION BY RANKED.InstitutionOPEID) PDEAgencyTypeDesc
        , MAX(CASE WHEN RANKED.INSTITUTION_RANK = 1 THEN RANKED.PDEAgencyTypeCd ELSE NULL END) OVER(PARTITION BY RANKED.InstitutionOPEID) PDEAgencyTypeCd
        , MAX(CASE WHEN RANKED.INSTITUTION_RANK = 1 THEN RANKED.AUNumber ELSE NULL END) OVER(PARTITION BY RANKED.InstitutionOPEID) PDEAUNumber

        --, RANKED.PDEInstitutionType
        --, RANKED.PDEInstitutionTypeAbbr
    FROM RANKED
    )
    SELECT DISTINCT 
            CAST(I.SURVEY_YEAR AS INT) + 1 [MostRecentIPEDSAcademicYr]
            , HIERARCHY.UnitID
            
            , CAST(NULLIF(NULLIF(I.OPEID,'-2'),'') AS VARCHAR(10)) [OPEID]
            , HIERARCHY.SystemName
            , HIERARCHY.InstitutionOPEID
            , HIERARCHY.[InstitutionName (Main Campus)]
            , HIERARCHY.CampusOPEID
            , HIERARCHY.CampusName
            

        /*FOREIGN KEYS from HD (Directory)*/
            , cast(SECTOR.LookupDesc as varchar(50)) [Sector]
            , cast(ICLEVEL.LookupDesc as varchar(50)) [InstitutionLevel]
            , cast(CONTROL.LookupDesc as varchar(50)) [InstitutionControl]
            , cast(DEGGRANT.LookupDesc as varchar(50)) [DegreeGrantingStatus]
            , cast(HBCU.LookupDesc as varchar(50)) [HistoricallyBlackCollegeOrUniversityStatus]
            , cast(TRIBAL.LookupDesc as varchar(50)) [TribalCollegeStatus]
            , cast(LOCALE.LocaleDesc as varchar(40)) [InstitutionLocaleDetail]
            , cast(LOCALE.UrbanRuralStatus as varchar(20)) [InstitutionLocaleCategory]
            , cast(INSTCAT.LookupDesc as varchar(100)) [InstitutionCategory]
            , cast(CCBASIC.LookupDesc as varchar(100)) [CarnegieClassification]
            , cast(LANDGRNT.LookupDesc as varchar(100)) [LandGrantInstitutionStatus]
            , cast(INSTSIZE.LookupDesc as varchar(100)) [InstitutionSizeCategory]
            , cast(OBEREG.LookupDesc as varchar(100)) [USGeographicRegion]

            , cast(HOSPITAL.LookupDesc as varchar(100)) [InstitutionHasHospital]
            , cast(MEDICAL.LookupDesc as varchar(100)) [InstitutionGrantsMedicalDegree]
            , cast(OPEFLAG.LookupDesc as varchar(100)) [OPETitleIVEligibleInd] /*rm pending testing*/
            , cast(POSTSEC.LookupDesc as varchar(100)) [PrimarilyPostsecondaryInd] /*rm pending testing*/
            , cast(PSEFLAG.LookupDesc as varchar(100)) [PostsecondaryInstitutionInd] /*rm pending testing*/
            , cast(PSET4FLG.LookupDesc as varchar(100)) [PostsecondaryAndTitleIVInstitutionInd] /*keep */
            , cast(RPTMTH.LookupDesc as varchar(100)) [ReportingMethod]

        /* FOREIGN KEYS from IC (Characteristics)*/
            , cast(RELAFFIL.LookupDesc as varchar(100)) [ReligiousAffiliation]
            , cast(OPENADMP.LookupDesc as varchar(100)) [OpenAdmissionPolicy]
            , cast(LIBFAC.LookupDesc as varchar(100)) [HasLibaryFacilities]
            , cast(ALLONCAM.LookupDesc as varchar(100)) [FirstTimeFullTimeDegreeSeekingStudentsRequiredToLiveOnCampus]
            , cast(ROOM.LookupDesc as varchar(100)) [InstitutionProvidesOnCampusHousing]
            , cast(BOARD.LookupDesc as varchar(100)) [InstitutionProvidesMealPlan]

        /*GEOGRAPHY
            lot of coalescing; merging EDGE Geocoded data with directory survey file data; 
            should result in (ideally) the latest geographic information avialable for an institution
        */
        
            , CAST(COALESCE(S.StatePostalCode, I.STABBR,'NA') AS VARCHAR(2)) AS [StatePostalCd]
            , COALESCE(GEO.StateFIPSCd, CAST(S.StateFIPSCode AS CHAR(2)),'00') StateFIPSCd
            , CAST(coalesce(GEO.CountyFIPSCd,
                        Nullif(CASE WHEN LEN(I.COUNTYCD)=4 THEN '0'+I.COUNTYCD ELSE I.COUNTYCD END,'')
                        ,'-2') AS CHAR(5)) AS CountyFIPSCd
            , CAST(COALESCE(GEO.CityName,I.CITY,'') AS VARCHAR(1000)) City
            , CAST(COALESCE(LEFT(GEO.ZipCd,5),LEFT(I.ZIP,5),'') AS VARCHAR(10)) [ZipCd]
            , COALESCE(GEO.InstitutionAddress, 
                    CAST(CASE WHEN '' NOT IN (I.ADDR, I.CITY, I.ZIP) THEN 
                    COALESCE(I.ADDR,'') +' '+
                    COALESCE(I.CITY,'')+', '+
                    COALESCE(I.STABBR,'')+' '+
                    COALESCE(I.ZIP,'') 
                    ELSE '' END AS VARCHAR(2000)))
                [InstitutionAddress]

            , GEO.CoreBasedStatisticalAreaCd
            , GEO.CoreBasedStatisticalAreaName
            , GEO.CoreBasedStatisticalAreaType

            , GEO.CombinedStatisticalAreaCd
            , GEO.CombinedStatisticalAreaName

            , GEO.CongresionalDistrict
            , GEO.StateHouseDistrict
            , GEO.StateSenateDistrict

            , CAST(COALESCE(GEO.Latitude, LEFT(NULLIF(I.LATITUDE,''),18), LATEST.AvgLatitude) AS NUMERIC(18,4)) [Latitude]
            , CAST(coalesce(GEO.Longitude, LEFT(NULLIF(I.LONGITUD,''),18), LATEST.AvgLongitude) AS NUMERIC(18,4)) [Longitude]

            , CAST(NULLIF(I.DEATHYR,-2) AS VARCHAR(10)) [InstitutionCloseYr]
        
        /*PDE pennsylvania specific attributes*/
            , HIERARCHY.PDEAgencyTypeDesc
            , HIERARCHY.PDEAgencyTypeCd
            , HIERARCHY.PDEAUNumber
            , CASE WHEN PDEIC.PubSchOrBranchNumber = '0' THEN '9999' 
                ELSE RIGHT('0000' + CAST(PDEIC.PubSchOrBranchNumber AS VARCHAR(4)),4) 
                END [PDEBranchCampusCd]
            , CASE WHEN HIERARCHY.UnitID IN (
                            /*Community Colleges*/
                                437431 --  Community College           Bucks County Community College
                                , 211307 --  Community College           Bucks County Community College
                                , 211343 --  Community College           Butler County Community College
                                , 210605 --  Community College           Community College of Allegheny County
                                , 211079 --  Community College           Community College of Beaver County
                                , 215239 --  Community College           Community College of Philadelphia
                                , 211927 --  Community College           Delaware County Community College
                                , 437246 --  Community College           Harrisburg Area Community College-Harrisburg
                                , 212878 --  Community College           Harrisburg Area Community College-Harrisburg
                                , 369172 --  Community College           Harrisburg Area Community College-Harrisburg
                                , 369181 --  Community College           Harrisburg Area Community College-Harrisburg
                                , 452142 --  Community College           Harrisburg Area Community College-Harrisburg
                                , 213525 --  Community College           Lehigh Carbon Community College
                                , 213659 --  Community College           Luzerne County Community College
                                , 214111 --  Community College           Montgomery County Community College
                                , 445009 --  Community College           Montgomery County Community College
                                , 214379 --  Community College           Northampton County Area Community College
                                , 407638 --  Community College           Northampton County Area Community College
                                , 414911 --  Community College           Pennsylvania Highlands Community College
                                , 215585 --  Community College           Reading Area Community College
                                , 216825 --  Community College           Westmoreland County Community College
                            /*PASSHE*/
                                , 211158 --  State University    Bloomsburg University of Pennsylvania
                                , 211361 --  State University    California University of Pennsylvania
                                , 211608 --  State University    Cheyney University of Pennsylvania
                                , 211644 --  State University    Clarion University of Pennsylvania
                                , 211662 --  State University    Clarion University of Pennsylvania
                                , 212115 --  State University    East Stroudsburg University of Pennsylvania
                                , 212160 --  State University    Edinboro University of Pennsylvania
                                , 213048 --  State University    Indiana University of Pennsylvania-Main Campus
                                , 213020 --  State University    Indiana University of Pennsylvania-Main Campus
                                , 213039 --  State University    Indiana University of Pennsylvania-Main Campus
                                , 213349 --  State University    Kutztown University of Pennsylvania
                                , 213613 --  State University    Lock Haven University
                                , 381389 --  State University    Lock Haven University
                                , 213783 --  State University    Mansfield University of Pennsylvania
                                , 216010 --  State University    Shippensburg University of Pennsylvania
                                , 216038 --  State University    Slippery Rock University of Pennsylvania
                                , 216764 --  State University    West Chester University of Pennsylvania
                            /*OPT-IN*/
                                , 211352 --  Private College and University  Cabrini University
                                , 211431 --  Private College and University  Carlow University
                                , 212656 --  Private College and University  Geneva College
                                , 213376 --  Private Two-Year College    Lackawanna College
                                , 213598 --  State-Related Commonwealth University   Lincoln University,  --

                        ) THEN 'PA Transfer System University'
                    WHEN I.STABBR = 'PA' THEN 'PA Non-Transfer System University'
                    ELSE 'Non-PA University'
                END [PATransferSystemStatus]
        FROM IPEDS.tblHD I
            INNER JOIN (
                    SELECT UNITID, max(SURVEY_YEAR) MAX_SY
                        , avg(CAST(LEFT(NULLIF(LONGITUD,''),18) AS NUMERIC(18,4))) [AvgLongitude]
                        , avg(CAST(LEFT(NULLIF(LATITUDE,''),18) AS NUMERIC(18,4))) [AvgLatitude]
                    FROM IPEDS.tblHD
                    GROUP BY UNITID
                ) LATEST ON I.UNITID = LATEST.UNITID AND I.SURVEY_YEAR = LATEST.MAX_SY
            INNER JOIN HIERARCHY ON I.UNITID = HIERARCHY.UnitID
            LEFT JOIN IPEDS.tblIC IC ON I.UNITID = IC.UNITID AND I.SURVEY_YEAR = IC.SURVEY_YEAR
            LEFT JOIN IPEDS.vw_GeocodedInstitutions GEO ON I.UNITID = GEO.UnitID
            LEFT JOIN SHARED.tblStateImport S ON I.STABBR = S.StatePostalCode --OR GEO.StateFIPSCd = S.StateFIPSCode
        /*Get simple HD file lookups (Coalesce to NA or Unknown codes when null for each lookup)*/
            LEFT JOIN SHARED.vw_DimLocaleCode LOCALE ON COALESCE(I.LOCALE,'-3') = LOCALE.LocalePK
            LEFT JOIN SHARED.vw_UniqueLookupList SECTOR ON COALESCE(I.SECTOR,'99') = SECTOR.LookupCd AND upper(SECTOR.LookupName) = 'SECTOR'
            LEFT JOIN SHARED.vw_UniqueLookupList ICLEVEL ON COALESCE(I.ICLEVEL,'-3') = ICLEVEL.LookupCd AND UPPER(ICLEVEL.LookupName)='ICLEVEL'
            LEFT JOIN SHARED.vw_UniqueLookupList CONTROL ON COALESCE(I.CONTROL,'-3') = CONTROL.LookupCd AND UPPER(CONTROL.LookupName)='CONTROL'
            LEFT JOIN SHARED.vw_UniqueLookupList DEGGRANT ON COALESCE(I.DEGGRANT,'-3') = DEGGRANT.LookupCd AND UPPER(DEGGRANT.LookupName)='DEGGRANT'
            LEFT JOIN SHARED.vw_UniqueLookupList HBCU ON COALESCE(I.HBCU,'-3') = HBCU.LookupCd AND UPPER(HBCU.LookupName)='HBCU'
            LEFT JOIN SHARED.vw_UniqueLookupList TRIBAL ON COALESCE(I.TRIBAL,'-3') = TRIBAL.LookupCd AND UPPER(TRIBAL.LookupName)='TRIBAL'
            
            LEFT JOIN SHARED.vw_UniqueLookupList INSTCAT ON COALESCE(NULLIF(I.INSTCAT,'0'),'-2') = INSTCAT.LookupCd AND UPPER(INSTCAT.LookupName)='INSTCAT'
            LEFT JOIN SHARED.vw_UniqueLookupList CCBASIC ON COALESCE(I.CCBASIC,'0') = CCBASIC.LookupCd AND UPPER(CCBASIC.LookupName)='CCBASIC'
            LEFT JOIN SHARED.vw_UniqueLookupList LANDGRNT ON COALESCE(I.LANDGRNT,'0') = LANDGRNT.LookupCd AND UPPER(LANDGRNT.LookupName)='LANDGRNT'
            LEFT JOIN SHARED.vw_UniqueLookupList INSTSIZE ON COALESCE(NULLIF(I.INSTSIZE,'0'),'-2') = INSTSIZE.LookupCd AND UPPER(INSTSIZE.LookupName)='INSTSIZE'
            LEFT JOIN SHARED.vw_UniqueLookupList OBEREG ON COALESCE(I.OBEREG,'0') = OBEREG.LookupCd AND UPPER(OBEREG.LookupName)='OBEREG'
            LEFT JOIN SHARED.vw_UniqueLookupList HOSPITAL ON COALESCE(I.HOSPITAL,'-2') = HOSPITAL.LookupCd AND UPPER(HOSPITAL.LookupName)='HOSPITAL'
            LEFT JOIN SHARED.vw_UniqueLookupList MEDICAL ON COALESCE(I.MEDICAL,'-2') = MEDICAL.LookupCd AND UPPER(MEDICAL.LookupName)='MEDICAL'
            LEFT JOIN SHARED.vw_UniqueLookupList OPEFLAG ON COALESCE(I.OPEFLAG,'-2') = OPEFLAG.LookupCd AND UPPER(OPEFLAG.LookupName)='OPEFLAG'
            LEFT JOIN SHARED.vw_UniqueLookupList POSTSEC ON COALESCE(I.POSTSEC,'-2') = POSTSEC.LookupCd AND UPPER(POSTSEC.LookupName)='POSTSEC'
            LEFT JOIN SHARED.vw_UniqueLookupList PSEFLAG ON COALESCE(I.PSEFLAG,'-2') = PSEFLAG.LookupCd AND UPPER(PSEFLAG.LookupName)='PSEFLAG'
            LEFT JOIN SHARED.vw_UniqueLookupList PSET4FLG ON COALESCE(I.PSET4FLG,'-2') = PSET4FLG.LookupCd AND UPPER(PSET4FLG.LookupName)='PSET4FLG'
            LEFT JOIN SHARED.vw_UniqueLookupList RPTMTH ON COALESCE(I.RPTMTH,'-2') = RPTMTH.LookupCd AND UPPER(RPTMTH.LookupName)='RPTMTH'
        /*Get simple IC file lookups*/
            LEFT JOIN SHARED.vw_UniqueLookupList RELAFFIL ON COALESCE(IC.RELAFFIL,'-1') = RELAFFIL.LookupCd AND UPPER(RELAFFIL.LookupName)='RELAFFIL'
            LEFT JOIN SHARED.vw_UniqueLookupList OPENADMP ON COALESCE(IC.OPENADMP,'-1') = OPENADMP.LookupCd AND UPPER(OPENADMP.LookupName)='OPENADMP'
            LEFT JOIN SHARED.vw_UniqueLookupList LIBFAC ON COALESCE(IC.LIBFAC,'-1') = LIBFAC.LookupCd AND UPPER(LIBFAC.LookupName)='LIBFAC'
            LEFT JOIN SHARED.vw_UniqueLookupList ALLONCAM ON COALESCE(IC.ALLONCAM,'-1') = ALLONCAM.LookupCd AND UPPER(ALLONCAM.LookupName)='ALLONCAM'
            LEFT JOIN SHARED.vw_UniqueLookupList BOARD ON COALESCE(IC.BOARD,'-1') = BOARD.LookupCd AND UPPER(BOARD.LookupName)='BOARD'
            LEFT JOIN SHARED.vw_UniqueLookupList ROOM ON COALESCE(IC.ROOM,'-1') = ROOM.LookupCd AND UPPER(ROOM.LookupName)='ROOM'
    LEFT JOIN PDE.tblPDEInstitutionData PDEIC ON I.UNITID = PDEIC.NCESIdNumber_UnitID

    --ORDER BY InstitutionOPEID
    GO


    DROP TABLE OSDS_RPT.IPEDS.tblDimInstitution
    SELECT DISTINCT * INTO OSDS_RPT.IPEDS.tblDimInstitution FROM OSDS_ETL.IPEDS.vw_DimInstitution
    CREATE CLUSTERED COLUMNSTORE INDEX IX_tblDimInstitution on OSDS_RPT.IPEDS.tblDimInstitution

