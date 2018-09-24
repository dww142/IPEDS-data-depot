/****************************************
	Table: 			tblCountyImport
	Description: 	Holds the imported list of US Counties and their ANSI FIPS Codes
					as obtained from the US Census Bureau. 
	Data File Source: https://www.census.gov/geo/reference/codes/cou.html
****************************************/
USE OSDS_ETL;
DROP TABLE IF EXISTS SHARED.tblCountyImport ;

CREATE TABLE SHARED.tblCountyImport (
	StatePostalCode char(2), 
	StateFIPSCode char(2), 
	CountyFIPSCode char(3),
	CountyName varchar(100),
	CountyCensusClass char(10),
	AuditAddUserName  varchar(48)   Null default suser_sname(),
	AuditAddDate datetime2  Null	default sysdatetime(),
	AuditChangeUserName varchar(48)  Null default suser_sname(),
    AuditChangeDate datetime2  Null default sysdatetime(),
	PRIMARY KEY CLUSTERED(StateFIPSCode, CountyFIPSCode)
)
GO
/*
	Unique Index on table to facilitate lookup of FIPS codes by Postal Code
*/
CREATE UNIQUE INDEX IX_SHAREDtblCountyImport_UNIQ  --IX_UNIQ_Counties 
	on SHARED.tblCountyImport(StatePostalCode, CountyFIPSCode)
	Include (StateFIPSCode)
GO

/*
	This trigger SETs values in fields AuditChangeDate
	and AuditChangeUserName each time a row is UPDATED
	where clause should identify primary key fields
*/
	CREATE TRIGGER SHARED.tr_tblCountyImport_AuditUpdate
		ON  SHARED.tblCountyImport
 		FOR  UPDATE
			AS
		--IF suser_sname() not like '%AppUser%'
		--BEGIN
			UPDATE  SHARED.tblCountyImport
	        		SET  	AuditChangeDate = GETDATE(),
 	                		AuditChangeUserName  = (suser_sname())
			FROM  inserted
			WHERE SHARED.tblCountyImport.StateFIPSCode = inserted.StateFIPSCode
						AND SHARED.tblCountyImport.CountyFIPSCode = inserted.CountyFIPSCode
		--END
	GO

/*
	Table Extended Property Description
*/
EXEC sys.sp_addextendedproperty @name=N'TableDesc'
	, @value=N'Holds the imported US County ANSI-FIPS Code and Names from US Census.' 
	, @level0type=N'SCHEMA'
	, @level0name=N'SHARED'
	, @level1type=N'TABLE'
	, @level1name=N'tblCountyImport'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'US Postal abbreviation for States and Territories', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblCountyImport', @level2type=N'COLUMN',@level2name=N'StatePostalCode'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'ANSI-FIPS 2 digit identifying code for US States and territories', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblCountyImport', @level2type=N'COLUMN',@level2name=N'StateFIPSCode'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'ANSI-FIPS 3 digit identifying County Code (Combines with State FIPS code to create a 5 digit unique identifying FIPS code)', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblCountyImport', @level2type=N'COLUMN',@level2name=N'CountyFIPSCode'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'County Name', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblCountyImport', @level2type=N'COLUMN',@level2name=N'CountyName'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'FIPS Classification code (not used)', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblCountyImport', @level2type=N'COLUMN',@level2name=N'CountyCensusClass'
GO
