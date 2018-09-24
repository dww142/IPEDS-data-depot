/****************************************
	Table: 			tblLookupImport
	Description: 	Holds the imported codeset and description pairs from multiple systems in a
					single integrated table. 

	Allows up to 3 additional descriptive categorical values to be set for any code-description combination. 

	Allows Year specific code-description sets, however dimension views will NOT be time specific, they will
	show the latest description value present for any given code. 

****************************************/
USE OSDS_ETL;
DROP TABLE IF EXISTS SHARED.tblLookupImport

CREATE TABLE SHARED.tblLookupImport(
	[Source] varchar(50) not null,
	SourceYear int not null, 
	LookupName varchar(100) not null,
	LookupCd varchar(50) not null, 
	LookupDesc varchar(255) not null, 
	LookupCategory1 varchar(100), 
	LookupCategory2 varchar(100), 
	LookupCategory3 varchar(100),
	AuditAddUserName  varchar(48)   Null default suser_sname(),
	AuditAddDate datetime2  Null	default sysdatetime(),
	AuditChangeUserName varchar(48)  Null default suser_sname(),
    AuditChangeDate datetime2  Null default sysdatetime(),
	
	primary key clustered ([Source], SourceYear, LookupName, LookupCd)
)
GO
/*
	Unique Index on table to facilitate individual codeset references
*/
CREATE NonCLUSTERED INDEX IX_SHAREDtblLookupImport ON SHARED.tblLookupImport(LookupName) include(LookupCd, LookupDesc, SourceYear)
GO


/*
	This trigger SETs values in fields AuditChangeDate
	and AuditChangeUserName each time a row is UPDATED
	where clause should identify primary key fields
*/
	CREATE  TRIGGER SHARED.tr_tblLookupImport_AuditUpdate
		ON  SHARED.tblLookupImport
 		FOR  UPDATE
			AS
		--IF suser_sname() not like '%AppUser%'
		--BEGIN
			UPDATE  SHARED.tblLookupImport
	        		SET  	AuditChangeDate = GETDATE(),
 	                		AuditChangeUserName  = (suser_sname())
			FROM  inserted
			WHERE SHARED.tblLookupImport.[Source] = inserted.[Source]
						AND SHARED.tblLookupImport.SourceYear = inserted.SourceYear
						AND SHARED.tblLookupImport.LookupName = inserted.LookupName
						AND SHARED.tblLookupImport.LookupCd = inserted.LookupCd
		--END
	GO


/*
	table descriptions script 
*/
EXEC sys.sp_addextendedproperty @name=N'TableDesc'
	, @value=N'Contains simple lookup records mapping codes to descriptions and up to 3 categories or alternate descriptions' 
	, @level0type=N'SCHEMA'
	, @level0name=N'SHARED'
	, @level1type=N'TABLE'
	, @level1name=N'tblLookupImport'

GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Descriptive code identifying the system from which the lookup code and description originated', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'Source'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Numeric year value identifying the year/time period of the original lookup', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'SourceYear'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Name identifying a set/group of lookup codes-descriptions', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupName'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Code representing a specific descriptive text value', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupCd'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Descriptive text represented by the LookupCd value', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupDesc'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Optional categorical variable providing additional descriptive or contextual information for a code, usage can vary between lookup sets', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupCategory1'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Optional categorical variable providing additional descriptive or contextual information for a code, usage can vary between lookup sets', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupCategory2'
GO
/* column descriptions script */EXEC sys.sp_addextendedproperty @name=N'ColumnDesc', @value=N'Optional categorical variable providing additional descriptive or contextual information for a code, usage can vary between lookup sets', @level0type=N'SCHEMA',@level0name=N'SHARED', @level1type=N'TABLE',@level1name=N'tblLookupImport', @level2type=N'COLUMN',@level2name=N'LookupCategory3'
GO
