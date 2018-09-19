/****************************************
	Table: 			tblStateImport
	Description: 	Holds the imported list of US States and territories along with their
					postal and ANSI FIPS codes. 
	Data File Source:	https://www.census.gov/geo/reference/ansi_statetables.html
****************************************/

CREATE TABLE SHARED.tblStateImport (
	StateFIPSCode char(2), 
	StatePostalCode char(2), 
	StateName varchar(75),
	AuditAddUserName  varchar(48)  Not Null default suser_sname(),
	AuditAddDate datetime2 Not Null	default sysdatetime(),
	AuditChangeUserName varchar(48) Not Null default suser_sname(),
    AuditChangeDate datetime2 Not Null default sysdatetime(),
	PRIMARY KEY CLUSTERED(StateFIPSCode)
)
GO
/*
	Unique Index on table to facilitate lookup of state information by postal code
*/
CREATE UNIQUE INDEX IX_SHAREDtblStateImport_UNIQ --IX_UNIQ_State
on SHARED.tblStateImport(StatePostalCode)
GO

/*
	This trigger SETs values in fields AuditChangeDate
	and AuditChangeUserName each time a row is UPDATED
	where clause should identify primary key fields
*/
	CREATE  TRIGGER SHARED.tr_tblStateImport_AuditUpdate
		ON  SHARED.tblStateImport
 		FOR  UPDATE
			AS
		--IF suser_sname() not like '%AppUser%'
		--BEGIN
			UPDATE  SHARED.tblStateImport
	        		SET  	AuditChangeDate = GETDATE(),
 	                		AuditChangeUserName  = (suser_sname())
			FROM  inserted
			WHERE SHARED.tblStateImport.StateFIPSCode = inserted.StateFIPSCode
						
		--END
	GO


/*
	table descriptions script 
*/
EXEC sys.sp_addextendedproperty @name=N'TableDesc'
	, @value=N'Holds the imported US State ANSI-FIPS Code and Names from US Census.' 
	, @level0type=N'SCHEMA'
	, @level0name=N'SHARED'
	, @level1type=N'TABLE'
	, @level1name=N'tblStateImport'