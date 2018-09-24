<b> IPEDS Data Depot </b>  
This project is intended to be a repository for all types of aggragated publicly available data sources (eventually beyond IPEDS); compiled and transformed to simplify the process of linking those sources through common time, geographic, and demographic dimnesions. The primary data source is the Integrated Postsecondary Education Data System (IPEDS). 

<b>Process</b>  
* Python Scripts download IPEDS complete data files and load them into a SQL Server Database (OSDS_ETL).  
* SQL views are created to pivot wide data structures into tall structures while standardizing common time, geographic, and demographic dimensions across subject areas. Views cast data types appropriately, handle null values, create referential integrity while transforming data. Views can be queried for testing purposes but performance is slow.  
* Views are inserted into tables in a second database (OSDS_RPT) where clustered columnstore indexes are created on every table for improved read performance.
  
<b>Structure</b>  
Schemas logically separate data by source  (will extend beyond IPEDS eventually)  
* <i>SHARED</i>  
    * The shared schema contains dimension data common to multiple data sources and uses, notably time and geography, as well as demographic categorizations like Gender and Race dimensions. Categorical items are added to County and State dimensions to allow aggregation and comparison by category (e.g., this project is based in Pennsylvania, PA counties are identified by economic development regions within the state).
* <i> IPEDS</i>  
    * This is the schema holding IPEDS survey file data
  
<i>Setup</i>  
This project is designed for MS SQL Server at the moment, developer edition is free for personal use: https://www.microsoft.com/en-us/sql-server/sql-server-downloads  
<i>TODO: automate this for first time setup</i>  
1. Execute the CreateDatabases.sql script; creates ETL and reporting database; creates a sql_alchemy user with default password;
2. in the SHARED/ETL_Tables folder, execute the table creation scripts; 
3. in the SHARED folder execute shared_data_loader.py script (loads the CSV files in the data folder into tables)
4. In the IPEDS folder - configure settings.py as desired (start/end years to download, surveys and files to download
5. from the IPEDS folder - execute ipeds_downloader.py
6. from the IPEDS folder - execute ipeds_data_loader.py
7. execute each view creation script in SHARED\ETL_Views
8. execute each view creation script in IPEDS\ETL_Views  
  
<i>The view creation scripts also extract data from the ETL database and load to the RPT database</i>


