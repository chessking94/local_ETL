# local_ETL

This abomination is the brainchild of wanting to get away from xp_cmdshell in SQL Server but also
a desire to avoid SSIS. One half of that sentence is respectable, while the other half is perhaps
puzzling.

The intent is to use this application to handle the file system interactions (i.e. iterating through
a directory, moving files, etc) but passing filenames as parameters for SQL Stored Procedures in
order to benefit from SQL's set-based operations once data is loaded to the appropriate staging table.
