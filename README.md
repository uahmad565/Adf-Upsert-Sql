# Problem: 
Create an Azure ADF pipeline to copy multiple tables from Source Sql db to destination Sql db. In the article, one time sync changes were limited to only 100 max
operations as Merge SQL statement stores changes in variable. How to meet requirements for bulk operations. 

# Solution: 
Used Azure Blob Storage as medium for temporary holding staging upsert operations depeding upon the watermark values. As this article has not given generic approach 
to handle different tables scripts but initially prepare sql scripts for variables, and limited to 100 operations, use Azure function in place of stored procedures

usp_upsert_customer_table

usp_upsert_project_table

...

same for 18 tables 

# Followed microsoft article.
https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-multiple-tables-portal
