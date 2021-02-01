# Mini-SQL-Engine

Python implementation of a mini SQL engine which runs a subset of SQL Queries using command line interface.


List of queries that can be executed:
  

1. Select all records from 1 or multiple tables 
```
      Format: Select * from table_name
```
2. Aggragate functions (Sum, average, max, min) 
```
      Format: Select max(col1) from table_name
```
3. Project columns 
```
      Format: Select col1,col2 from table_name
```
4. Project with distinct from one table
```
      Format: Select distict(col1) from table_name
```
5. Select with "WHERE" condition( Relational operators "<, >, <=, >=, =" are handled )
```
      Format: Select * from table_name where <condition>
```
6. Select with multiple "WHERE" conditions joined by AND/OR operator
```
      Format: Select * from table_name where <condition-1> and/or <condition-2>
```
7. Sort based on order specified in "ORDER BY" clause
```
      Format: Select * from table_1 order by col1,col2
```      
8. Join 2 tables with/without "WHERE" condition
```
      Format: Select * from table_1,table_2 [where <condition>]   
```
9. Select/Project Columns(could be any number of columns) from table using
“​ group by​ ”:
```
          Format: Select col1, COUNT(col2) from table_name group by col1.
```

### Data Format:

	1. .csv file for tables. File name table1.csv, table name is "table1" 

	2. metadata.txt file: having the structure for each table

	    eg. <begin_table> 
		<table_name> 
		<attribute1> 
		.... 
 
		<attributeN> 
		<end_table>  
		
### Sample input :
```
	python3 Sql_engine.py "SELECT * from table1 ORDER BY C;"
```
