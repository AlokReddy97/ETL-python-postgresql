# ETL-python-postgresql
This Repo will Tranform csv files and insert the data to a postgresql table / postgresql database


![Flow chart ETL](https://github.com/AlokReddy97/ETL-python-postgresql/raw/main/Flow_chart_ETL.png)


python3 make_trimmedcsv.py zillow.csv 4

CSV file 'zillow.csv' processed successfully and saved as 'processed/zillow/zillow_2023-04-06_07-55-13.csv'.

[Errno 2] No such file or directory: 'archived/zillow'

[Errno 2] No such file or directory: 'archived/zillow'

True

False

Column # | Column Name | Data Type

-------- + ------------+ ----------

1        | Index        | INTEGER

2        | LivingSpacesqft | INTEGER

3        | Beds         | INTEGER

4        | Baths        | INTEGER

5        | Zip          | INTEGER

6        | Year         | INTEGER

7        | ListPrice    | INTEGER

8        | test         | INTEGER

Please enter the number of columns in the primary key: 2

Please enter the number of primary key column 1 (1-8): 1

Please enter the number of primary key column 2 (1-8): 5

Create Table Query: CREATE TABLE IF NOT EXISTS zillow (Index INTEGER, LivingSpacesqft INTEGER, Beds INTEGER, Baths INTEGER, Zip INTEGER, Year INTEGER, ListPrice INTEGER, test INTEGER, PRIMARY KEY (Index, Zip));

Do you want to create the table? Press 1 to confirm, any other key to abort: 1

Table created successfully!

CREATE TABLE

Insert Query: INSERT INTO zillow (Index, LivingSpacesqft, Beds, Baths, Zip, Year, ListPrice, test) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

Values: [(10, 1997, 3, 3, 32311, 2006, 295000, 1), (11, 2097, 4, 3, 32311, 2016, 290000, 1), (12, 3200, 5, 4, 32312, 1964, 465000, 1), (13, 4892, 5, 6, 32311, 2005, 799900, 1), (14, 1128, 2, 1, 32303, 1955, 89000, 1), (15, 1381, 3, 2, 32301, 2006, 143000, 1), (16, 4242, 4

