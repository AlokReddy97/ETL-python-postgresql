# ETL-python-postgresql
This Repo will Tranform csv files and insert the data to a postgresql table / postgresql database


![Flow chart ETL](https://github.com/AlokReddy97/ETL-python-postgresql/raw/main/Flow_chart_ETL.png)

```
zillow.csv

"Index", "Living Space (sq ft)", "Beds", "Baths", "Zip", "Year", "List Price ($)"

10, 1997, 3, 3,   32311, 2006, 295000

11, 2097, 4, 3,   32311, 2016, 290000

12, 3200, 5, 4,   32312, 1964, 465000

13, 4892, 5, 6,   32311, 2005, 799900

14, 1128, 2, 1,   32303, 1955,  89000

15, 1381, 3, 2,   32301, 2006, 143000

16, 4242, 4, 5,   32303, 2007, 569000

17, 2533, 3, 2,   32310, 1991, 365000

18, 1158, 3, 2,   32303, 1993, 155000

19, 2497, 4, 4,   32309, 1990, 289000

20, 4010, 5, 3,   32309, 2002, 549900

```


## Python terminal commands and output 
```python3 make_trimmedcsv.py zillow.csv 4 ```
### Terminal output 
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

Values: [(10, 1997, 3, 3, 32311, 2006, 295000, 1), (11, 2097, 4, 3, 32311, 2016, 290000, 1), (12, 3200, 5, 4, 32312, 1964, 465000, 1), (13, 4892, 5, 6, 32311, 2005, 799900, 1), (14, 1128, 2, 1, 32303, 1955, 89000, 1), (15, 1381, 3, 2, 32301, 2006, 143000, 1), (16, 4242, 4, 5, 32303, 2007, 569000, 1), (17, 2533, 3, 2, 32310, 1991, 365000, 1), (18, 1158, 3, 2, 32303, 1993, 155000, 1), (19, 2497, 4, 4, 32309, 1990, 289000, 1), (20, 4010, 5, 3, 32309, 2002, 549900, 1)]

Do you want to insert all rows? Press 1 to confirm, any other key to skip: 1

CSV data inserted successfully!

INSERT 0 1

CSV file saved in archived/zillow/zillow_2023-04-06_07-55-34.csv


## Data inserted to the database

db1=> ```select * from zillow;```

 index | livingspacesqft | beds | baths |  zip  | year | listprice | test

-------+-----------------+------+-------+-------+------+-----------+------

    10 |            1997 |    3 |     3 | 32311 | 2006 |    295000 |    1

    11 |            2097 |    4 |     3 | 32311 | 2016 |    290000 |    1

    12 |            3200 |    5 |     4 | 32312 | 1964 |    465000 |    1

    13 |            4892 |    5 |     6 | 32311 | 2005 |    799900 |    1

    14 |            1128 |    2 |     1 | 32303 | 1955 |     89000 |    1

    15 |            1381 |    3 |     2 | 32301 | 2006 |    143000 |    1

    16 |            4242 |    4 |     5 | 32303 | 2007 |    569000 |    1

    17 |            2533 |    3 |     2 | 32310 | 1991 |    365000 |    1

    18 |            1158 |    3 |     2 | 32303 | 1993 |    155000 |    1

    19 |            2497 |    4 |     4 | 32309 | 1990 |    289000 |    1

    20 |            4010 |    5 |     3 | 32309 | 2002 |    549900 |    1

(11 rows)

```
zillow.csv

Some ,instruction

To ,confuse, you

testing ,all, cases, csvs

"Index", "Living Space (sq ft)", "Beds", "Baths", "Zip", "Year", "List Price ($)"

 1, 3333, 3, 3.5, 32312, 1981, 250000

 2, 1628, 3, 2,   32308, 2009, 185000

 3, 3824, 5, 4,   32312, 1954, 399000

 4, 1137, 3, 2,   32309, 1993, 150000

 5, 4590, 6, 4,   32309, 1973, 315000

 6, 2893, 4, 3,   32312, 1994, 699000

 7, 3631, 4, 3,   32309, 1996, 649000

 8, 9999, 4, 3,   32312, 2016, 399000

 9, 2400, 4, 4,   32312, 2002, 613000```

## python terminal command
```python3 make_trimmedcsv.py zillow.csv 4```
### Terminal output

CSV file 'zillow.csv' processed successfully and saved as 'processed/zillow/zillow_2023-04-06_08-13-36.csv'.

Headers of both CSV files do not match.

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice']

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice', 'test']

Headers are not matching so quitting


## python terminal command
```python3 make_trimmedcsv.py zillow.csv 4```
### Terminal output

CSV file 'zillow.csv' processed successfully and saved as 'processed/zillow/zillow_2023-04-06_08-19-41.csv'.

Headers of both CSV files match.

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice', 'test']

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice', 'test']

The CSV file 'processed/zillow' is the same as the most recent archived CSV file 'zillow_2023-04-06_07-55-34.csv'.

Headers of both CSV files match.

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice', 'test']

['Index', 'LivingSpacesqft', 'Beds', 'Baths', 'Zip', 'Year', 'ListPrice', 'test']

The CSV file 'processed/zillow' is the same as the most recent archived CSV file 'zillow_2023-04-06_07-55-34.csv'.

False

True

Archive file is not empty. Table updation is done

['index', 'zip']

0 rows updated and 9 rows inserted.

Error occurred: cursor already closed

## Data inserted to the database

db1=> ```select * from zillow;```

db1=> select * from zillow;

 index | livingspacesqft | beds | baths |  zip  | year | listprice | test

-------+-----------------+------+-------+-------+------+-----------+------

    10 |            1997 |    3 |     3 | 32311 | 2006 |    295000 |    1

    11 |            2097 |    4 |     3 | 32311 | 2016 |    290000 |    1

    12 |            3200 |    5 |     4 | 32312 | 1964 |    465000 |    1

    13 |            4892 |    5 |     6 | 32311 | 2005 |    799900 |    1

    14 |            1128 |    2 |     1 | 32303 | 1955 |     89000 |    1

    15 |            1381 |    3 |     2 | 32301 | 2006 |    143000 |    1

    16 |            4242 |    4 |     5 | 32303 | 2007 |    569000 |    1

    17 |            2533 |    3 |     2 | 32310 | 1991 |    365000 |    1

    18 |            1158 |    3 |     2 | 32303 | 1993 |    155000 |    1

    19 |            2497 |    4 |     4 | 32309 | 1990 |    289000 |    1

    20 |            4010 |    5 |     3 | 32309 | 2002 |    549900 |    1

     1 |            2097 |    4 |     3 | 32311 | 2016 |    290000 |    9

     2 |            3200 |    5 |     4 | 32312 | 1964 |    465000 |    8

     3 |            4892 |    5 |     6 | 32311 | 2005 |    799900 |    7

     4 |            1128 |    2 |     1 | 32303 | 1955 |     89000 |    6

     5 |            1381 |    3 |     2 | 32301 | 2006 |    143000 |    5

     6 |            4242 |    4 |     5 | 32303 | 2007 |    569000 |    4

     7 |            2533 |    3 |     2 | 32310 | 1991 |    365000 |    4

     8 |            1158 |    3 |     2 | 32303 | 1993 |    155000 |    3

     9 |            2497 |    4 |     4 | 32309 | 1990 |    289000 |    2

(20 rows)
