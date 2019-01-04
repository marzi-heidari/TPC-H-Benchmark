
## Before queries 
### 1. connect to postgres bash 

```docker exec   -it  marzie-postgres psql```

### 2. create tables using ``create_table.sql``
### 3. load data
####  3.1.   converte ``.tbl`` files to ``.csv`` file 
(In the end I think it wasn't neccessary. I did it because all scripts that I found online for loading data to postgres were from csv files)

```
for tbl in *.tbl ; do
    sed 's/|$//' "$tbl" > /another/path/"${tbl%.tbl}".csv
    echo "$tbl"
done
```

####  3.2.   copy ``.csv`` files from server to docker container (Yep! it took ages)

```docker cp  {table_name}.csv marzie_postgres:/{table_name}.csv```
### 4. finally load data to tables:

```\copy {table_name} FROM '{table_name}.csv' (FORMAT CSV, DELIMITER '|');```



