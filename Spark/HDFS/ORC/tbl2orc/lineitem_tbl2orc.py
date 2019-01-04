from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions
data = sc.textFile("/data/OLAP_Benchmark_data/lineitem.tbl")

spark.sql("set spark.sql.orc.impl=native")

fields=[
    StructField("l_orderkey",IntegerType(), True), 
    StructField("l_partkey",IntegerType(), True), 
    StructField("l_suppkey",IntegerType(), True), 
    StructField("l_linenumber",IntegerType(), True), 
    StructField("l_quantity",  DoubleType(), True), 
    StructField("l_extendedprice",  DoubleType(), True), 
    StructField("l_discount",  DoubleType(), True), 
    StructField("l_tax",  DoubleType(), True), 
  
    StructField("l_returnflag", StringType(), True),
    StructField("l_linestatus", StringType(), True),

    StructField("l_shipdate", StringType(), True),
    StructField("l_commitdate", StringType(), True),
    StructField("l_receiptdate", StringType(), True),

    StructField("l_shipinstruct", StringType(), True),
    StructField("l_shipmode", StringType(), True),
    StructField("l_comment", StringType(), True),
    StructField("l_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'l_orderkey':int(x[0]),\
        'l_partkey':int(x[1]),\
        'l_suppkey':int(x[2]),\
        'l_linenumber':int(x[3]),\

        'l_quantity':float(x[4]),\
        'l_extendedprice':float(x[5]),\
        'l_discount':float(x[6]),\
        'l_tax':float(x[7]),\
        
        'l_returnflag':x[8],\
        'l_linestatus':x[9],\
        'l_shipdate':x[10],\
        'l_commitdate':x[11],\
        'l_receiptdate':x[12],\
        'l_shipinstruct':x[13],\
        'l_shipmode':x[14],\
        'l_commen':x[15],\

        'l_dummy':x[16]})\
    .toDF(schema)
df.write.mode("overwrite").orc("hdfs://namenode:8020/lineitem.orc")
