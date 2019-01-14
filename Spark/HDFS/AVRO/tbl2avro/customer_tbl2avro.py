from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions

spark.sql("set spark.sql.orc.impl=native")

data = sc.textFile("/data/OLAP_Benchmark_data/customer.tbl")
fields=[
    StructField("c_custkey",IntegerType(), True), 
    StructField("c_name",StringType(), True), 
    StructField("c_address",StringType(), True), 
    StructField("c_nationkey",IntegerType(), True), 
    StructField("c_phone", StringType(), True),
    StructField("c_acctbal", DoubleType(), True),
    StructField("c_mktsegment", StringType(), True),
    StructField("c_comment",StringType(), True),
    StructField("c_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'c_custkey':int(x[0]),\
        'c_name':x[1],\
        'c_address':x[2],\
        'c_nationkey':int(x[3]),\
        'c_phone':x[4],\
        'c_acctbal':float(x[5]),\
        'c_mktsegment':x[6],\
        'c_comment':x[7],\
        'c_dummy':x[8]})\
    .toDF(schema)
    
df.write.mode('overwrite').format("com.databricks.spark.avro").save("hdfs://namenode:8020/customer.avro")
