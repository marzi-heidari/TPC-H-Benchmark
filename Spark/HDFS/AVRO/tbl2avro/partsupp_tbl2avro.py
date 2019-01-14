from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions
data = sc.textFile("/data/OLAP_Benchmark_data/partsupp.tbl")
fields=[
    StructField("ps_partkey",IntegerType(), True), 
    StructField("ps_suppkey",IntegerType(), True), 
    StructField("ps_availqty",IntegerType(), True), 
    StructField("ps_supplycost",  DoubleType(), True), 

    StructField("ps_comment", StringType(), True),
    StructField("ps_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'ps_partkey':int(x[0]),\
        'ps_suppkey':int(x[1]),\
        'ps_availqty':int(x[2]),\
        'ps_supplycost':float(x[3]),\
        'ps_comment':x[4],\
        'ps_dummy':x[5]})\
    .toDF(schema)
df.write.mode('overwrite').format("com.databricks.spark.avro").save("hdfs://namenode:8020/partsupp.avro")
