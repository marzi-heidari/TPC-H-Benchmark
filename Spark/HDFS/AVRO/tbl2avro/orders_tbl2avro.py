from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions
data = sc.textFile("/data/OLAP_Benchmark_data/orders.tbl")
spark.sql("set spark.sql.orc.impl=native")

fields=[
    StructField("o_orderkey",IntegerType(), True), 
    StructField("o_custkey",IntegerType(), True), 
    StructField("o_orderstatus", StringType(), True),
    StructField("o_totalprice",  DoubleType(), True), 

    StructField("o_orderdates", StringType(), True),
    StructField("o_orderpriority", StringType(), True),
    StructField("o_clerk", StringType(), True),
    StructField("o_shippriority", IntegerType(), True),
    StructField("o_comment", StringType(), True),
   
    StructField("o_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {
        'o_orderkey':int(x[0]),\
        'o_custkey':int(x[1]),\
        'o_orderstatus':x[2],\
        'o_totalprice':float(x[3]),\

        'o_orderdates':x[4],\
        'o_orderpriority':x[5],\
        'o_clerk':x[6],\
        'o_shippriority':int(x[7]),\
        'o_commen':x[8],\
        'o_dummy':x[9]})\
    .toDF(schema)
   df.write.mode('overwrite').format("com.databricks.spark.avro").save("hdfs://namenode:8020/orders.avro")
