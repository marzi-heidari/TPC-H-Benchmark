from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions
data = sc.textFile("/data/OLAP_Benchmark_data/part.tbl")
spark.sql("set spark.sql.orc.impl=native")

fields=[
    StructField("p_partkey",IntegerType(), True), 
    StructField("p_name",StringType(), True), 
    StructField("p_mfgr",StringType(), True), 
    StructField("p_brand",StringType(), True), 
    StructField("p_type", StringType(), True),
    StructField("p_size", IntegerType(), True),
    StructField("p_container", StringType(), True),
    StructField("p_retailprice",DoubleType(), True),
    StructField("p_comment",StringType(), True),
    StructField("p_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'p_partkey':int(x[0]),\
        'p_name':x[1],\
        'p_mfgr':x[2],\
        'p_brand':x[3],\
        'p_type':x[4],\
        'p_size':int(x[5]),\
        'p_container':x[6],\
        'p_retailprice':float(x[7]),\
        'p_comment':x[8],\
        'ps_dummy':x[9]})\
    .toDF(schema)
    df.write.mode("overwrite").orc("hdfs://namenode:8020/part.orc")
