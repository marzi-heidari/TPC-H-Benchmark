data = sc.textFile("/data/OLAP_Benchmark_data/supplier.tbl")
spark.sql("set spark.sql.orc.impl=native")
fields=[
    StructField("s_suppkey",IntegerType(), True), 
    StructField("s_name",StringType(), True), 
    StructField("s_address",StringType(), True), 
    StructField("s_nationkey",IntegerType(), True), 
    StructField("s_phone", StringType(), True),
    StructField("s_acctbal", DoubleType(), True),
    StructField("s_comment",StringType(), True),
    StructField("s_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         's_suppkey':int(x[0]),\
        's_name':x[1],\
        's_address':x[2],\
        's_nationkey':int(x[3]),\
        's_phone':x[4],\
        's_acctbal':float(x[5]),\
        's_comment':x[6],\
        's_dummy':x[7]})\
    .toDF(schema)
df.write.mode("overwrite").orc("hdfs://namenode:8020/supplier.orc")
