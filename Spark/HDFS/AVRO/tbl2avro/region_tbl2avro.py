data = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")
spark.sql("set spark.sql.orc.impl=native")
fields=[
    StructField("r_regionkey",IntegerType(), True), 
    StructField("r_name",StringType(), True), 

    StructField("r_comment",StringType(), True),
    StructField("r_dummy", StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'r_regionkey':int(x[0]),\
        'r_name"':x[1],\
        'r_comment':x[2],\
        'r_dummy':x[3]})\
    .toDF(schema)
df.write.mode('overwrite').format("com.databricks.spark.avro").save("hdfs://namenode:8020/region.avro")
