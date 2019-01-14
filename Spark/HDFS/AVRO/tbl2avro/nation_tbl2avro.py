data = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")
spark.sql("set spark.sql.orc.impl=native")

fields=[
    StructField("n_nationkey",IntegerType(), True), 
    StructField("n_name",StringType(), True), 
    StructField("n_regionkey",IntegerType(), True), 

    StructField("n_comment",StringType(), True),
  ]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'n_nationkey':int(x[0]),\
        'n_name':x[1],\
        'n_regionkey':int(x[2]),\
        'n_comment':x[3]})\
    .toDF(schema)
df.write.mode('overwrite').format("com.databricks.spark.avro").save("hdfs://namenode:8020/nation.avro")
