data = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")
fields=[
    StructField("r_regionkey",IntegerType(), True), 
    StructField("r_name",StringType(), True), 

    StructField("r_comment",StringType(), True)]
schema=StructType(fields)
df = data.\
    map(lambda x: x.split("|")).\
    map(lambda x: {\
         'r_regionkey':int(x[0]),\
        'r_name':x[1],\
        'r_comment':x[2]})\
    .toDF(schema)
