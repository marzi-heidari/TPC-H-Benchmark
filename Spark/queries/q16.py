from pyspark.sql.functions import *
from pyspark.sql.types import *

region=spark.read.parquet("hdfs://namenode:8020/region.parquet")
part=spark.read.parquet("hdfs://namenode:8020/part.parquet")
partsupp=spark.read.parquet("hdfs://namenode:8020/partsupp.parquet")
supplier=spark.read.parquet("hdfs://namenode:8020/supplier.parquet")
nation=spark.read.parquet("hdfs://namenode:8020/nation.parquet")
lineitem=spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
orders=spark.read.parquet("hdfs://namenode:8020/orders.parquet")
customer=spark.read.parquet("hdfs://namenode:8020/customer.parquet")



complains = udf ( lambda x :".*Customer.*Complaints.*" in x ,BooleanType())
polished = udf (lambda x : x.startswith("SMALL PLATED") ,BooleanType())
numbers = udf (lambda x : ("19" or "17" or "16" or "23" or "10" or "4" or "38" or "11") in str(x),BooleanType())
   parts_filter = part.filter((~(func.col("p_brand") == 'Brand#45')) & (~polished(func.col("p_type"))) & 
   numbers(func.col("p_size"))).select("p_partkey", "p_brand", "p_type", "p_size")

    q16= supplier.filter(~complains(func.col("s_comment")))
      .join(partsupp, func.col("s_suppkey") == partsupp.ps_suppkey)
      .select("ps_partkey", "ps_suppkey")
      .join(parts_filter, func.col("ps_partkey") == parts_filter.p_partkey)
      .groupBy("p_brand", "p_type", "p_size")
      .agg(countDistinct("ps_suppkey").alias("supplier_count"))
.sort(desc("supplier_count"), "p_brand", "p_type", "p_size")
