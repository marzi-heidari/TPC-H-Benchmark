from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

region=spark.read.parquet("hdfs://namenode:8020/region.parquet")
part=spark.read.parquet("hdfs://namenode:8020/part.parquet")
partsupp=spark.read.parquet("hdfs://namenode:8020/partsupp.parquet")
supplier=spark.read.parquet("hdfs://namenode:8020/supplier.parquet")
nation=spark.read.parquet("hdfs://namenode:8020/nation.parquet")
lineitem=spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
orders=spark.read.parquet("hdfs://namenode:8020/orders.parquet")
customer=spark.read.parquet("hdfs://namenode:8020/customer.parquet")


 revenue = lineitem.filter("l_shipdate >= '1993-01-01' and
      l_shipdate < '1993-04-01'")
      .select("l_suppkey", (func.col("l_extendedprice")*(1-func.col("l_discount"))).alias("value"))
      .groupBy("l_suppkey")
      .agg(sum("value").alias("total"))

 q15=revenue.agg(max("total").alias("max_total"))
      .join(revenue, func.col("max_total") == revenue.total)
      .join(supplier, func.col("l_suppkey") ==supplier.s_suppkey)
      .select("s_suppkey", "s_name", "s_address", "s_phone", "total")
.sort("s_suppkey")
