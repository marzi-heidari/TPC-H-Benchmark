
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


nation_filter = nation.filter("n_name == 'ARGENTINA'")
      .join(supplier, func.col("n_nationkey") == supplier.s_nationkey)
      .select("s_suppkey")
      .join(partsupp,func.col("s_suppkey") == partsupp.ps_suppkey)
      .select("ps_partkey", (func.col("ps_supplycost")*func.col( "ps_availqty")).alias("ue"))


sumRes = nation_filter.agg(sum("ue").alias("total_ue"))

q11=  nation_filter.groupBy("ps_partkey").agg(sum("ue").alias("part_ue"))
      .join(sumRes, func.col("part_ue") > func.col("total_ue")* 0.0001)
.sort(desc("part_ue")
