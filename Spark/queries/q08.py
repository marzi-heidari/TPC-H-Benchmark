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


def india( x, y):
  if ("INDIA" in x): return y 
  else :  return 0

is_india = udf (lambda x, y : india(x,y) ,IntegerType())
getYear = udf ( lambda x: x[0:4] ,StringType())

 region_filter = region.filter("r_name == 'ASIA'")
 orders_filter = orders.filter("o_orderdates <= '1996-12-31' and o_orderdates >= '1995-01-01'")
 part_filter = part.filter("p_type == 'PROMO BRUSHED COPPER'")

 nation_join = nation.join(supplier, nation.n_nationkey == supplier.s_nationkey)

 line_select = lineitem.select("l_partkey", "l_suppkey", "l_orderkey",
      (func.col("l_extendedprice")*(1- func.col("l_discount"))).alias("volume")).
      join(part_filter, func.col("l_partkey") == part_filter.p_partkey)
      .join(nation_join, func.col("l_suppkey") == nation_join.s_suppkey)

q8=nation.join(region_filter, func.col("n_regionkey") == region_filter.r_regionkey)
      .select("n_nationkey")
      .join(customer, func.col("n_nationkey") == customer.c_nationkey)
      .select("c_custkey")
      .join(orders_filter, func.col("c_custkey") == orders_filter.o_custkey)
      .select("o_orderkey", "o_orderdates")
      .join(line_select, func.col("o_orderkey") == line_select.l_orderkey)
      .select(getYear("o_orderdates").alias("o_year"), "volume",
        is_india("n_name", "volume").alias("case_volume"))
      .groupBy("o_year")
      .agg(sum("case_volume") / sum("volume"))
.sort("o_year")
