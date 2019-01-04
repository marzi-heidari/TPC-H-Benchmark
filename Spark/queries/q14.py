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

def is_promo(x,y):
  if (x.startswith("PROMO")): return y
  else :  return 0

def reduce_fun(x,y):
  return x * (1 - y)

reduce = udf (lambda x, y: reduce_fun(x,y),IntegerType() )
promo = udf (lambda x , y: is_promo(x,y),IntegerType())

q14=  part.join(lineitem, (func.col("l_partkey") == func.col("p_partkey")) &
      (func.col("l_shipdate") >= '1995-09-01') &(func.col("l_shipdate") < '1995-10-01'))
      .select("p_type", (reduce(func.col("l_extendedprice"),func.col( "l_discount"))).alias("ue"))
.agg(sum(promo(func.col("p_type"),func.col("ue"))) * 100 / sum("ue"))
