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






q18= lineitem.groupBy("l_orderkey")
      .agg(sum("l_quantity").alias("sum_quantity"))
      .filter("sum_quantity > 313")
      .select(func.col("l_orderkey").alias("key"), "sum_quantity")
      .join(orders, orders.o_orderkey == func.col("key"))
      .join(lineitem,func.col("o_orderkey") == lineitem.l_orderkey)
      .join(customer, customer.c_custkey ==func.col("o_custkey"))
      .select("l_quantity", "c_name", "c_custkey", "o_orderkey", "o_orderdates", "o_totalprice")
      .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdates", "o_totalprice")
      .agg(sum("l_quantity"))
      .sort(desc("o_totalprice"), "o_orderdates")
.limit(100)
