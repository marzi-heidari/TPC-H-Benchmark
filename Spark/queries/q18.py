from pyspark.sql.functions import *
from pyspark.sql.types import *

q18= lineitem.groupBy("l_orderkey")
      .agg(sum("l_quantity").alias("sum_quantity"))
      .filter("sum_quantity > 313")
      .select(col("l_orderkey").alias("key"), "sum_quantity")
      .join(orders, orders.o_orderkey == col("key"))
      .join(lineitem,col("o_orderkey") == lineitem.l_orderkey)
      .join(customer, customer.c_custkey ==col("o_custkey"))
      .select("l_quantity", "c_name", "c_custkey", "o_orderkey", "o_orderdates", "o_totalprice")
      .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdates", "o_totalprice")
      .agg(sum("l_quantity"))
      .sort(desc("o_totalprice"), "o_orderdates")
.limit(100)
