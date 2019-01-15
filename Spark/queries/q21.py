from pyspark.sql.functions import *
from pyspark.sql.types import *

   
fsupplier = supplier.select("s_suppkey", "s_nationkey", "s_name")

plineitem = lineitem.select("l_suppkey", "l_orderkey", "l_receiptdate", "l_commitdate")


flineitem = plineitem.filter("l_receiptdate > l_commitdate")
  

line1 = plineitem.groupBy("l_orderkey")
      .agg(countDistinct("l_suppkey").alias("suppkey_count"), max("l_suppkey").alias("suppkey_max"))
      .select(col("l_orderkey").alias("key"), "suppkey_count", "suppkey_max")

line2 = flineitem.groupBy("l_orderkey")
      .agg(countDistinct("l_suppkey").alias("suppkey_count"), max("l_suppkey").alias("suppkey_max"))
      .select(col("l_orderkey").alias("key"), "suppkey_count", "suppkey_max")

forder = orders.select("o_orderkey", "o_orderstatus").filter("o_orderstatus == 'F'")

q21= nation.filter(col("n_name").startswith('GERMANY')) # it has to be 'startswith' instead of '=='. beacause nation names contain space
      .join(fsupplier, col("n_nationkey") == fsupplier.s_nationkey)
      .join(flineitem, col("s_suppkey") == flineitem.l_suppkey)
      .join(forder, col("l_orderkey") == forder.o_orderkey)
      .join(line1, col("l_orderkey") == line1.key)
      .filter((col("suppkey_count") > 1) | ((col("suppkey_count") == 1 )& (col("l_suppkey") == col("suppkey_max"))))
      .select("s_name", "l_orderkey", "l_suppkey")
      .join(line2, col("l_orderkey") == line2.key, "left_outer")
      .select("s_name", "l_orderkey", "l_suppkey", "suppkey_count", "suppkey_max")
      .filter((col("suppkey_count") == 1 )& (col("l_suppkey") == col("suppkey_max")))
      .groupBy("s_name")
      .agg(count("l_suppkey").alias("numwait"))
      .sort(desc("numwait"), "s_name")
.limit(100)
