
from pyspark.sql.functions import *
from pyspark.sql.types import *


getYear = udf ( lambda x: x[0:4] ,StringType())
expr = udf (lambda x,y,v,w:  x * (1 - y) - (v * w),IntegerType() )

linePart = part.filter(col("p_name").contains("yellow")).join(lineitem,col("p_partkey") == lineitem.l_partkey)

natSup = nation.join(supplier, col("n_nationkey") == supplier.s_nationkey)

q9=linePart.join(natSup, col("l_suppkey") == natSup.s_suppkey)
.join(partsupp,( col("l_suppkey") == partsupp.ps_suppkey) & (col("l_partkey") == partsupp.ps_partkey))
.join(orders, col("l_orderkey") == orders.o_orderkey)
.select("n_name", getYear("o_orderdates").alias("o_year"),expr(col("l_extendedprice"), col("l_discount"), col("ps_supplycost"),col("l_quantity")).alias("amount"))
.groupBy("n_name", "o_year").agg(sum("amount"))
.sort("n_name", desc("o_year"))

