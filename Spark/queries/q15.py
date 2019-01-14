from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType




 revenue = lineitem.filter("l_shipdate >= '1993-01-01' and
      l_shipdate < '1993-04-01'")
      .select("l_suppkey", (col("l_extendedprice")*(1-col("l_discount"))).alias("value"))
      .groupBy("l_suppkey")
      .agg(sum("value").alias("total"))

 q15=revenue.agg(max("total").alias("max_total"))
      .join(revenue, col("max_total") == revenue.total)
      .join(supplier, col("l_suppkey") ==supplier.s_suppkey)
      .select("s_suppkey", "s_name", "s_address", "s_phone", "total")
.sort("s_suppkey")
