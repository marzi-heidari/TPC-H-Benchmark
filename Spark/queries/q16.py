from pyspark.sql.functions import *
from pyspark.sql.types import *


complains = udf ( lambda x :".*Customer.*Complaints.*" in x ,BooleanType())
polished = udf (lambda x : x.startswith("SMALL PLATED") ,BooleanType())
numbers = udf (lambda x : ("19" or "17" or "16" or "23" or "10" or "4" or "38" or "11") in str(x),BooleanType())
   parts_filter = part.filter((~(col("p_brand") == 'Brand#45')) & (~polished(col("p_type"))) & 
   numbers(col("p_size"))).select("p_partkey", "p_brand", "p_type", "p_size")

    q16= supplier.filter(~complains(col("s_comment")))
      .join(partsupp, col("s_suppkey") == partsupp.ps_suppkey)
      .select("ps_partkey", "ps_suppkey")
      .join(parts_filter, col("ps_partkey") == parts_filter.p_partkey)
      .groupBy("p_brand", "p_type", "p_size")
      .agg(countDistinct("ps_suppkey").alias("supplier_count"))
.sort(desc("supplier_count"), "p_brand", "p_type", "p_size")
