
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType



nation_filter = nation.filter(col("n_name").startswith( 'ARGENTINA'))
      .join(supplier, col("n_nationkey") == supplier.s_nationkey)
      .select("s_suppkey")
      .join(partsupp,col("s_suppkey") == partsupp.ps_suppkey)
      .select("ps_partkey", (col("ps_supplycost")*col( "ps_availqty")).alias("ue"))


sumRes = nation_filter.agg(sum("ue").alias("total_ue"))

q11=  nation_filter.groupBy("ps_partkey").agg(sum("ue").alias("part_ue"))
      .join(sumRes, col("part_ue") > col("total_ue")* 0.0001)
.sort(desc("part_ue"))
