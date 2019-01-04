from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


lineitem_filter = lineitem.filter("l_returnflag == 'R'")
q10=orders.filter("o_orderdates < '1994-01-01' and o_orderdates >= '1993-10-01'")
      .join(customer,col("o_custkey") == customer.c_custkey)
      .join(nation,col("c_nationkey") == nation.n_nationkey)
      .join(lineitem_filter,col("o_orderkey") == lineitem_filter.l_orderkey)
      .select("c_custkey", "c_name",
       (col("l_extendedprice")*(1-col( "l_discount"))).alias("volume"),
        "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
      .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
      .agg(sum("volume").alias("revenue"))
      .sort(desc("revenue"))
.limit(20)
