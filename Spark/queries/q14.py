from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def is_promo(x,y):
  if (x.startswith("PROMO")): return y
  else :  return 0

def reduce_fun(x,y):
  return x * (1 - y)

reduce = udf (lambda x, y: reduce_fun(x,y),IntegerType() )
promo = udf (lambda x , y: is_promo(x,y),IntegerType())

q14=  part.join(lineitem, (col("l_partkey") == col("p_partkey")) &
      (col("l_shipdate") >= '1995-09-01') &(col("l_shipdate") < '1995-10-01'))
      .select("p_type", (reduce(col("l_extendedprice"),col( "l_discount"))).alias("ue"))
.agg(sum(promo(col("p_type"),col("ue"))) * 100 / sum("ue"))
