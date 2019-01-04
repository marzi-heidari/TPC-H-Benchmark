from pyspark.sql.functions import *
from pyspark.sql.types import *



def reduce_fun(x,y):
  return x * (1 - y)

decrease = udf (lambda x, y: reduce_fun(x,y),IntegerType() )


sm = udf ( lambda x : ("SM CASE" or "SM BO" or "SM PAC" or "SM PKG") in x, BooleanType() )
md = udf ( lambda x :("MED BA" or "MED BO" or "MED PK" or "MED PACK") in x, BooleanType() )
lg = udf (lambda x :("LG CAS" or "LG BO" or "LG PAC" or "LG PKG") in x, BooleanType())


    q20=part.join(lineitem, lineitem.l_partkey == part.p_partkey)
      .filter("(l_shipmode == 'AIR' or  l_shipmode == 'AIR REG') and
        l_shipinstruct == 'DELIVER IN PERSON'")
      .filter(
        ((col("p_brand") == "Brand#12") &
          (sm(col("p_container"))) &
          (col("l_quantity") >= 1) &
          ( col("l_quantity") <= 11 )&
          (col("p_size")>= 1 ) & 
          (col("p_size") <= 5) 
           ) | 
          (
            (col("p_brand") == "Brand#23") &
            (md(col("p_container"))) &
            (col("l_quantity") >= 24) & 
            (col("l_quantity") <= 34) &
            (col("p_size") >= 1) & 
            (col("p_size") <= 15)
            ) |
            (
              (col("p_brand") == "Brand#34") &
              (lg(col("p_container"))) &
              (col("l_quantity")>= 20) & 
              (col("l_quantity") <= 30) &
              (col("p_size") >= 1) & 
              (col("p_size") <= 15)))
      .select(decrease(col("l_extendedprice"), col("l_discount")).alias("volume"))
.agg(sum("volume"))
