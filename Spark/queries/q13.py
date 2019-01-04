from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

special = udf ( lambda x :".*special.*requests.*" in str(x) ,BooleanType())

q13=customer.join(orders, (col("c_custkey") == orders.o_custkey) & (~special(col("o_comment"))), "left_outer") 
            .groupBy(col("o_custkey")) 
            .agg(count(col("o_orderkey")).alias("c_count"))
            .groupBy("c_count") .agg(count(col("o_custkey")).alias("custdist"))
            .sort(desc("custdist"), desc("c_count")) 
