
from pyspark.sql.functions import *
from pyspark.sql.types import *


sub2 = udf ( lambda x:x[0:2] ,StringType())
phone = udf ( lambda x: ('30'or '24'or '31' or '38' or '25'or'34' or '37') in x,BooleanType() )


fcustomer = customer.select("c_acctbal", "c_custkey", sub2(col("c_phone")).alias("cntrycode")).filter(phone(col("cntrycode")))

avg_customer = fcustomer.filter("c_acctbal > 0.0").agg(avg("c_acctbal").alias("avg_acctbal"))

q22= orders
    .select("o_custkey")
     .join(fcustomer, col("o_custkey") == fcustomer.c_custkey, "right_outer")
     .filter(col("o_custkey").isNull()).join(avg_customer).filter("c_acctbal > avg_acctbal")
     .groupBy("cntrycode") .agg(count("c_acctbal"), sum("c_acctbal"))
     .sort("cntrycode")
