import time
from pyspark.sql.functions import *


getYear = udf(lambda x: x[0:4], StringType())

nation_filter = nation.filter((col("n_name").startswith(
    'ROMANIA')) | (col("n_name").startswith('INDIA')))
lineitem_filter = lineitem.filter(
    "l_shipdate >= '1995-01-01' and l_shipdate <= '1996-12-31'")

supplier_nation = nation_filter.join(
    supplier, nation_filter.n_nationkey == supplier.s_nationkey)
.join(lineitem_filter, col("s_suppkey") == lineitem_filter.l_suppkey)
.select(col("n_name").alias("supp_nation"), "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate")

q7 = nation_filter
.join(customer, nation_filter.n_nationkey == customer.c_nationkey)
.join(orders, col("c_custkey") == orders.o_custkey)
.select(col("n_name").alias("cust_nation"), "o_orderkey")
.join(supplier_nation, col("o_orderkey") == supplier_nation.l_orderkey)
.filter((col("supp_nation").startswith('ROMANIA')) &
        (col("cust_nation").startswith('INDIA')) |
        (col("supp_nation").startswith('INDIA')) &
        (col("cust_nation").startswith('ROMANIA')))
.select("supp_nation", "cust_nation", getYear("l_shipdate").alias("l_year"), (col("l_extendedprice") * (1 - col("l_discount"))).alias("volume")).groupBy("supp_nation", "cust_nation", "l_year")
.agg(sum("volume") .alias("revenue"))
.sort("supp_nation", "cust_nation", "l_year")
startTimeQuery = time.clock()
q7.show()
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
