import time
from pyspark.sql.functions import *

order_filter = orders.filter(
    "o_orderdates < '1995-01-01' and o_orderdates >= '1994-01-01'")
temp = region.filter("r_name == 'MIDDLE EAST'")
.join(nation, region.r_regionkey == nation.n_regionkey)
.join(supplier, nation.n_nationkey == supplier.s_nationkey)
.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
temp2 = temp.select("n_name", "l_extendedprice", "l_discount", "l_orderkey", "s_nationkey")
            .join(order_filter, temp.l_orderkey == order_filter.o_orderkey)
temp3 = temp2
            .join(customer, (temp2.o_custkey == customer.c_custkey) & (temp2.s_nationkey == customer.c_nationkey))


q5 = temp3.select("n_name", (temp3.l_extendedprice *(1-temp3.l_discount)).alias("ue"))
            .groupBy("n_name")
            .agg(sum("ue").alias("revenue"))
            .sort(desc("revenue"))

startTimeQuery = time.clock()
q5.show()
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
