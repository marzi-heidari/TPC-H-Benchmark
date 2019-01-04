import time
from pyspark.sql.functions import *

orders_filter = orders.filter(
    "o_orderdates >= '1993-07-01' and o_orderdates < '1993-10-01'")
lineitem_filter = lineitem.filter(
    "l_commitdate < l_receiptdate").select("l_orderkey").distinct()

lineitem_filter.join(orders_filter, lineitem_filter.l_orderkey === orders_filter.o_orderkey)
.groupBy("o_orderpriority")
.agg(count("o_orderpriority"))
.sort("o_orderpriority")
startTimeQuery = time.clock()
q4.show()
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
runTimeQuery
