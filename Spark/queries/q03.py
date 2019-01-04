import time
from pyspark.sql.functions import *


lineitem_date = lineitem.filter("l_shipdate > '1995-03-29'")
order_date = orders.filter("o_orderdates < '1995-03-29'")
furniture = customer.filter("c_mktsegment =='FURNITURE'")
temp = furniture.join(order_date, furniture.c_custkey == order_date.o_custkey)
temp2 = temp.select("o_orderkey", "o_orderdates", "o_shippriority")
            .join(lineitem_date, temp.o_orderkey == lineitem_date.l_orderkey)
q3 = temp2
            .select("l_orderkey", (temp2.l_extendedprice * (1 - temp2.l_discount)).alias("orders_count"), "o_orderdates", "o_shippriority")
            .groupBy("l_orderkey", "o_orderdates", "o_shippriority")
            .agg(sum("orders_count").alias("revenue"))
            .sort(desc("revenue"), "o_orderdates")
            .limit(10)
startTimeQuery = time.clock()
q3.show()
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
runTimeQuery
