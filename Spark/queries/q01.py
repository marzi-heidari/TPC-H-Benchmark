import time
from pyspark.sql.functions import *

q1 = lineitem.filter("l_shipdate <= '1998-11-27'")
.groupBy("l_returnflag", "l_linestatus")
.agg(sum("l_quantity"), sum("l_extendedprice"),
     sum(col("l_extendedprice") * (1 - col("l_discount"))),
     sum(col("l_extendedprice") * (1 - col("l_discount")*(1 + col("l_tax")))),
     avg("l_quantity"),
     avg("l_extendedprice"),
     avg("l_discount"),
     count("l_quantity"))
.sort("l_returnflag", "l_linestatus")

startTimeQuery = time.clock()
q1.show()
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
runTimeQuery
