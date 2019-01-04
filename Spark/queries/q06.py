
from pyspark.sql.functions import *

 q6=lineitem.filter("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.07 and l_discount <= 0.09 and l_quantity < 24")
.agg(sum(lineitem.l_extendedprice * lineitem.l_discount))
