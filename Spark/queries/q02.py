from pyspark.sql.functions import *


middle_east = region.filter("'r_name' == 'MIDDLE EAST'")
      .join(nation, region.r_regionkey == nation.n_regionkey)
      .join(supplier, nation.n_nationkey == supplier.s_nationkey)
.join(partsupp, supplier.s_suppkey == partsupp.ps_suppkey)

tin = part.filter("p_size == 38").filter(part.p_type.endswith('%TIN'))
.join(middle_east, middle_east.ps_partkey == part.p_partkey)
minCost = tin.groupBy(tin.ps_partkey)
.agg(mintinps_supplycost).alias("min"))

q2=tin.join(minCost, tin.ps_partkey == minCost.ps_partkey)
      .filter(tin.ps_supplycost == minCost.min)
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort(desc("s_acctbal"), "n_name", "s_name", "p_partkey")
.limit(100)
startTimeQuery=time.clock()
q2.show()
endTimeQuery=time.clock()
runTimeQuery=endTimeQuery - startTimeQuery
