CREATE INDEX i_p_partkey ON part (p_partkey); 
CREATE INDEX i_s_suppkey ON supplier (s_suppkey); 
CREATE INDEX i_c_custkey ON customer (c_custkey); 
CREATE INDEX i_o_orderkey ON orders (o_orderkey); 


CREATE INDEX i_n_regionkey ON nation (n_regionkey); 
CREATE INDEX i_s_nationkey ON supplier (s_nationkey); 
CREATE INDEX i_c_nationkey ON customer (c_nationkey); 
CREATE INDEX i_ps_suppkey ON partsupp (ps_suppkey); 
CREATE INDEX i_ps_partkey ON partsupp (ps_partkey); 
CREATE INDEX i_o_custkey ON orders (o_custkey); 
CREATE INDEX i_l_orderkey ON lineitem (l_orderkey); 
CREATE INDEX i_l_suppkey_partkey ON lineitem (l_partkey, l_suppkey); 


CREATE INDEX i_l_shipdate ON lineitem (l_shipdate); 
CREATE INDEX i_l_partkey ON lineitem (l_partkey); 
CREATE INDEX i_l_suppkey ON lineitem (l_suppkey); 
CREATE INDEX i_l_receiptdate ON lineitem (l_receiptdate); 
CREATE INDEX i_l_orderkey_quantity ON lineitem (l_orderkey, l_quantity); 
CREATE INDEX i_o_orderdate ON orders (o_orderdate); 
CREATE INDEX i_l_commitdate ON lineitem (l_commitdate);  

