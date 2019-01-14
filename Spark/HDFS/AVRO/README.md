load dataframes

```part=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/part.avro")
partsupp=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/partsupp.avro")
supplier=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/supplier.avro")
nation=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/nation.avro")
lineitem=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/lineitem.avro")
orders=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/orders.avro")
customer=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/customer.avro")
region=spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/region.avro")

