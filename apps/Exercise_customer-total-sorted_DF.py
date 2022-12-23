from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("customer-orders-DF") \
    .getOrCreate()

schema = StructType([StructField("customer_id", IntegerType(), True),
                     StructField("item_id", FloatType(), True),
                     StructField("price", FloatType(), True)])

# spark.sparkContext.setLogLevel("WARN")
lines = spark.read.schema(schema).csv("file:///opt/spark-data/customer-orders.csv")
lines.createOrReplaceTempView("customers")

# SQL can be run over DataFrames that have been registered as a table.
customersDataAgg = spark.sql("SELECT "
                             "  customer_id, Round(sum(price),2) as total_spendings "
                             "FROM "
                             "  customers "
                             "GROUP BY "
                             "  customer_id "
                             "ORDER BY "
                             "  total_spendings ")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for one_customer in customersDataAgg.collect():
    print(one_customer.total_spendings, one_customer.customer_id)

spark.stop()
