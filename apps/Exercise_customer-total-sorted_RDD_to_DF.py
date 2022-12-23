from pyspark.sql import Row
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("customer-orders-DF") \
    .getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(customer_id=int(fields[0]), item_id=str(fields[1].encode("utf-8")),
               price=float(fields[2]))


# spark.sparkContext.setLogLevel("WARN")
lines = spark.sparkContext.textFile("file:///opt/spark-data/customer-orders.csv")
RDDRows = lines.map(mapper)

schemaCustomers = spark.createDataFrame(RDDRows)
schemaCustomers.createOrReplaceTempView("customers")

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
