from pyspark import SparkConf, SparkContext


def fn_read_structure(line: str):
    l_input_arr = line.split(',')

    return float(l_input_arr[0]), float(l_input_arr[2])


conf = SparkConf().setAppName("customer-orders")
sc = SparkContext(conf=conf)

customer_orders = sc.textFile("file:///opt/spark-data/customer-orders.csv")
customerPreparedData = customer_orders.map(fn_read_structure)

customerSpendingAgg = customerPreparedData.reduceByKey(lambda x, y: round(x + y))
customerSpendingAggSorted = customerSpendingAgg.map(lambda x: (x[1], x[0])).sortByKey()
results = customerSpendingAggSorted.collect()

for oneCustomerResult in results:
    print(oneCustomerResult)
