import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("CustomersOrdersExample")\
        .master("local[*]")\
        .getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org").setLevel(log4jLogger.Level.OFF)
log4jLogger.LogManager.getLogger("akka").setLevel(log4jLogger.Level.OFF)

#Perform Right Join
orders_data = [
        (1,101,"Delivered"),
        (2,102,"Pending"),
        (3,103,"Shipped"),
        (4,101,"Cancelled")
]
orders_columns = ["order_id","customer_id","order_status"]

customers_data = [
        (101,"Alice","New York"),
        (102,"Bob","Los Angeles"),
        (103,"Charlie","Chicago"),
        (104,"David","Houston")
]
customers_columns = ["customer_id","customer_name","customer_city"]

#Create DataFrames
orders_df = spark.createDataFrame(orders_data, orders_columns)

customers_df = spark.createDataFrame(customers_data, customers_columns)

#Perform a right Join (right_outer in Pyspark)
result_df = orders_df.join(customers_df, orders_df["customer_id"] == customers_df["customer_id"], "right_outer")
result_df.show()
