import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, column, sum

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("ReadJsonFileExample")\
        .master("local[*]")\
        .getOrCreate()

customers_df = spark.read.format("json")\
             .option("multiline", True)\
             .option("header", True)\
             .option("inferSchema", True)\
             .option("path", "E://files/customers.json")\
             .load()

#customers_df.show()

transactions_df = spark.read.format("json")\
                    .option("multiline", True)\
                    .option("header", True)\
                    .option("inferSchema", True)\
                    .option("path","E://files/transactions.json")\
                    .load()

#transactions_df.show()
#Read data from JSON files, join two DataFrames (customers and transactions), group by month
#using MONTH() function, and calculate the total transaction amount per month. Handle null
#transaction values with default values and write the output to Parquet format

condition = customers_df["customer_id"] == transactions_df["customer_id"]

join_type = "inner"

joined_df = customers_df.join(transactions_df, condition, join_type)
extracted_df = joined_df.withColumn("month", month(column("transaction_date")))

filled_df = extracted_df.fillna({"amount": 000.00})

filled_df.groupby(column("month")).agg(sum(column("amount")).alias("total_transaction")).show()
