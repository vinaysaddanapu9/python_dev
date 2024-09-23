import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("Categorizing_Values")\
        .master("local[*]").getOrCreate()

data = [(1, 1000),(2, 200),(3, 5000)]
schema = ["transaction_id", "amount"]

transactions_df = spark.createDataFrame(data, schema)

transactions_df.withColumn("category", when(col("amount") > 1000, "High")
                           .when((col("amount") > 500) & (col("amount") <= 1000), "Medium").otherwise("Low")).show()

#transactions_df.show()