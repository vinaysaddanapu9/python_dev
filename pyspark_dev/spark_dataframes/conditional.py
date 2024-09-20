from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import col, column, when

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("Conditional_Values")\
        .master("local[*]").getOrCreate()

data = [(1, "John", 28),(2, "Jane", 35),(3, "Doe", 22)]
schema = ["id", "name", "age"]

employee_df = spark.createDataFrame(data, schema)

employee_df.select(col("id"), col("name"), col("age"),
                   when(col("age") >= 18, "True").otherwise("False").alias("is_adult")).show()

#employee_df.show()