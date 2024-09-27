import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, column, when

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

# Initialize Spark session
spark = SparkSession.builder\
        .appName("DataFrameExample")\
        .master("local[*]")\
        .getOrCreate()

# Create DataFrame
data = [("John", 28, "New York"), ("Sarah", 24, "London"), ("Michael", 30, "Sydney")]
columns = ["Name", "Age", "City"]

df = spark.createDataFrame(data, columns)
df.show()

#1. Using lit() for a constant value

#Adding a new column with constant value
df_with_country = df.withColumn("Country", lit("USA"))
df_with_country.show()

#2.Adding a new column with conditional logic
df.withColumn("Country", when(column("City") == "New York", "USA") \
                        .when(column("City") == "London", "UK") \
                        .when(column("City") == "Sydney", "Australia") \
                        .otherwise("Unknown")).show()

#3. Using a dictionary for mapping and replace()

#Using a dictionary for mapping city to country
city_to_country = {"New York":"USA", "London":"UK", "Sydney":"Australia"}
df.replace(city_to_country, subset=["City"]).withColumnRenamed("City", "Country").show()