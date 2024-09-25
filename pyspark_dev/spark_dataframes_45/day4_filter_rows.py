import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

#Create a Spark Session
spark = SparkSession.builder \
       .appName("FilterRowsExample")\
       .master("local[*]")\
       .getOrCreate()

data = [
    (1,"Alice", 4000),
    (2,"Bob", 6000),
    (3,"Charlie", 5500),
    (4,"David",4500),
    (5,"Eve",7000),
]
schema = ["id", "name", "salary"]

#Create DataFrame
df = spark.createDataFrame(data, schema)

#Filter rows where salary is greater than 5000
filtered_df = df.filter(column("salary") > 5000).select("name")

filtered_df.show()
#df.show()