import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

#Create a Spark Session
spark = SparkSession.builder \
        .appName("RemoveDuplicates") \
        .master("local[*]") \
        .getOrCreate()

data = [
    (1,"Alice", 2000),
    (2,"Bob", 3000),
    (3, "Alice", 2000),
    (4,"David",4000),
    (5,"Alice",5000),
    (6,"Bob",3000)
]
schema = ["ID", "Name", "Salary"]

df = spark.createDataFrame(data, schema)

#Remove duplicates using distinct
distinct_df = df.distinct()
distinct_df.show()

#Remove duplicate rows based on specific columns(Name, Salary)
drop_duplicates_df = df.dropDuplicates(["Name", "Salary"])
drop_duplicates_df.show()
