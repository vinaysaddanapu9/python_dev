import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("WriteDataToCSVFile")\
        .master("local[*]")\
        .getOrCreate()

data = [
    ("John",28,"Engineer"),
    ("Anna",23,"Doctor"),
    ("Mike",35,"Professor")
]

columns = ["name", "age", "occupation"]

#Create a DataFrame
df = spark.createDataFrame(data, columns)
df.show()

df.write.csv("E:/Files/oct1/sample_data.csv", header=True)