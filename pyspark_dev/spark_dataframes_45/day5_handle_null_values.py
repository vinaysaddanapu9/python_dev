import os

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
       .appName("HandleNullValues")\
       .master("local[*]")\
       .getOrCreate()

data = [("Alice", 30),
 ("Bob", None),
 ("Catherine", 25),
 (None, 35),
 ("Eve", None)]

columns = ["Name", "Age"]

#Create DataDataFrame
df = spark.createDataFrame(data, columns)

df.show()

#To Drop rows where the age column has null values using dropna
df.dropna(subset= ["Age"]).show()

#To fill null values with default values
df.fillna({"Age": 0}).show()

#Replacing null values with na.fill()
df.na.fill(0, subset= ["Age"]).show()

#Filter out rows with null values in the age column
df.filter(df.Age.isNotNull()).show()
