import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("DailyStockPrices")\
        .master("local[*]")\
        .getOrCreate()

data = [
    (1,"Alice", 30),
    (2,"Bob", 25),
    (3, "Charlie", 35)
]
columns = ["id","name","age"]

#Create DateFrames
df1 = spark.createDataFrame(data, columns)
df1.show()

df1.withColumnRenamed("name", "full_name").show()

#Rename multiple columns
df2 = df1.withColumnRenamed("name", "full_name").withColumnRenamed("age", "years")
df2.show()

#Rename columns using selectExpr
df3 = df1.selectExpr("id", "name as full_name", "age as years")
df3.show()

#Rename all columns using toDF()
df4 = df1.toDF("id", "Full_Name", "Years")
df4.show()

#Rename in select using alias
df5 = df1.select(column("id"), column("name").alias("full_NamE"), column("age").alias("YeaRs"))
df5.show()