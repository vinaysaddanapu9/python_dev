import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("SimpleArithmeticExample")\
        .master("local[*]")\
        .getOrCreate()

data = [(10, 5), (20, 4), (30, 3),(40, 2), (50, 1)]

#Create DataFrame
df = spark.createDataFrame(data, ["col1", "col2"])

#Add the values of col1 and col2
df1 = df.withColumn("add", column("col1") + column("col2"))
df1.show()
#Subtract the values of col1 from col2
df2 = df.withColumn("subtract", column("col1") - column("col2"))
df2.show()
#Multiply the values of col1 and col2
df3 = df.withColumn("multiply", column("col1") * column("col2"))
df3.show()
#Divide the values of col1 by col2 (assuming no division by zero)
df4 = df.withColumn("divide", column("col1") / column("col2"))
df4.show()