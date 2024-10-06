import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import desc, column, rank

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("RankExample")\
        .master("local[*]")\
        .getOrCreate()

data = [
     (1,'Alice','HR',5000),
     (2,'Bob','HR',6000),
     (3,'Charlie','IT',7000),
     (4,'David','IT',9000),
     (5,'Eve','HR',5500),
     (6,'Frank','IT',8000)
]

columns = ["EmployeeID", "Name", "Department", "Salary"]

#Create DataFrame
df = spark.createDataFrame(data, columns)

#df.show()

#Create a window
window = Window.partitionBy("Department").orderBy(desc("salary"))
ranked_df = df.select(column("*"), rank().over(window).alias("rank"))

#Show the result
ranked_df.show()


