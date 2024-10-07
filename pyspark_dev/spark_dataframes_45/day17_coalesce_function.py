import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("CoalesceExample")\
        .master("local[*]")\
        .getOrCreate()

data = [(1,'Apple'), (2,'Banana'), (3,'Orange'), (4,'Grapes'),(5,'Mango'),(6,'Pineapple')]
columns = ["Id", "Fruit"]

#Create a DataFrame
df = spark.createDataFrame(data, columns)
#df.show()

#Check the current number of partitions
print("Initial number of partitions:", df.rdd.getNumPartitions())

# Given a PySpark DataFrame with 8 partitions, use the coalesce function to reduce the number of partitions to 4.

coalesce_df = df.coalesce(2)
print("No.of partitions after coalesce:", coalesce_df.rdd.getNumPartitions())

coalesce_df.show()
