from pyspark.sql import SparkSession
from pyspark.sql.functions import column, col

spark =  SparkSession.builder\
         .appName("Details_CSV")\
         .master("local[*]").getOrCreate()

df = spark.read.format("csv").option("header", True).option("path", "E:/Files/details.csv").load()

df.select(col("id"), column("Name")).show()
#df.show(truncate=False)
