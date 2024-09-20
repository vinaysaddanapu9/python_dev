import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("Categorizing_Values")\
        .master("local[*]").getOrCreate()

data = [(1, 85),(2, 42),(3, 73)]
schema = ["student_id", "score"]

grades_df = spark.createDataFrame(data, schema)

grades_df.select(col("student_id"), col("score"),
                 when(col("score") >= 50, "Pass").otherwise("Fail").alias("grade")).show()

#grades_df.show()
