import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, sum, round

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("PercentageExample")\
        .master("local[*]")\
        .getOrCreate()

data = [
    (1, "Alice", "HR", 5000),
    (2, "Bob", "HR", 7000),
    (3, "Charlie", "IT", 10000),
    (4, "David", "IT", 8000),
    (5, "Eve", "IT", 6000)
]

columns = ["Employee_ID", "Name", "Department", "Salary"]

employee_df  = spark.createDataFrame(data, columns)
employee_df.show()

#calculate the percentage of total salary that each employee contributes to their respective department.
aggregated_df = employee_df.groupby(column("Department")).agg(sum("Salary").alias("sum_salary"))

joined_df = employee_df.join(aggregated_df, employee_df["Department"] == aggregated_df["Department"], "inner")

joined_df.withColumn("Salary_Percentage", round((column("Salary")/column("sum_salary"))*100, 2)).show()
