import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, when

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("ReplaceDepartmentExample")\
        .master("local[*]")\
        .getOrCreate()

# Sample data
data = [ (1, "John", "Finance"), (2, "Alice", "HR"),
 (3, "Bob", "Finance"), (4, "Carol", "IT") ]

# Create DataFrame
columns = ["EmployeeID", "Name", "Department"]

df = spark.createDataFrame(data, columns)

#Replace the department name "Finance" with "Financial Services" in the DataFrame

replaced_df = df.withColumn("Department", when((column("Department") == "Finance"), "Financial Services").otherwise(column("Department")))
replaced_df.show()