import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, avg, count

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("Practice")\
        .master("local[*]")\
        .getOrCreate()


data = [
    ("Sales",5000, "John"),
    ("Sales",6000, "Doe"),
    ("HR",7000, "Jane"),
    ("HR",8000, "Alice"),
    ("IT",4500, "Bob"),
    ("IT",5500, "Charlie")
]
schema = ["department", "salary", "employee_name"]

#Create DataFrame
df = spark.createDataFrame(data, schema)
df.show()

#Group by department and calculate average salary and employee count
result_df = df.groupby(column("department")).agg(avg(column("salary")).alias("average_salary"),
                                     count(column("employee_name")).alias("employee_count"))
#Show Result
result_df.show()



