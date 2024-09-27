import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("LeftJoinExample")\
        .master("local[*]")\
        .getOrCreate()

employees_data = [
    (1,"Alice",10),
    (2,"Bob",20),
    (3, "Charlie",None),
    (4,"David",30),
    (5,"Edward",40)

]
employees_columns = ["emp_id","name","dept_id"]
employees_df = spark.createDataFrame(employees_data, employees_columns)

departments_data = [
    (10, "HR"),
    (20, "Finance"),
    (30, "Marketing")
]
departments_columns = ["dept_id","dept_name"]
departments_df = spark.createDataFrame(departments_data, departments_columns)

#Performing a left join
result_df = employees_df.join(departments_df, employees_df["dept_id"] == departments_df["dept_id"], "left")

#Display result
result_df.select(column("emp_id"), column("name"), employees_df["dept_id"], column("dept_name")).show()