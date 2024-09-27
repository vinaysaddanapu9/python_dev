import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder \
        .appName("InnerJoin")\
        .master("local[*]")\
        .getOrCreate()

#employee_df
employee_data = [
    (1,"Alice",101),
    (2,"Bob",102),
    (3, "Carol",103),
    (4,"Dave",101),
    (5,"Eve",104)
]
employee_columns = ["emp_id", "name", "dept_id"]
employee_df = spark.createDataFrame(employee_data, employee_columns)

#department_df
department_data = [
    (101,"HR"),
    (102,"Engineering"),
    (103,"Marketing"),
    (105,"Sales")
]
department_columns = ["dept_id", "dept_name"]
department_df = spark.createDataFrame(department_data, department_columns)

#Performing an inner join on 'dept_id'
join_data_df = employee_df.join(department_df, employee_df["dept_id"] == department_df["dept_id"], "inner")
join_data_df.show()

#Selecting the required columns(emp_id, name, dept_name)
result_df = join_data_df.select(column("emp_id"), column("name"), column("dept_name"))

#show result
result_df.show()
