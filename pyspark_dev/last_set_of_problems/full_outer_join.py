import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, sum, date_add

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("FullOuterJoinExample")\
        .master("local[*]")\
        .getOrCreate()

departments = [
  (101, "HR"),
  (102, "IT"),
  (103, "Finance"),
  (104, "Marketing")
]
dep_columns = ["dept_id", "dept_name"]

departments_df  = spark.createDataFrame(departments, dep_columns)

budget = [
  (101, 50000, "2024-05-01"),
  (103, 75000, "2024-06-01"),
  (None, 60000, "2024-06-15")
]
budget_columns = ["dept_id", "amount", "budget_date"]

budget_df = spark.createDataFrame(budget, budget_columns)

#Perform a FULL OUTER JOIN between two DataFrames (departments and budget), fill missing
#values in budget, group by department, and calculate the total budget using SUM. Use DATE_ADD
#to forecast future budgets

joined_df = departments_df.join(budget_df, "dept_id","fullouter")

filled_df = joined_df.na.fill("0000-00-00", subset=["budget_date"])

filled_df.show()

filled_df.groupby(column("dept_name")).agg(sum(column("amount")).alias("total_budget")).show()

# Forecast future budgets (e.g., adding one year)
forecasted_df = filled_df.withColumn("forecasted_date", date_add(column("budget_date"), 365))
forecasted_df.show()
