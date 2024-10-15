import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("FilterExample")\
        .master("local[*]")\
        .getOrCreate()

data = [
        (1,"Anudeep",9000),
        (2,"Aamesh", 6000),
        (3,"Suresh", 3000),
        (4,"Krishna", 2000)
]

columns = ["Id", "Name", "Salary"]

employee_df = spark.createDataFrame(data, columns)

#Filter customers whose names start with 'A' and display their details.
employee_df.filter(column("Name").startswith("A")).show()


