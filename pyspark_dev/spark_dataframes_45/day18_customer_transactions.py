import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, sum, avg, count

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("CoalesceExample")\
        .master("local[*]")\
        .getOrCreate()

data = [
    (1,"2024-01-01",200),
    (1,"2024-01-02",150),
    (2,"2024-01-01",300),
    (3,"2024-01-01",100),
    (1,"2024-01-03",250),
    (3,"2024-01-02",200),
    (2,"2024-01-02",100),
    (2,"2024-01-03",200)
]
columns = ["customer_id", "transaction_date", "amount"]

#Create DataFrame
df = spark.createDataFrame(data, columns)
#df.show()

aggregated_df = df.groupby(column("customer_id")).agg(
    sum(column("amount")).alias("total_amount"),
    avg(column("amount")).alias("avg_amount"),
    count(column("amount")).alias("num_transactions"))

#Filter
aggregated_df.filter(column("num_transactions") <= 5).show()