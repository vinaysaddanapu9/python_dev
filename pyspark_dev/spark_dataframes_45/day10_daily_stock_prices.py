import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import column, sum

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

spark = SparkSession.builder\
        .appName("DailyStockPrices")\
        .master("local[*]")\
        .getOrCreate()

data = [
    ("2024-09-01", "AAPL", 150),
    ("2024-09-02", "AAPL", 160),
    ("2024-09-03", "AAPL", 170),
    ("2024-09-01", "GOOGL", 1200),
    ("2024-09-02", "GOOGL", 1250),
    ("2024-09-03", "GOOGL", 1300)
]

columns = ["date", "symbol", "price"]

#Create DataFrame
stock_price_df = spark.createDataFrame(data, columns)
#stock_price_df.show()

#Calculate the running total of stock prices for each stock symbol.

window = Window.partitionBy(column("symbol")).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_with_running_total = stock_price_df.select(column("*"),
                                              sum(column("price")).over(window).alias("running_total"))
#Show Result
df_with_running_total.show()
