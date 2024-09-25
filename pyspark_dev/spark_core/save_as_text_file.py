from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

sc = SparkContext("local[4]", "SparkRDD")

data = [10,20,30,40,50,60]
rdd = sc.parallelize(data)
#rdd.saveAsTextFile("E:/Files/Sep20")

for i in rdd.collect():
    print(i)
