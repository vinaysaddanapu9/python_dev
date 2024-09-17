#Word Count in PySpark
from pyspark import SparkContext
import os

os.environ[ "PYSPARK_PYTHON"] = "E:/Python/Python/Python37/python.exe"

sc = SparkContext("local[4]", "SparkRDD")
rdd = sc.textFile("E:/Files/data5.txt")
rdd2 = rdd.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
rdd5 = rdd4.sortBy(lambda x: x[1], False)

for i in rdd5.collect():
    print(i)