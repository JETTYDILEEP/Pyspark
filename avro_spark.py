from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
import os

# SUBMIT_ARGS = "--packages c:/users/jetty/desktop/practise/spark_jars/spark-avro_2.12-3.3.1.jar pyspark-shell"
# os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
jar_path = "C:\\Users\\jetty\\Desktop\\Practise\\Spark_jars\\"
spark = SparkSession.builder \
    .appName('DataFrame') \
    .master("local[*]")\
    .config('spark.jars', jar_path+'spark-avro_2.12-3.3.1.jar') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(spark.sparkContext)
print(spark.version)
df = spark.read.format('avro').load("file:///c:/data/data.avro")

df.show()