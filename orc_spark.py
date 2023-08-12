from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *

jar_path = "C:\\Users\\jetty\\Desktop\\Practise\\Spark_jars\\"
spark = SparkSession.builder\
        .appName("Orc_Practise")\
        .master("local[*]")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


print(spark.sparkContext)
print(spark.version)
df = spark.read.format('orc').load("file:///c:/data/data.orc")

df.show()