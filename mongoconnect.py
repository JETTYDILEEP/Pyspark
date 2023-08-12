from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas

# sc = SparkContext()
# sc.setLogLevel("ERROR")
jar_path = "C:\\Users\\jetty\\Desktop\\Practise\\Spark_jars\\"
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Practise_Spark.iris_dataset")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Practise_Spark.iris_dataset")\
    .config('spark.jars', jar_path+'mongo-spark-connector-10.0.5.jar') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df.printSchema()
df.show()

# myclient = MongoClient("mongodb://localhost:27017/")
# mydb = myclient["Practise_Spark"]
# mycol = mydb["iris_dataset"]
# x =[]
# for i in mycol.find():
#     x.append(i)
#
# print(x)
# df = pandas.DataFrame(x)
#
# print(df)
#
# sparkDF = spark.createDataFrame(df)
# sparkDF.printSchema()
# sparkDF.show()