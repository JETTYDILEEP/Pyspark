from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from urllib.request import Request, urlopen

spark = SparkSession.builder\
        .appName("Orc_Practise")\
        .master("local[*]")\
        .getOrCreate()
sc = spark.sparkContext
sc = sc.setLogLevel("ERROR")
# data = spark.read.csv("C:\data\devices.csv",header = True)
# data.show()
http = urlopen("https://randomuser.me/api/0.8/?results=10").read()
print(http)
rdd_str = sc.parallelize([http])
df = spark.read.json(rdd_str)
df.show()
flatten_df = df.select(col("nationality"),
                       explode(col("results")).alias("results"),
                       col("seed"),
                       col("version")
                       )
flatten_df.show()

finaldf = flatten_df.select(
      col("results.user.*")
)
finaldf.show(truncate=False)