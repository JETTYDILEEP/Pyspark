from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
import requests
spark = SparkSession.builder\
        .appName("URL_practise")\
        .master("local[*]")\
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')
print(sc.getConf().get("spark.master"))
print(sc.getConf().get("spark.driver.host"))
print(sc.getConf().get("spark.driver.port"))
http = requests.get("https://randomuser.me/api/0.8/?results=10")
print(http)
rdd_str = sc.parallelize([http.text])
df = spark.read.json(rdd_str)
df.show()
flatten_df = df.select(col("nationality"),
                       explode(col("results")).alias("results"),
                       col("seed"),
                       col("version")
                       )
flatten_df.show()
#import pdb; pdb.set_trace()
finaldf = flatten_df.select(
      col("results.user.*")
)
finaldf.show(truncate=False)