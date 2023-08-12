from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .appName("Orc_Practise")\
        .master("local[*]")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(spark.sparkContext.getConf().get("spark.master"))
print(spark.sparkContext.getConf().get("spark.driver.host"))
print(spark.sparkContext.getConf().get("spark.driver.port"))
