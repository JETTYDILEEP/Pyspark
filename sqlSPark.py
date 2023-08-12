from pyspark.sql import SparkSession
from pyspark.sql.functions import col

jar_path = "C:\\Users\\jetty\\Desktop\\Practise\\Spark_jars\\"

spark = SparkSession.builder\
    .appName("mysql connector")\
    .master("local[*]")\
    .config("spark.jars", jar_path+"mysql-connector-java-8.0.25.jar")\
    .getOrCreate()
spark.sparkContext.setLogLevel("Error")
# db = practise
#table = students_performance
print("enter db details:")
mysql_url = "jdbc:mysql://localhost:3306/" + str(input())
print("Enter Db Table:")
database = str(input())
df = spark.read\
    .format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url",mysql_url)\
    .option("dbtable",database)\
    .option("user","root")\
    .option("password","1234")\
    .load()

print(df.count())
df.show(df.count())
print("\n\n--------------after filter------------\n\n")

final_df = df.where((col("gender")== "female") & (col("lunch")== "standard"))
print(50)
final_df.show()


status = spark.read\
    .format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://localhost:3306/practise")\
    .option("query","select gender,lunch from students_performance")\
    .option("user","root")\
    .option("password","1234")\
    .load()
status.show()
# status = final_df.write\
#                 .format("jdbc")\
#                 .option("driver","com.mysql.cj.jdbc.Driver")\
#                 .option("url","jdbc:mysql://localhost:3306/practise")\
#                 .option("dbtable","students_performance_female")\
#                 .option("user","root")\
#                 .option("password","1234")\
#                 .save()

print(status)



