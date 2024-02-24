from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
# start = time.time()
# print(f"start time - {start}")

spark = SparkSession.builder\
                    .appName('practise')\
                    .master('local[1]')\
                    .getOrCreate()
##### Pivot table #########
# match_data = [
# ("Jadeja", 'Wankhede', 40, 2, 1),
# ("Hardik", 'Eden Gardens', 55, 0, 2),
# ("Jadeja", 'Eden Gardens', 25, 3, 0),
# ("Watson", 'Wankhede', 30, 1, 0),
# ("Hardik", 'Wankhede', 45, 0, 1),
# ("Jadeja", 'Wankhede', 60, 1, 1),
# ("Hardik", 'Eden Gardens', 45, 0, 3),
# ("Hardik", 'Eden Gardens', 30, 4, 0),
# ("Watson", 'Wankhede', 20, 2, 2),
# ("Watson", 'Eden Gardens', 50, 0, 0)
# ]
#
# match_schema = "Player_Name String , Stadium String , Runs int , Wickets int , Catches int"
#
# match_df = spark.createDataFrame(data = match_data, schema = match_schema)
# match_df.show()
# pivot_df = match_df.groupby(col("Player_Name")).pivot("Stadium").sum("Runs","Wickets")
# pivot_df = pivot_df.withColumnRenamed("Eden Gardens_sum(Runs)","Runs_in_Eden_Garden")\
#                    .withColumnRenamed("Eden Gardens_sum(Wickets)","Wickets_in_Ede_Garden")\
#                    .withColumnRenamed("Wankhede_sum(Runs)","Runs_in_Wankhede")\
#                    .withColumnRenamed("Wankhede_sum(Wickets)","Wickets_in_Wankhede")
#
# pivot_df.show()
# end = time.time()
# print(f"ending time - {end}")
# print(end - start)

