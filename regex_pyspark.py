import pyspark.sql.functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("regex")\
                            .getOrCreate()

df = spark.createDataFrame([("93 NORTH 9TH STREET, BROOKLYN NY 11211",),
("380 WESTMINSTER ST, PROVIDENCE RI 02903",),
("177 MAIN STREET, LITTLETON NH 03561",),
("202 HARLOW ST, BANGOR ME 04401",),
("46 FRONT STREET, WATERVILLE, ME 04901",),
("22 SUSSEX ST, HACKENSACK NJ 07601",),
("75 OAK STREET, PATCHOGUE NY 11772",),
("1 CLINTON AVE, ALBANY NY 12207",),
("7242 ROUTE 9, PLATTSBURGH NY 12901",),
("520 5TH AVE, MCKEESPORT PA 15132",),
("122 W 3RD STREET, GREENSBURG PA 15601",) ], ["Address"])
df.show(truncate=False)
df = df.withColumn("clean_address", F.regexp_replace(F.col("address"), "[^A-Za-z\s]", ""))\
 .withColumn("number", F.regexp_extract(F.col("address"), "(\d*)", 1))\
 .withColumn("city", F.regexp_extract(F.col("address"), "(,) (\w*)", 2))\
 .withColumn("state", F.regexp_extract(F.col("address"), "(\w*) (\w*$)", 1))\
 .withColumn("zip_code", F.regexp_extract(F.col("address"), "(\d*$)", 1))
df.show(truncate=False)
