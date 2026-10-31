from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("profiles_power").getOrCreate()

# 1. Lire les donnees propres, garder la periode de reference (2006-2008)
df = spark.read.parquet("hdfs:///data/power/clean")
ref = df.filter(F.col("year") <= 2008)

# 2. Moyenne et ecart-type par (jour de la semaine, heure)
profiles = (ref
    .withColumn("dow", F.dayofweek("ts"))   # 1 = dimanche ... 7 = samedi
    .withColumn("hour", F.hour("ts"))
    .groupBy("dow", "hour")
    .agg(F.avg("Global_active_power").alias("mean_kw"),
         F.stddev("Global_active_power").alias("std_kw"),
         F.count("*").alias("n"))
    .orderBy("dow", "hour"))

profiles.show(10)
print("Nombre de profils :", profiles.count())

# 3. Sauvegarder dans HDFS
profiles.write.mode("overwrite").parquet("hdfs:///data/power/profiles")

spark.stop()