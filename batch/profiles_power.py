import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Usage : spark-submit profiles_power.py [source] [sortie]
SOURCE = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/power/clean_multi"
SORTIE = sys.argv[2] if len(sys.argv) > 2 else "hdfs:///data/power/profiles"

spark = SparkSession.builder.appName("profiles_power").getOrCreate()

# 1. Lire les donnees (tous les foyers), garder la periode de reference (2006-2008)
df = spark.read.parquet(SOURCE)
ref = df.filter(F.col("year") <= 2008)

# 2. Moyenne et ecart-type par (foyer, jour de la semaine, heure)
profiles = (ref
    .withColumn("dow", F.dayofweek("ts"))   # 1 = dimanche ... 7 = samedi
    .withColumn("hour", F.hour("ts"))
    .groupBy("household_id", "dow", "hour")
    .agg(F.avg("Global_active_power").alias("mean_kw"),
         F.stddev("Global_active_power").alias("std_kw"),
         F.count("*").alias("n"))
    .orderBy("household_id", "dow", "hour"))

profiles.show(10)
print("Nombre de profils :", profiles.count())   # 168 par foyer

# 3. Sauvegarder dans HDFS
profiles.write.mode("overwrite").parquet(SORTIE)

spark.stop()
