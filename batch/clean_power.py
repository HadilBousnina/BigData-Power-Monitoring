from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("clean_power").getOrCreate()

# 1. Lecture du fichier brut (tout est lu comme du texte au départ)
raw = (spark.read
       .option("header", True)
       .option("sep", ";")
       .csv("hdfs:///data/power/raw/household_power_consumption.txt"))

# 2. Remplacer les "?" par des valeurs vides et convertir en nombres
num_cols = ["Global_active_power", "Global_reactive_power", "Voltage",
            "Global_intensity", "Sub_metering_1", "Sub_metering_2",
            "Sub_metering_3"]
df = raw
for c in num_cols:
    df = df.withColumn(
        c, F.when(F.col(c) == "?", None).otherwise(F.col(c)).cast("double"))

# 3. Fabriquer une vraie colonne date-heure
df = (df.withColumn("ts", F.to_timestamp(
            F.concat_ws(" ", F.col("Date"), F.col("Time")),
            "d/M/yyyy HH:mm:ss"))
        .drop("Date", "Time"))

# 4. Retirer les lignes inutilisables
avant = df.count()
clean = df.dropna(subset=["ts", "Global_active_power"])
apres = clean.count()
print(f"Lignes avant : {avant}, apres : {apres}, supprimees : {avant - apres}")

# 5. Ecrire en Parquet, range par annee
clean = clean.withColumn("year", F.year("ts"))
clean.write.mode("overwrite").partitionBy("year").parquet("hdfs:///data/power/clean")

spark.stop()
