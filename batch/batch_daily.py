from datetime import datetime

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("batch_daily").getOrCreate()

# Donnees de tous les foyers : Global_active_power en kW par minute,
# Sub_metering_1/2/3 en Wh par minute (cuisine, buanderie, chauffe-eau et clim)
df = spark.read.parquet("hdfs:///data/power/clean_multi")

daily = (df.withColumn("day", F.to_date("ts"))
           .groupBy("household_id", "day")
           .agg((F.sum("Global_active_power") / 60).alias("kwh"),
                F.max("Global_active_power").alias("max_kw"),
                (F.sum("Sub_metering_1") / 1000).alias("kwh_kitchen"),
                (F.sum("Sub_metering_2") / 1000).alias("kwh_laundry"),
                (F.sum("Sub_metering_3") / 1000).alias("kwh_heating_ac"))
           .orderBy("household_id", "day")
           .collect())

docs = [{"_id": f"{r['household_id']}-{r['day']}",
         "household_id": r["household_id"],
         "day": datetime.combine(r["day"], datetime.min.time()),
         "kwh": float(r["kwh"]), "max_kw": float(r["max_kw"]),
         "kwh_kitchen": float(r["kwh_kitchen"] or 0),
         "kwh_laundry": float(r["kwh_laundry"] or 0),
         "kwh_heating_ac": float(r["kwh_heating_ac"] or 0)} for r in daily]

col = MongoClient("mongodb://mongo:27017")["power"]["daily_stats"]
col.delete_many({})
col.insert_many(docs)
col.create_index([("household_id", 1), ("day", 1)])
print("Jours-foyers dans MongoDB :", col.count_documents({}))
spark.stop()
