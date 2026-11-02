from datetime import datetime
from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

SEUIL = 2.0   # alerte si z > SEUIL (a ajuster pendant les tests)

spark = SparkSession.builder.appName("streaming_alerts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# La reference calculee par le batch (168 lignes par foyer)
profiles = spark.read.parquet("hdfs:///data/power/profiles")

# Index MongoDB pour que le dashboard filtre vite par foyer
_db = MongoClient("mongodb://mongo:27017")["power"]
_db["windows"].create_index([("household_id", 1), ("start", -1)])
_db["alerts"].create_index([("household_id", 1), ("start", -1)])

# 1. Lire le flux Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "power_readings")
       .option("startingOffsets", "earliest")
       .load())

schema = StructType([StructField("household_id", IntegerType()),
                     StructField("ts", StringType()),
                     StructField("power", DoubleType())])
readings = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
               .select(F.col("d.household_id").alias("household_id"),
                       F.to_timestamp("d.ts").alias("ts"),
                       F.col("d.power").alias("power")))

# 2. Moyenne par foyer et par fenetre de 15 minutes
windows = (readings.withWatermark("ts", "30 minutes")
           .groupBy("household_id", F.window("ts", "15 minutes"))
           .agg(F.avg("power").alias("avg_kw"), F.count("*").alias("n"))
           .select("household_id", F.col("window.start").alias("start"), "avg_kw", "n"))

# 3. Pour chaque lot : z-score, puis ecriture dans MongoDB
def traiter(batch_df, batch_id):
    scored = (batch_df
              .withColumn("dow", F.dayofweek("start"))
              .withColumn("hour", F.hour("start"))
              .join(profiles, ["household_id", "dow", "hour"], "left")
              .withColumn("z", (F.col("avg_kw") - F.col("mean_kw")) / F.col("std_kw")))
    rows = scored.collect()
    db = MongoClient("mongodb://mongo:27017")["power"]
    now = datetime.utcnow()
    w_ops, a_ops = [], []
    for r in rows:
        doc = {"_id": f"{r['household_id']}-{r['start']}", "household_id": r["household_id"],
               "start": r["start"], "avg_kw": r["avg_kw"], "mean_kw": r["mean_kw"],
               "z": r["z"], "alert": bool(r["z"] is not None and r["z"] > SEUIL)}
        w_ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if doc["alert"]:
            a_ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    if w_ops:
        db["windows"].bulk_write(w_ops, ordered=False)
    if a_ops:
        db["alerts"].bulk_write(a_ops, ordered=False)
    if rows:
        db["status"].replace_one({"_id": "streaming"},{"_id": "streaming", "last_write": now}, upsert=True)
    print(f"lot {batch_id} : {len(rows)} fenetres traitees")

query = (windows.writeStream
         .outputMode("update")
         .foreachBatch(traiter)
         .option("checkpointLocation", "/tmp/checkpoint_power")
         .start())
query.awaitTermination()
