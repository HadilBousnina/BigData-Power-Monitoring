from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pymongo import MongoClient

SEUIL = 2.0   # alerte si z > SEUIL (a ajuster pendant les tests)

spark = SparkSession.builder.appName("streaming_alerts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# La reference calculee par le batch (168 lignes)
profiles = spark.read.parquet("hdfs:///data/power/profiles")

# 1. Lire le flux Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "power_readings")
       .option("startingOffsets", "earliest")
       .load())

schema = StructType([StructField("ts", StringType()),
                     StructField("power", DoubleType())])
readings = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
               .select(F.to_timestamp("d.ts").alias("ts"),
                       F.col("d.power").alias("power")))

# 2. Moyenne par fenetre de 15 minutes
windows = (readings.withWatermark("ts", "30 minutes")
           .groupBy(F.window("ts", "15 minutes"))
           .agg(F.avg("power").alias("avg_kw"), F.count("*").alias("n"))
           .select(F.col("window.start").alias("start"), "avg_kw", "n"))

# 3. Pour chaque lot : z-score, puis ecriture dans MongoDB
def traiter(batch_df, batch_id):
    scored = (batch_df
              .withColumn("dow", F.dayofweek("start"))
              .withColumn("hour", F.hour("start"))
              .join(profiles, ["dow", "hour"], "left")
              .withColumn("z", (F.col("avg_kw") - F.col("mean_kw")) / F.col("std_kw")))
    rows = scored.collect()
    db = MongoClient("mongodb://mongo:27017")["power"]
    now = datetime.utcnow()
    for r in rows:
        doc = {"_id": str(r["start"]), "start": r["start"],
               "avg_kw": r["avg_kw"], "mean_kw": r["mean_kw"],
               "z": r["z"], "alert": bool(r["z"] is not None and r["z"] > SEUIL)}
        db["windows"].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        if doc["alert"]:
            db["alerts"].replace_one({"_id": doc["_id"]}, doc, upsert=True)
    if rows:
        db["status"].replace_one({"_id": "streaming"},{"_id": "streaming", "last_write": now}, upsert=True)   
    print(f"lot {batch_id} : {len(rows)} fenetres traitees")

query = (windows.writeStream
         .outputMode("update")
         .foreachBatch(traiter)
         .option("checkpointLocation", "/tmp/checkpoint_power")
         .start())
query.awaitTermination()