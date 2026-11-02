from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("profiles_to_mongo").getOrCreate()

# 1. Lire les profils depuis HDFS (168 par foyer)
rows = spark.read.parquet("hdfs:///data/power/profiles").collect()

# 2. Se connecter a MongoDB (le conteneur s'appelle "mongo")
client = MongoClient("mongodb://mongo:27017")
col = client["power"]["profiles"]
col.create_index([("household_id", 1), ("dow", 1), ("hour", 1)])

# 3. Remplacer les profils (upsert : pas de doublons si on relance).
#    On vide d'abord la collection pour ne pas garder les foyers d'un ancien essai.
col.delete_many({})
ops = []
for r in rows:
    doc = {"_id": f"{r['household_id']}-{r['dow']}-{r['hour']}",
           "household_id": r["household_id"], "dow": r["dow"], "hour": r["hour"],
           "mean_kw": r["mean_kw"], "std_kw": r["std_kw"], "n": r["n"]}
    ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
if ops:
    col.bulk_write(ops, ordered=False)

print("Profils dans MongoDB :", col.count_documents({}))
spark.stop()
