from pyspark.sql import SparkSession
from pymongo import MongoClient

spark = SparkSession.builder.appName("profiles_to_mongo").getOrCreate()

# 1. Lire les 168 profils depuis HDFS
rows = spark.read.parquet("hdfs:///data/power/profiles").collect()

# 2. Se connecter a MongoDB (le conteneur s'appelle "mongo")
client = MongoClient("mongodb://mongo:27017")
col = client["power"]["profiles"]

# 3. Inserer chaque profil (upsert : pas de doublons si on relance)
for r in rows:
    doc = {"_id": f"{r['dow']}-{r['hour']}",
           "dow": r["dow"], "hour": r["hour"],
           "mean_kw": r["mean_kw"], "std_kw": r["std_kw"], "n": r["n"]}
    col.replace_one({"_id": doc["_id"]}, doc, upsert=True)

print("Profils dans MongoDB :", col.count_documents({}))
spark.stop()
