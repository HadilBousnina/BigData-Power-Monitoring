"""Nettoyage du fichier brut : HDFS /data/power/raw -> HDFS /data/power/clean (Parquet).

Usage (dans hadoop-master) :
  spark-submit clean_power.py [source] [sortie]
  defauts : source = hdfs:///data/power/raw, sortie = hdfs:///data/power/clean

Ce que fait le script :
  - lit le fichier brut UCI (separateur ';', valeurs manquantes notees '?') ;
  - construit la colonne ts (timestamp) a partir de Date et Time ;
  - convertit les 7 mesures en nombres (double) ;
  - supprime les lignes dont une mesure manque (ou dont l'horodatage est invalide)
    et les doublons d'horodatage ;
  - ajoute la colonne year et ecrit en Parquet, partitionne par annee.

Le resultat est l'entree de batch/generate_households.py, qui attend les colonnes ts, year,
Global_active_power, Global_reactive_power, Global_intensity et Sub_metering_1/2/3.
Pour le fichier UCI complet, on doit obtenir 2 049 280 lignes (valeur utilisee par
scripts/scale_test.sh).
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/power/raw"
SORTIE = sys.argv[2] if len(sys.argv) > 2 else "hdfs:///data/power/clean"

MESURES = ["Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity",
           "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]

spark = SparkSession.builder.appName("clean_power").getOrCreate()

# 1. Lecture : tout en texte (pas d'inferSchema), pour traiter nous-memes les '?'
raw = (spark.read.option("header", True).option("sep", ";")
       .option("inferSchema", False).csv(SOURCE))
n_brut = raw.count()

# 2. Typage : '?' ou case vide -> null, sinon nombre
df = raw
for c in MESURES:
    texte = F.trim(F.col(c))
    df = df.withColumn(c, F.when(texte.isin("", "?"), F.lit(None)).otherwise(texte.cast("double")))

# 3. Horodatage. Les dates du fichier ne sont pas completees par des zeros (1/1/2007),
#    d'ou le motif "d/M/yyyy". Fuseau de session par defaut, comme les autres jobs.
df = df.withColumn("ts", F.to_timestamp(
    F.concat_ws(" ", F.trim(F.col("Date")), F.trim(F.col("Time"))), "d/M/yyyy H:mm:ss"))

# 4. Nettoyage : on garde les lignes completes (horodatage valide et 7 mesures presentes)
valide = F.col("ts").isNotNull()
for c in MESURES:
    valide = valide & F.col(c).isNotNull()
complet = df.filter(valide).cache()
n_complet = complet.count()

# Une seule ligne par minute (pas de doublon d'horodatage)
clean = (complet.dropDuplicates(["ts"])
         .select("ts", *MESURES)
         .withColumn("year", F.year("ts")))

# 5. Ecriture en Parquet, partitionne par annee (un fichier par annee, trie par date)
(clean.repartition("year").sortWithinPartitions("ts")
      .write.mode("overwrite").partitionBy("year").parquet(SORTIE))

# 6. Controle : on relit ce qui a ete ecrit
out = spark.read.parquet(SORTIE)
n_clean = out.count()
print(f"Lignes brutes : {n_brut}")
print(f"Lignes supprimees (mesure manquante ou date invalide) : {n_brut - n_complet}")
print(f"Doublons supprimes : {n_complet - n_clean}")
print(f"Lignes nettoyees : {n_clean} (fichier UCI complet : 2049280), destination : {SORTIE}")
out.printSchema()
out.groupBy("year").count().orderBy("year").show()

spark.stop()