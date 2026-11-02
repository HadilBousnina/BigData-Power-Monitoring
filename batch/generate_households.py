"""Multiplie le jeu de donnees : fabrique N foyers synthetiques a partir du foyer reel.

Usage (dans hadoop-master) :
  spark-submit --master local[2] --driver-memory 2g generate_households.py [N] [sortie] [source]
  defauts : N = 20, sortie = hdfs:///data/power/clean_multi, source = hdfs:///data/power/clean

Le foyer 0 est le foyer reel, inchange (N = 1 reproduit donc la version initiale).
Chaque foyer h >= 1 est une copie dont la consommation est :
  - mise a l'echelle par un facteur fixe entre 0,6 et 1,4 (foyer plus ou moins gourmand),
  - perturbee par un bruit multiplicatif d'environ 5 % a chaque mesure.
Les dates ne sont PAS decalees : la jointure du streaming et le watermark (global a tous
les foyers) supposent que tous les foyers avancent dans le temps au meme rythme.

Attention : la formule du facteur est dupliquee dans producer/producer.py. Si on la change
ici, il faut la changer la-bas, sinon les profils batch ne correspondent plus au flux.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N = int(sys.argv[1]) if len(sys.argv) > 1 else 20
SORTIE = sys.argv[2] if len(sys.argv) > 2 else "hdfs:///data/power/clean_multi"
SOURCE = sys.argv[3] if len(sys.argv) > 3 else "hdfs:///data/power/clean"
SIGMA = 0.05   # ecart-type relatif du bruit

spark = SparkSession.builder.appName("generate_households").getOrCreate()

base = spark.read.parquet(SOURCE)

# 1. Chaque ligne est dupliquee N fois, une copie par foyer (0 .. N-1)
df = base.select("*", F.explode(F.sequence(F.lit(0), F.lit(N - 1))).alias("household_id"))

# 2. Facteur multiplicatif : 1 pour le foyer 0, echelle fixe x bruit pour les autres
h = F.col("household_id")
echelle = F.when(h == 0, F.lit(1.0)).otherwise(0.6 + 0.8 * ((h * 37) % 100) / 99.0)
bruit = F.when(h == 0, F.lit(1.0)).otherwise(1.0 + SIGMA * F.randn(42))
df = df.withColumn("k", echelle * bruit)

# 3. Appliquer le facteur, en gardant la precision du capteur (arrondi comme le fichier
#    d'origine : cela garde aussi une bonne compression Parquet)
for col, decimales in [("Global_active_power", 3), ("Global_reactive_power", 3),
                       ("Global_intensity", 1), ("Sub_metering_1", 0),
                       ("Sub_metering_2", 0), ("Sub_metering_3", 0)]:
    df = df.withColumn(col, F.round(F.col(col) * F.col("k"), decimales))
df = df.drop("k")

# 4. Ecriture en Parquet, partitionne par annee comme avant
df.write.mode("overwrite").partitionBy("year").parquet(SORTIE)

out = spark.read.parquet(SORTIE)
print(f"Foyers : {N}, lignes ecrites : {out.count()}, destination : {SORTIE}")

spark.stop()
