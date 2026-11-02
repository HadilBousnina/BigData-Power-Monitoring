#!/bin/bash
# Test de montee en charge du batch : a lancer DANS le conteneur hadoop-master.
# Les scripts doivent etre copies dans /root (docker cp), et /data/power/clean doit exister.
# Usage : bash scale_test.sh 1 5 20      (nombres de foyers a tester)
# Chaque essai genere N foyers, calcule les profils, mesure, puis supprime ses fichiers.
# Resultat : une ligne par essai, a recopier dans le rapport.

LIGNES_FOYER=2049280   # lignes nettoyees d'un foyer
echo "foyers,lignes,taille_hdfs_Mo,blocs,t_generation_s,t_profils_s"
for N in "$@"; do
  SORTIE=hdfs:///data/power/bench_$N
  t0=$(date +%s)
  spark-submit --master local[2] --driver-memory 2g /root/generate_households.py $N $SORTIE > /dev/null 2>&1
  t1=$(date +%s)
  spark-submit --master local[2] --driver-memory 2g /root/profiles_power.py $SORTIE hdfs:///data/power/bench_prof_$N > /dev/null 2>&1
  t2=$(date +%s)
  octets=$(hdfs dfs -du -s /data/power/bench_$N | awk '{print $1}')
  blocs=$(hdfs fsck /data/power/bench_$N -files -blocks 2>/dev/null | grep -c "len=")
  echo "$N,$((N * LIGNES_FOYER)),$((octets / 1024 / 1024)),$blocs,$((t1 - t0)),$((t2 - t1))"
  hdfs dfs -rm -r -f -skipTrash /data/power/bench_$N /data/power/bench_prof_$N > /dev/null 2>&1
done
