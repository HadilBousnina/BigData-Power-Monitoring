import csv
import json
import random
import sys
import time
from datetime import datetime
from kafka import KafkaProducer

FICHIER = r"C:\Users\bousn\Downloads\individual+household+electric+power+consumption\household_power_consumption.txt"
PAUSE = 0.1   # secondes entre deux minutes rejouees (plus petit = plus rapide)

# Nombre de foyers rejoues : doit etre egal au N passe a batch/generate_households.py.
# Usage : python producer/producer.py 20     (sans argument : 1 foyer, comme avant)
N_FOYERS = int(sys.argv[1]) if len(sys.argv) > 1 else 1
SIGMA = 0.05


def echelle(h):
    """Meme facteur que batch/generate_households.py (foyer 0 = foyer reel)."""
    return 1.0 if h == 0 else 0.6 + 0.8 * ((h * 37) % 100) / 99.0


ECHELLES = [echelle(h) for h in range(N_FOYERS)]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

envoyes = 0
with open(FICHIER, encoding="utf-8") as f:
    for row in csv.DictReader(f, delimiter=";"):
        # on ne rejoue que 2009-2010 et on saute les mesures manquantes
        dt = datetime.strptime (row["Date"] + " " + row["Time"], "%d/%m/%Y %H:%M:%S")
        if dt.year < 2009 or row["Global_active_power"] == "?":
            continue
        power = float(row["Global_active_power"])
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
        # une mesure par foyer pour cette minute, avant de passer a la minute suivante
        # (tous les foyers avancent au meme rythme : le watermark Spark est global)
        for h in range(N_FOYERS):
            bruit = 1.0 if h == 0 else 1.0 + SIGMA * random.gauss(0, 1)
            producer.send("power_readings", key=h, value={
                "household_id": h, "ts": ts,
                "power": round(power * ECHELLES[h] * bruit, 3)})
        envoyes += 1
        if envoyes % 100 == 0:
            print(envoyes, "minutes envoyees (", N_FOYERS, "foyers ), derniere :", dt)
        time.sleep(PAUSE)

producer.flush()
