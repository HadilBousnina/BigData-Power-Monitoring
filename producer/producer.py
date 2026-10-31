import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer

FICHIER = r"C:\Users\bousn\Downloads\individual+household+electric+power+consumption\household_power_consumption.txt"
PAUSE = 0.1   # secondes entre deux mesures (plus petit = plus rapide)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

envoyes = 0
with open(FICHIER, encoding="utf-8") as f:
    for row in csv.DictReader(f, delimiter=";"):
        # on ne rejoue que 2009-2010 et on saute les mesures manquantes
        dt = datetime.strptime (row["Date"] + " " + row["Time"], "%d/%m/%Y %H:%M:%S")
        if dt.year < 2009 or row["Global_active_power"] == "?":
            continue
        producer.send("power_readings", {
            "ts": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "power": float(row["Global_active_power"])})
        envoyes += 1
        if envoyes % 100 == 0:
            print(envoyes, "mesures envoyees, derniere :", dt)
        time.sleep(PAUSE)