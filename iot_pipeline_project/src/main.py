from fastapi import FastAPI
import pandas as pd
import psycopg2

app = FastAPI()

# Databasanslutning
conn = psycopg2.connect(
    host="localhost",
    database="iot",
    user="postgres",
    password="postgres"
)

# API-endpoint för att hämta sensorstatistik
@app.get("/sensor-stats") # hämtar statistik från den kuraterade tabellen
def get_sensor_stats(): # läser data från den kuraterade tabellen
    df = pd.read_sql("SELECT * FROM curated_sensor_stats", conn) # läser data från den kuraterade tabellen
    return df.to_dict(orient="records") # returnerar data som en lista av ordböcker