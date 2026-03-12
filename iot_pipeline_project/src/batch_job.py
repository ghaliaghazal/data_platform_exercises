import pandas as pd # för att hantera data i tabellformat
import psycopg2 # för att ansluta till PostgreSQL

# Anslut till PostgreSQL-databasen
conn = psycopg2.connect(
     host="localhost",
    database="iot",
    user="postgres",
    password="postgres"
) 

# läser raw data från staging
df = pd.read_sql_query("SELECT * FROM staging.sensor_data", conn)
print (df.head()) # visar de första raderna av data

# beräknar tex: medelvärde av temperatur, fuktighet och vibration per sensor



# Spara resultatet i en ny tabell i PostgreSQL

