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
avg_temp = df['temperature'].mean()
max_vibration = df['vibration'].max() 



# spara resultatet i silver tabellen

result.to_sql ("curated.sensor_data_summary", conn, if_exists='replace', index=False)
print ("batch job klar") 