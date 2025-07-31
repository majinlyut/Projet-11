#!/usr/bin/env python3
import time
import yaml
from confluent_kafka.admin import AdminClient
from pyspark.sql import SparkSession, functions as F, types as T
import os

def wait_for_topic(topic, bootstrap_servers, timeout=1000, interval=5):
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    start = time.time()
    while True:
        md = admin.list_topics(timeout=5)
        if topic in md.topics and md.topics[topic].partitions:
            print(f"✅ Topic '{topic}' détecté, on démarre le stream.")
            return
        elapsed = int(time.time() - start)
        if elapsed > timeout:
            raise TimeoutError(f"❌ Topic '{topic}' introuvable après {timeout} s.")
        print(f"⏳ En attente du topic '{topic}'… ({elapsed}s écoulés)")
        time.sleep(interval)

# ——— Lecture de la config ———
with open("/config/config.yml") as f:
    cfg = yaml.safe_load(f)

# ——— Paramètres Kafka ———
KAFKA_TOPIC     = "sportsdata.public.sport_activities"
KAFKA_BOOTSTRAP = "redpanda:9092"

wait_for_topic(KAFKA_TOPIC, KAFKA_BOOTSTRAP)

# ——— Spark session ———
spark = SparkSession.builder.appName("BronzeStream").getOrCreate()

# ——— Schéma Debezium minimal ———
full_schema = T.StructType([
    T.StructField("payload", T.StructType([
        T.StructField("op", T.StringType(), True),
        T.StructField("after", T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("employee_id", T.IntegerType(), True),
            T.StructField("date_debut", T.LongType(), True),            # microsec epoch
            T.StructField("type_activite", T.StringType(), True),
            T.StructField("distance_m", T.DoubleType(), True),
            T.StructField("duree_s", T.IntegerType(), True),
            T.StructField("commentaire", T.StringType(), True),
        ]), True),
    ]))
])

# ——— Lecture du flux Kafka ———
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("subscribe", KAFKA_TOPIC)
         .option("startingOffsets", "earliest")
         .option("failOnDataLoss", "false")
         .load()
)

# ——— Extraction + conversion ———
bronze_events = (
    raw.select(F.col("value").cast("string").alias("json"))
       .select(F.from_json("json", full_schema).alias("p"))
       .select("p.payload.after.*")
       # on ajoute la colonne date_debut_ts en timestamp
       .withColumn(
           "date_debut_ts",
           (F.col("date_debut") / F.lit(1_000_000)).cast("timestamp")
       )
)

# ——— Écriture brute en Delta Lake ———
checkpoint_path = "/delta/_ck/bronze_sport"
output_path     = "/delta/bronze/sport_activities"

bronze_query = (
    bronze_events.writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", checkpoint_path)
                 .start(output_path)
)

bronze_query.awaitTermination()
