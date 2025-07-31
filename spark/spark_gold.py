#!/usr/bin/env python3
import os
import time
import yaml
import hashlib
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

# ——— Chemins Delta ———
DELTA_BRONZE_PATH = "/delta/bronze/sport_activities"
DELTA_GOLD_PATH   = "/delta/gold/eligibilites"
CHECKPOINT_PATH   = "/delta/checkpoints/gold_streaming"
HASH_FILE         = "/delta/last_config_hash.txt"

# ——— Spark ———
spark = SparkSession.builder.appName("GoldStreaming").getOrCreate()

# ——— Charger config + hash ———
def load_config_and_hash():
    with open("/config/config.yml", "rb") as f:
        raw = f.read()
    return yaml.safe_load(raw), hashlib.md5(raw).hexdigest()

# Vérification hash au démarrage
cfg, current_hash = load_config_and_hash()
last_hash = open(HASH_FILE).read().strip() if os.path.exists(HASH_FILE) else None
if last_hash != current_hash:
    print("🔥 Config modifiée depuis le dernier run → suppression Gold.")
    import shutil
    shutil.rmtree(DELTA_GOLD_PATH, ignore_errors=True)
    with open(HASH_FILE, "w") as f:
        f.write(current_hash)

# ——— RH + BU + Distance ———
rh_pdf = pd.read_excel("/data/DonnéesRH.xlsx")
rh_pdf.columns = rh_pdf.columns.str.strip()
ref_rh = (
    spark.createDataFrame(rh_pdf)
         .selectExpr(
             "cast(`ID salarié` as long) as employee_id",
             "`BU` as bu",
             "`Moyen de déplacement` as moyen_dep",
             "cast(`Salaire brut` as double) as salaire_brut_annuel",
             "cast(`distance_km` as double) as distance_km"
         )
)

# ——— Fonction de calcul Gold (réutilisable pour batch initial et streaming) ———
def build_gold_from_bronze(full_bronze, cfg):
    MIN_ACTIVS = int(cfg["bien_etre"]["min_activities"])
    PRIME_PCT  = float(cfg["prime_sportive"]["percent"])
    MAX_WALK_KM = float(cfg["distance"]["max_walk_km"])
    MAX_BIKE_KM = float(cfg["distance"]["max_bike_km"])

    # Prime dynamique
    prime_df = (
        ref_rh
        .withColumn(
            "prime_sportive",
            (
                (F.col("moyen_dep") == "Marche/running") &
                (F.col("distance_km") <= F.lit(MAX_WALK_KM))
            ) |
            (
                (F.col("moyen_dep") == "Vélo/Trottinette/Autres") &
                (F.col("distance_km") <= F.lit(MAX_BIKE_KM))
            )
        )
        .withColumn(
            "montant_prime_euros",
            F.when(F.col("prime_sportive"),
                   F.col("salaire_brut_annuel") * F.lit(PRIME_PCT)
            ).otherwise(0.0)
        )
        .withColumn("declaration_correcte", F.col("prime_sportive"))
        .select("employee_id", "bu", "prime_sportive", "montant_prime_euros", "distance_km", "declaration_correcte")
    )

    # Activités bien-être
    well_df = (
        full_bronze.groupBy("employee_id")
        .agg(F.count("*").alias("nb_activites"))
        .withColumn("jours_bien_etre", F.col("nb_activites") >= MIN_ACTIVS)
    )

    # Fusion
    gold_df = (
        prime_df.join(well_df, on="employee_id", how="left")
        .fillna({"nb_activites": 0, "jours_bien_etre": False})
        .select("employee_id", "bu", "prime_sportive", "montant_prime_euros",
                "distance_km", "nb_activites", "jours_bien_etre", "declaration_correcte")
    )

    # Écriture Gold
    gold_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(DELTA_GOLD_PATH)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_eligibilites
        USING DELTA
        LOCATION '{DELTA_GOLD_PATH}'
    """)

# ——— Bootstrap Bronze Delta ———
log_dir = os.path.join(DELTA_BRONZE_PATH, "_delta_log")
timeout, interval, elapsed = 1000, 5, 0
print("⏳ J’attends l’initialisation de Bronze…")
while True:
    if os.path.isdir(log_dir) and any(f.endswith(".json") for f in os.listdir(log_dir)):
        print("✅ Bronze prêt.")
        break
    if elapsed > timeout:
        raise TimeoutError("Bronze pas initialisé en Delta après 1000 s.")
    time.sleep(interval); elapsed += interval

# ——— ⚡ Recalcul initial Gold dès le démarrage ———
full_bronze_initial = spark.read.format("delta").load(DELTA_BRONZE_PATH)
if not full_bronze_initial.rdd.isEmpty():
    print("⚡ Recalcul Gold initial à partir de l’historique Bronze…")
    build_gold_from_bronze(full_bronze_initial, cfg)
else:
    print("⚠️ Bronze est vide, Gold sera construit lors du premier batch avec données.")

# ——— Streaming Bronze ———
bronze_stream = (
    spark.readStream
         .format("delta")
         .load(DELTA_BRONZE_PATH)
)

# ——— Streaming : process_microbatch ———
def process_microbatch(bronze_df, batch_id):
    global last_hash
    print(f"▶️ Batch {batch_id} démarré")

    # Recharge config dynamique
    cfg, new_hash = load_config_and_hash()

    # Si config change → suppression Gold avant recalcul
    if last_hash != new_hash:
        print(f"🔥 Config changée → suppression et reconstruction complète Gold (batch {batch_id})")
        import shutil
        shutil.rmtree(DELTA_GOLD_PATH, ignore_errors=True)
        last_hash = new_hash
        with open(HASH_FILE, "w") as f:
            f.write(new_hash)

    # Lecture Bronze complet (même si batch vide) pour reconstruire Gold
    full_bronze = spark.read.format("delta").load(DELTA_BRONZE_PATH)
    if full_bronze.rdd.isEmpty():
        print(f"⚠️ Batch {batch_id} : Bronze complet vide, aucun calcul possible.")
        return

    build_gold_from_bronze(full_bronze, cfg)
    print(f"💾 Batch {batch_id} : Gold reconstruit.")

# ——— Lancement streaming ———
query: StreamingQuery = (
    bronze_stream
    .writeStream
    .foreachBatch(process_microbatch)
    .outputMode("update")
    .trigger(processingTime="20 seconds")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("🚀 GoldStreaming interactif avec recalcul immédiat au démarrage et rebuild automatique en cas de changement config.yml.")
query.awaitTermination()
