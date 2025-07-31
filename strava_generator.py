#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import io

# ——— 1) Lecture des IDs dans le fichier Excel ———
df_rh = pd.read_excel("data/DonnéesRH.xlsx")
df_rh.columns = df_rh.columns.str.strip()
ids = df_rh["ID salarié"].dropna().astype(int).tolist()

# ——— 2) Génération des activités sur 1 an ———
TYPES = ["course à pied", "vélo", "marche", "randonnée", "escalade"]
rows = []

for emp_id in ids:
    nb_activites = random.randint(10, 25)
    for _ in range(nb_activites):
        date = datetime.utcnow() - timedelta(days=random.randint(0, 364))
        type_activite = random.choice(TYPES)
        trajet = random.random() < 0.5 and type_activite in ["vélo", "marche", "course à pied"]
        distance = round(random.uniform(800, 20000), 2)
        duree = random.randint(600, 7200)
        commentaire = f"Session de {type_activite} de {distance} m, durée {duree} s."
        rows.append((emp_id, date, type_activite, distance, duree, commentaire))

df = pd.DataFrame(rows, columns=[
    "employee_id", "date_debut", "type_activite",
    "distance_m", "duree_s", "commentaire"
])
df = df.sort_values("date_debut")

# ——— 3) Connexion à PostgreSQL ———
conn = psycopg2.connect(
    dbname="sportsdb",
    user="user",
    password="password",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# ——— 4) Bulk insert via COPY ———
buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False, sep='\t')
buffer.seek(0)
cur.copy_from(buffer, "sport_activities", sep='\t', columns=df.columns.tolist())
conn.commit()
print("✅ Back‑fill d’1 an terminé.")

# ——— 5) Insertion du marqueur de fin de back‑fill ———
cur.execute(
    """
    INSERT INTO sport_activities
      (employee_id, date_debut, type_activite, distance_m, duree_s, commentaire)
    VALUES (%s, %s, %s, %s, %s, %s)
    """,
    (
        0,
        datetime.utcnow(),
        "BACKFILL_COMPLETE",
        0.0,
        0,
        "--- FIN DU BACKFILL POC ---"
    )
)
conn.commit()
print("✅ Marqueur BACKFILL_COMPLETE inséré.")

cur.close()
conn.close()
