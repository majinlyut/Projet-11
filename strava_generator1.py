#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import psycopg2
import pandas as pd
import io

# ——— 1) Lecture des IDs dans le fichier Excel ———
df_rh = pd.read_excel("data/DonnéesRH.xlsx")
df_rh.columns = df_rh.columns.str.strip()
ids = df_rh["ID salarié"].dropna().astype(int).tolist()

# On prend le premier salarié (ou tu peux mettre un ID fixe)
employee_id = 999999999

# ——— 2) Préparation de la ligne unique à insérer ———
# Exemple : activité de 5 km en course à pied aujourd'hui
single_row = [{
    "employee_id":   employee_id,
    "date_debut":    datetime.now(),            # timestamp actuel
    "type_activite": "course à pied",           # type fixe
    "distance_m":    5000.0,                    # 5 000 m
    "duree_s":       1800,                      # 30 minutes
    "commentaire":   "Session de test unique"   # commentaire
}]

df = pd.DataFrame(single_row)

# ——— 3) Connexion à PostgreSQL ———
conn = psycopg2.connect(
    dbname="sportsdb",
    user="user",
    password="password",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# ——— 4) Préparation du buffer CSV pour COPY ———
buffer = io.StringIO()
# On écrit **sans** header, en TSV
df.to_csv(buffer, index=False, header=False, sep='\t')
buffer.seek(0)

# ——— 5) Insertion en bulk (mais ici ça n'insère qu'une ligne) ———
cur.copy_from(
    file=buffer,
    table="sport_activities",
    sep='\t',
    columns=df.columns.tolist()
)

conn.commit()
cur.close()
conn.close()

print("✅ 1 seule activité insérée dans sport_activities.")
