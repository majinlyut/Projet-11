#!/usr/bin/env python3
import time
import os
import yaml
import pandas as pd
import urllib.request
import urllib.parse
import json
from dotenv import load_dotenv

# ——— 1) Charge la clé API depuis .env ———
load_dotenv()
GMAPS_API_KEY = os.getenv("GMAPS_API_KEY")
if not GMAPS_API_KEY:
    raise RuntimeError("Il faut définir GMAPS_API_KEY dans le .env")

# ——— 2) Charge la config ———
with open("config/config.yml") as f:
    cfg = yaml.safe_load(f)

# On suppose que ton config.yml contient :
# distance:
#   max_walk_km: 15
#   max_bike_km: 25
MAX_WALK_KM = float(cfg["distance"]["max_walk_km"])
MAX_BIKE_KM = float(cfg["distance"]["max_bike_km"])

COMPANY_ADDR = "1362 Av. des Platanes, 34970 Lattes, France"
INPUT_XLSX   = "data/DonnéesRH.xlsx"

# ——— 3) Lecture du fichier RH ———
df = pd.read_excel(INPUT_XLSX)

# ——— 4) Fonction pour appeler l'API Distance Matrix (robuste) ———
def fetch_distance(origin_addr: str) -> float:
    base = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin_addr,
        "destinations": COMPANY_ADDR,
        "mode": "driving",
        "units": "metric",
        "key": GMAPS_API_KEY
    }
    url = f"{base}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url) as resp:
            data = json.load(resp)
    except Exception as e:
        print(f"❌ Erreur réseau pour '{origin_addr}': {e}")
        return None

    # 1) Vérifie le status global
    if data.get("status") != "OK":
        print(f"⚠️ Status API '{data.get('status')}' pour '{origin_addr}'")
        return None

    rows = data.get("rows", [])
    if not rows or "elements" not in rows[0]:
        print(f"⚠️ Pas de rows/elements pour '{origin_addr}'")
        return None

    elem = rows[0]["elements"][0]
    # 2) Vérifie le status élément
    if elem.get("status") != "OK":
        print(f"⚠️ Element status '{elem.get('status')}' pour '{origin_addr}'")
        return None

    # 3) Tout est OK, retourne la distance en km
    meters = elem.get("distance", {}).get("value")
    if meters is None:
        print(f"⚠️ Pas de distance renvoyée pour '{origin_addr}'")
        return None

    return round(meters / 1000, 2)

# ——— 5) Appelle une seule fois par adresse unique (throttle 1 req/sec) ———
unique_addrs = df["Adresse du domicile"].dropna().unique()
cache = {}
for addr in unique_addrs:
    dist = fetch_distance(addr)
    cache[addr] = dist
    print(f"{addr[:30]}… → {dist} km")
    time.sleep(1)

# ——— 6) Injecte la distance et le flag de déclaration ———
df["distance_km"] = df["Adresse du domicile"].map(cache)

def valide_decl(row):
    mode = row["Moyen de déplacement"]
    dist = row["distance_km"] or 0
    if mode == "Marche/running" and dist > MAX_WALK_KM:
        return False
    if mode in ["Vélo/Trottinette/Autres"] and dist > MAX_BIKE_KM:
        return False
    return True

df["declaration_correcte"] = df.apply(valide_decl, axis=1)

# ——— 7) Écrase le fichier d'origine avec les nouvelles colonnes ———
df.to_excel(INPUT_XLSX, index=False)
print(f"\n✅ Fichier {INPUT_XLSX} mis à jour et écrasé avec 'distance_km' et 'declaration_correcte'.")
