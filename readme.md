# Sport Data – Pipeline Spark 🔗 Delta Lake 🚴‍♀️

> **Objectif** : Déterminer automatiquement – et en continu – l’éligibilité des salariés à la **prime sportive** (+ % du salaire brut annuel) et aux **5 journées bien‑être** à partir :
>
> 1. du déclaratif RH (fichiers Excel)
> 2. du flux temps réel d’activités sportives (topic Redpanda)

---

## ⚙️ Architecture

```
Postgres (sportsdb) ─ Debezium ─▶ Redpanda (topic sportsdata.public.sport_activities)
                                           │
                             +-------------┴--------------+
                             |  spark_consumer (Docker)   |
                             +-----------┬---------------+
                                         │
                       ┌───────────── Delta Lake ─────────────┐
                       │   /delta/bronze/sport_activities      │  (brut)
                       │   /delta/gold/eligibilites           │  (règles)
                       └───────────────────────────────────────┘
```

\* **Bronze** : dump brut enrichi (RH)
\* **Gold**   : éligibilité ⚖️ + montant prime
\* Tous les paramètres métier vivent dans `config/config.yml` : aucun code à modifier pour changer un seuil.

---

## 📁 Arborescence du repo

```
.
├── data/                       # Excel RH (monté en lecture seule)
│   ├── DonnéesRH.xlsx
│   └── ...
├── config/
│   └── config.yml             # seuils & pourcentage prime
├── spark_consumer/
│   ├── Dockerfile
│   └── main.py                # pipeline Spark
├── docker-compose.yml
└── README.md                  # ce fichier
```

---

## 🚀 Démarrage rapide

```bash
# 1. Construire et lancer tous les services
$ docker compose up -d --build

# 2. Vérifier lUISmarche
docker compose ps

# 3. Consulter la console Redpanda
http://localhost:8080/

# 4. Interroger la table Gold depuis un shell Spark ou DuckDB
SELECT * FROM delta.`/delta/gold/eligibilites` LIMIT 10;
```

### Mettre à jour les paramètres métier

1. Modifier `config/config.yml`  (p. ex. `min_activities: 20`, `percent: 0.08`)
2. Redémarrer le consumer Spark :

```bash
docker compose restart spark_consumer
```

Le nouveau calcul est appliqué dès la reprise du stream.

---

## 🔧 Détails techniques

| Élément              | Valeur                            | Commentaire                              |
| -------------------- | --------------------------------- | ---------------------------------------- |
| **Spark**            | 3.5.0 (Bitnami)                   | Packages : Kafka 0‑10 & Delta 3.2.0      |
| **Delta Lake**       | 3.2.0                             | Stocké dans `delta_data` (volume Docker) |
| **Redpanda**         | v23.3.10                          | Ports : 19092 (Kafka ext.), 9644 (Admin) |
| **Debezium Connect** | 2.6                               | Stream CDC Postgres → Redpanda           |
| **Python libs**      | pandas, openpyxl, pyarrow, pyyaml | Lecture Excel + YAML                     |

---

## 🧪 Tests & validation

* **Unitaires PySpark** : à ajouter dans `spark_consumer/tests/` (ex : seuil d’activités)
* **Time‑travel Delta** : `SELECT * FROM delta... VERSION AS OF 3;`
* **Console Redpanda** : vérifier la présence du topic & le nombre de messages.

---

## 🚑 Troubleshooting rapide

| Problème                  | Piste                                                      |
| ------------------------- | ---------------------------------------------------------- |
| `No such file /data/...?` | Vérifier le montage `./data:/data:ro`.                     |
| `YAML parser error`       | Indentation ou clé manquante dans `config.yml`.            |
| `java.lang.OutOfMemory`   | Augmenter `SPARK_DRIVER_MEMORY` dans `docker-compose.yml`. |

---

## 📈 Prochaines améliorations

* Ingestion directe Strava (Webhook → topic Redpanda)
* Export automatique CSV mensuel pour la paie
* Tableau de bord Power BI (Dataset DirectQuery sur Delta Gold)

---

## 🖋️ Auteurs & maintenance

* **Lyuta** : pipeline & data‑engineering
* **…**

*🔥 Happy streaming & stay sporty!*
