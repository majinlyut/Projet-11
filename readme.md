# Sport Data Solution — ETL streaming d’activités sportives

## 🚀 Objectif
Ce projet implémente un **pipeline de streaming en temps réel** pour :
- **Ingestion** en continu des événements sportifs (Strava) 
- **Transformation et enrichissement** via Spark Structured Streaming 
- **Stockage** bdd Postgresql et delta lake
- **Monitoring en temps réel** avec Prometheus & Grafana
- **Notifications instantanées** via Slack des activités des emplyoé
- **Tableaux de bord** rapport Power BI avec les indicateurs à suivre (primes total,jours bien-être...)

---

## 📂 Structure du projet
```
.
├── docker-compose.yml        # Orchestration des services
├── requirements.txt          # Dépendances Python
├── maps.py                   # Calcul la distance maison/travail
├── strava_generator.py       # Générateur de flux d'activités sportives sur 1 an
├── strava_generator1.py      # Générateur d'une activité à date pour test
├── validate_strava.py        # Validation qualité des données
├── config/                   # Config Spark & YAML
├── connect/                  # Docker Kafka Connect + JMX
├── data/                     # Données Excel initiales (RH & sportives)
├── expectations/             # Tests Great Expectations
├── init/                     # Scripts d'initialisation PostgreSQL
├── monitoring/               # Config Prometheus + Grafana
├── slack/                    # Notifications Slack
├── spark/                    # Jobs Spark bronze/gold + métriques
├── spark_thrift/             # Dockerfile pour Spark Thrift
└── topic_creator/            # Docker pour créer les topics Redpanda
```

---

## 🛠️ Stack technique
- **Redpanda** pour l’ingestion d’événements en continu
- **Spark Structured Streaming** pour les traitements temps réel
- **Delta Lake** pour le stockage intermédiaire et la gestion des versions
- **PostgreSQL** comme base relationnelle cible
- **Great Expectations** pour la validation continue de la qualité
- **Prometheus + Grafana** pour visualiser les métriques en temps réel
- **Slack API** pour notifier instantanément les utilisateurs

---

## ▶️ Lancer le pipeline

1. **Cloner le repo**
   ```bash
   git clone <repo>
   cd projet11
   ```

2. **Construire & lancer les services**
   ```bash
   docker-compose up --build
   ```

3. **Lancer le script de calcul de distance Domicile/Travail (à faire 1 seule fois)**
   ```bash
   python maps.py
   ```


4. **Démarrer le connecteur Debezium**
   ```bash
   curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "user",
      "database.password": "password",
      "database.dbname": "sportsdb",
      "topic.prefix": "sportsdata",
      "table.include.list": "public.sport_activities",
      "publication.name": "debezium_publication",
      "slot.name": "debezium_slot",
      "tombstones.on.delete": "false"
    }
  }'

   ```

5. **Démarrer le flux de données**
   ```bash
   python strava_generator.py
   ```

6. **Vérifier la qualité des données**
   ```bash
   python validate_strava.py
   ```

## 📊 Monitoring en temps réel
- **Prometheus** : `http://localhost:9090`
- **Grafana** : `http://localhost:3000` (importer `monitoring/grafana.json` pour visualiser les dashboards)
---
## 📊 Architecture

<img width="960" height="308" alt="image" src="https://github.com/user-attachments/assets/1c2d4783-fb6e-4949-a395-9d7e6674ac7d" />


1-Les données sont importées dans la BDD postgresql
2-Debezium récupère le delta de la base(insertion,modification,supression)
3-Redpanda git comme bus de données temps réel, assurant la mise en file et la diffusion scalable vers les consommateurs 
4-Le consommateur python publie un message de nouvelle activité sur le canal Slack de l'équipe
5-Le consommateur spark streaming écrit les données brut dans un deltalake bronze
6-Un second job Spark tranforme les données et les agrèges avec les données RH puis écrit dans un delta lake Gold
7-Les données enrichies et fiables sont prêtes à être exploité par Power BI ou exports.

## 📊 Monitoring en temps réel
- **Prometheus** : `http://localhost:9090`
- **Grafana** : `http://localhost:3000` (importer `monitoring/grafana.json` pour visualiser les dashboards)




---

