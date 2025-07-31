#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from confluent_kafka import TopicPartition
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ====== Slack config ======
SLACK_TOKEN   = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")
if not SLACK_TOKEN or not SLACK_CHANNEL:
    raise RuntimeError("🔑 SLACK_BOT_TOKEN ou SLACK_CHANNEL_ID manquant")
slack = WebClient(token=SLACK_TOKEN)

def post_to_slack(a):
    ts = datetime.fromtimestamp(a["date_debut"] / 1_000_000).strftime("%Y-%m-%d %H:%M")
    text = (
        f"*Nouvelle activité sportive !* :runner:\n"
        f"> *Employé* : `{a['employee_id']}`\n"
        f"> *Type*    : *{a['type_activite']}*\n"
        f"> *Distance*: {a['distance_m']} m\n"
        f"> *Durée*   : {a['duree_s']} s\n"
        f"> *Date*    : {ts}"
    )
    while True:
        try:
            slack.chat_postMessage(channel=SLACK_CHANNEL, text=text)
            break
        except SlackApiError as e:
            if e.response.status_code == 429:
                retry = int(e.response.headers.get("Retry-After", 1))
                print(f"⏳ Rate-limited, retry dans {retry}s…")
                time.sleep(retry)
            else:
                raise

# ====== Kafka config ======
conf = {
    "bootstrap.servers": "redpanda:9092",
    "group.id": "sport-slack-bot",
    "auto.offset.reset": "earliest",
}
topic = "sportsdata.public.sport_activities"
consumer = Consumer(conf)
consumer.subscribe([topic])

# Attendre l'assignation des partitions
while not consumer.assignment():
    consumer.poll(1.0)

barrier = None
print("🔎 En attente du marqueur BACKFILL_COMPLETE…")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        payload = json.loads(msg.value()).get("payload", {})
        act = payload.get("after")
        if not act or payload.get("op") != "c":
            continue

        # 1) Détection du marqueur
        if act["type_activite"] == "BACKFILL_COMPLETE":
            barrier = msg.offset()
            print(f"🔔 Back‑fill terminé ! Barrier à l'offset {barrier}")
            continue

        # 2) Skip tant que le marker n'est pas atteint
        if barrier is None:
            continue

        # 3) Au‑delà, on poste
        if msg.offset() > barrier:
            post_to_slack(act)
            print(f"📤 Slack OK ID {act['id']} (offset {msg.offset()})")

except KeyboardInterrupt:
    print("⛔ Arrêt manuel")
finally:
    consumer.close()
