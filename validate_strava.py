#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pandas as pd
from dotenv import load_dotenv
import os
import great_expectations as ge
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.data_context.ephemeral_data_context import EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from datetime import datetime, timedelta

def main():
    try:
        print("▶️ Entrée dans le script, __name__ =", __name__)
        load_dotenv()

        # 1) Connexion SQL
        engine = create_engine(
            URL.create(
                "postgresql+psycopg2",
                username=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD") or None,
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=os.getenv("POSTGRES_DB", "sportsdb"),
            )
        )
        print("▶️ Connexion à la base établie")

        # 2) Chargement du DataFrame
        df = pd.read_sql("SELECT * FROM sport_activities;", engine)
        print(f"▶️ Récupération du DataFrame : {df.shape[0]} lignes, {df.shape[1]} colonnes")

        # 2bis) Filtrer BACKFILL_COMPLETE
        df = df[df['type_activite'] != 'BACKFILL_COMPLETE']
        print(f"▶️ Après filtrage BACKFILL_COMPLETE : {df.shape[0]} lignes restantes")

        # 3) Config DataContext
        project_config = DataContextConfig(
            config_version=3,
            datasources={
                "pandas_runtime": DatasourceConfig(
                    class_name="Datasource",
                    execution_engine={"class_name": "PandasExecutionEngine"},
                    data_connectors={
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["default_identifier_name"]
                        }
                    }
                )
            },
            stores={
                "expectations_store": {"class_name": "ExpectationsStore", "store_backend": {"class_name": "InMemoryStoreBackend"}},
                "validations_store": {"class_name": "ValidationsStore", "store_backend": {"class_name": "InMemoryStoreBackend"}},
                "evaluation_parameter_store": {"class_name": "EvaluationParameterStore", "store_backend": {"class_name": "InMemoryStoreBackend"}}
            },
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            data_docs_sites={},
            anonymous_usage_statistics={"enabled": False}
        )
        context = EphemeralDataContext(project_config=project_config)
        print("▶️ DataContext éphemère créé")

        # 4) Charger la suite JSON
        suite_path = os.path.join(os.getcwd(), "expectations", "sport_activities_suite.json")
        with open(suite_path, 'r', encoding='utf-8') as f:
            suite_dict = json.load(f)

        # 4bis) Supprimer expectation sur 'commentaire'
        suite_dict["expectations"] = [
            exp for exp in suite_dict["expectations"]
            if exp.get("kwargs", {}).get("column") != "commentaire"
        ]
        print("▶️ Expectation sur 'commentaire' supprimée")

        # 4ter) Injection dynamique des IDs RH valides
        df_rh = pd.read_excel("data/DonnéesRH.xlsx")
        valid_ids = df_rh["ID salarié"].dropna().astype(int).tolist()
        for exp in suite_dict["expectations"]:
            if exp.get("expectation_type") == "expect_column_values_to_be_in_set" \
               and exp["kwargs"].get("column") == "employee_id":
                exp["kwargs"]["value_set"] = valid_ids
                print(f"▶️ Expectation employee_id mise à jour avec {len(valid_ids)} IDs valides.")

        # 4quater) Mise à jour dynamique des bornes date_debut
        now = datetime.now()
        one_year_ago = now - timedelta(days=365)
        for exp in suite_dict.get("expectations", []):
            if exp.get("expectation_type") == "expect_column_values_to_be_between" \
               and exp["kwargs"].get("column") == "date_debut":
                exp["kwargs"]["min_value"] = one_year_ago
                exp["kwargs"]["max_value"] = now + timedelta(hours=1)

        suite = ExpectationSuite(**suite_dict)

        # 5) Affichage des expectations chargées
        print("▶️ Expectations chargées :")
        for exp in suite_dict["expectations"]:
            print(f" - {exp['expectation_type']} sur '{exp['kwargs'].get('column')}' avec {exp['kwargs']}")

        # 6) RuntimeBatchRequest
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_runtime",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="sport_activities_df",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "run_local"},
        )
        print("▶️ RuntimeBatchRequest configuré")

        # 7) Validator et validation
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        print("▶️ Validator instancié avec la suite existante")

        results = validator.validate()
        success = results.get("success")
        print("✅ Succès :", success)
        print(json.dumps(results.get("statistics", {}), indent=2))

        # 8) Gestion des échecs et affichage des IDs invalides si besoin
        if not success:
            print("❗ Détails des attentes échouées :")
            for r in results.get("results", []):
                if not r.get("success", True):
                    exp_conf = r.get("expectation_config", {})
                    col = exp_conf.get("kwargs", {}).get("column")
                    unexpected_count = r.get("result", {}).get("unexpected_count")
                    print(f" - {exp_conf.get('expectation_type')} sur '{col}' a échoué ({unexpected_count} valeurs inattendues)")

                    # 🔹 BONUS : afficher les IDs invalides pour employee_id
                    if col == "employee_id":
                        invalid_ids = df.loc[~df["employee_id"].isin(valid_ids), "employee_id"].unique()
                        print(f"   → IDs non trouvés dans DonnéesRH.xlsx : {list(invalid_ids)}")

            raise SystemExit("Validation échouée : voir attentes échouées ci-dessus.")

    except Exception as e:
        print("❌ Erreur attrapée :", type(e).__name__, str(e))
        raise

if __name__ == "__main__":
    main()
