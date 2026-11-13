# TP Airflow - pipeline user_processing

import csv
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue

API_URL = "https://raw.githubusercontent.com/FatimaMoqran/airflow-datasets/refs/heads/main/fakeuser.json"
CSV_PATH = "/tmp/user_info.csv"
# meme ordre que dans la table users sinon le COPY marche pas
COLUMNS = ["id", "firstname", "lastname", "email", "created_at"]


# version 1 avec PythonOperator (partie 5), remplacee par @task plus bas
# from airflow.providers.standard.operators.python import PythonOperator
#
# def _extract_user(ti):
#     fake_user = ti.xcom_pull(task_ids="is_api_available")
#     return {
#         "id": fake_user["id"],
#         "firstname": fake_user["personalInfo"]["firstName"],
#         "lastname": fake_user["personalInfo"]["lastName"],
#         "email": fake_user["personalInfo"]["email"],
#     }
#
# # dans le dag :
#     extract_user = PythonOperator(
#         task_id="extract_user",
#         python_callable=_extract_user,
#     )


@dag
def user_processing():

    # partie 2 : creation de la table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    # partie 4 : on verifie toutes les 30s, echec au bout de 5 min
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get(API_URL)
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    # partie 5 avec @task (plus besoin de ti et xcom_pull)
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    # partie 6 : on ecrit l'utilisateur dans un csv
    @task
    def process_user(user_info):
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerow(user_info)

    # partie 7 + bonus
    # premiere version (plantait au 2e run a cause de la cle primaire) :
    #     hook = PostgresHook(postgres_conn_id="postgres")
    #     hook.copy_expert(sql="COPY users FROM STDIN WITH CSV HEADER", filename=CSV_PATH)
    # maintenant on passe par une table users_staging et on ignore les doublons
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.run(
            "CREATE TABLE IF NOT EXISTS users_staging (LIKE users INCLUDING DEFAULTS);"
            "TRUNCATE users_staging;"
        )
        hook.copy_expert(
            sql="COPY users_staging FROM STDIN WITH CSV HEADER",
            filename=CSV_PATH,
        )
        hook.run(
            "INSERT INTO users SELECT * FROM users_staging ON CONFLICT (id) DO NOTHING;"
        )

    # partie 8 : dependances
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    processed = process_user(user_info)

    create_table >> fake_user
    processed >> store_user()


user_processing()
