from airflow.models import DagBag


def charger_dags():
    # Airflow 3.4 : DagBag n'a plus l'argument include_examples,
    # les DAGs d'exemple sont coupes par AIRFLOW__CORE__LOAD_EXAMPLES=False
    return DagBag(dag_folder="dags")


def test_aucune_erreur_import():
    dag_bag = charger_dags()
    assert dag_bag.import_errors == {}, f"Erreurs d'import : {dag_bag.import_errors}"


def test_user_processing_existe():
    dag_bag = charger_dags()
    assert "user_processing" in dag_bag.dags


def test_user_processing_taches():
    dag = charger_dags().dags["user_processing"]
    taches = {t.task_id for t in dag.tasks}
    attendues = {
        "create_table",
        "is_api_available",
        "extract_user",
        "process_user",
        "store_user",
    }
    assert taches == attendues, f"Taches trouvees : {taches}"
