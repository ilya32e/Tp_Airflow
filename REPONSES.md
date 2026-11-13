# TP Airflow - Mon premier pipeline de données

Le code complet est dans dags/user_processing.py.

Remarque : chez moi le port 8080 était déjà pris par un autre projet, donc j'ai mis l'interface sur le port 8081 dans le docker-compose.yaml (`"8081:8080"`). J'ai aussi désactivé les DAGs d'exemple pour ne voir que le mien.


## Partie 0 - Mise en place

Le conteneur airflow-init qui est en "Exited", ce n'est pas un problème. Il sert juste à initialiser au démarrage (création de la base de métadonnées, du compte airflow...) et une fois que c'est fait il s'arrête. Les autres conteneurs attendent qu'il ait fini pour démarrer.

Sur la page d'accueil j'ai MetaDatabase, Scheduler, Triggerer et DAG Processor en Healthy. Ça correspond au tableau, à part l'API server (qui marche forcément puisque la page s'affiche) et le worker.


## Partie 1 - Définir le DAG

```python
from airflow.sdk import dag

@dag
def user_processing():
    pass

user_processing()
```

Si on enlève la dernière ligne, le DAG n'est jamais vraiment créé, on a juste défini une fonction. J'ai testé : il n'y a aucune erreur d'import, mais `airflow tasks list user_processing` me dit que le DAG est introuvable. Comme il était déjà enregistré il est resté un peu dans la liste avant de disparaître, mais pour un nouveau DAG il n'apparaîtrait jamais.

Le décorateur dag est dans `airflow.sdk` (avant, dans Airflow 2, c'était `airflow.decorators`).


## Partie 2 - Créer la table users

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        ...
    )
    """
)
```

Pourquoi postgres et pas localhost : la tâche tourne dans un conteneur, donc localhost ce serait le conteneur lui-même et pas la base. Dans le docker-compose.yaml le service de la base s'appelle postgres, et Docker permet aux conteneurs de se joindre avec le nom du service.

5432 c'est le port par défaut de PostgreSQL. Pour MySQL c'est 3306.

Sans le IF NOT EXISTS, au deuxième lancement la tâche plante parce que la table existe déjà ("relation users already exists").

Pour Elasticsearch, le type n'est pas dans la liste parce que le provider n'est pas installé. Les types de connexion viennent des providers, donc il faudrait installer apache-airflow-providers-elasticsearch dans l'image Airflow et redémarrer.


## Partie 3 - Tester avec la CLI

```bash
docker compose exec airflow-scheduler bash
airflow tasks test user_processing create_table
```

Les deux arguments sont le dag_id et le task_id (on peut aussi donner une date en option).

Pour voir les tâches qu'Airflow détecte : `airflow tasks list user_processing`

Pour vérifier la table j'ai fait `airflow db shell` puis `\d users`, ou directement `SELECT * FROM users;`.

Les logs finissent bien en success.


## Partie 4 - Le sensor

```python
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
```

(30 secondes entre chaque vérification, et 300 secondes = 5 minutes avant l'échec)

Le code 200 veut dire que tout va bien. Autres codes : 404 quand la page n'existe pas, 500 quand il y a une erreur côté serveur.

is_done indique si la condition est remplie. Si c'est False, le sensor réessaie 30 secondes plus tard. xcom_value c'est ce que le sensor envoie à la tâche suivante quand il a réussi, ici le JSON de l'utilisateur.

L'import de requests est dans la fonction parce qu'Airflow relit le fichier du DAG très souvent, et tout ce qui est en haut du fichier est exécuté à chaque fois. Comme ça, requests n'est importé que quand la tâche tourne vraiment.

Le HttpSensor fait le même genre de vérification mais il renvoie juste vrai ou faux, il ne renvoie pas les données. Le nôtre renvoie directement le JSON, donc pas besoin de rappeler l'API après.


## Partie 5 - Extraire l'utilisateur

Version avec PythonOperator :

```python
def _extract_user(ti):
    fake_user = ti.xcom_pull(task_ids="is_api_available")
    return {
        "id": fake_user["id"],
        "firstname": fake_user["personalInfo"]["firstName"],
        "lastname": fake_user["personalInfo"]["lastName"],
        "email": fake_user["personalInfo"]["email"],
    }

extract_user = PythonOperator(
    task_id="extract_user",
    python_callable=_extract_user,
)
```

Version avec @task :

```python
@task
def extract_user(fake_user):
    return {
        "id": fake_user["id"],
        "firstname": fake_user["personalInfo"]["firstName"],
        "lastname": fake_user["personalInfo"]["lastName"],
        "email": fake_user["personalInfo"]["email"],
    }

fake_user = is_api_available()
user_info = extract_user(fake_user)
```

ti c'est la task instance, l'exécution en cours de la tâche. C'est avec ça qu'on fait xcom_pull. Il ne faut pas le renommer parce qu'Airflow le reconnaît par son nom pour le passer à la fonction.

Pour voir un XCom : on clique sur la tâche dans un run, puis onglet XCom.

Oui, les clés sont sensibles à la casse. J'ai essayé avec firstname au lieu de firstName et ça donne `KeyError: 'firstname'`.

Le test renvoie bien : {'id': '4815162', 'firstname': 'Camille', 'lastname': 'Martin', 'email': 'camille.martin@example.com'}


## Partie 6 - Sauvegarder en CSV

```python
@task
def process_user(user_info):
    user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("/tmp/user_info.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "firstname", "lastname", "email", "created_at"])
        writer.writeheader()
        writer.writerow(user_info)
```

L'ordre des colonnes est important parce que COPY (partie 7) remplit les colonnes dans l'ordre, il ne regarde pas les noms. Si l'ordre n'est pas le même que dans la table, les valeurs se retrouvent dans les mauvaises colonnes.

Le /tmp c'est celui du conteneur qui exécute la tâche, pas mon PC. Avec airflow tasks test c'est le scheduler, et pendant un vrai run c'est le worker.

En production avec plusieurs workers, process_user et store_user pourraient tourner sur deux workers différents, et le deuxième ne trouverait pas le fichier. Il faudrait le mettre sur un stockage partagé, par exemple S3.

Résultat dans le conteneur :
```
id,firstname,lastname,email,created_at
4815162,Camille,Martin,camille.martin@example.com,2026-11-13 09:37:15
```


## Partie 7 - Stocker dans PostgreSQL avec un hook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users FROM STDIN WITH CSV HEADER",
        filename="/tmp/user_info.csv",
    )
```

Installer le provider dans le .venv ne suffit pas parce que le .venv sert juste à VS Code (pour l'autocomplétion). Le code tourne en vrai dans les conteneurs Docker, qui ont leur propre Python.

HEADER veut dire que la première ligne du CSV contient les noms des colonnes, donc COPY ne l'insère pas.

La table a 5 colonnes. Avant d'ajouter created_at le CSV n'en avait que 4, donc ça n'aurait pas marché.


## Partie 8 - Dépendances et lancement

```python
fake_user = is_api_available()
user_info = extract_user(fake_user)
processed = process_user(user_info)

create_table >> fake_user
processed >> store_user()
```

Sur une seule ligne, create_table doit être juste avant is_api_available() parce que c'est la première tâche de la chaîne. Il faut que la table soit créée avant que tout le reste commence.

Pour lancer tous les jours à 8h : `@dag(schedule="0 8 * * *", start_date=datetime(2026, 1, 1), catchup=False)`

Quand on relance une deuxième fois, store_user plante avec "duplicate key value violates unique constraint". L'API renvoie toujours le même utilisateur avec le même id, et comme id est la clé primaire on ne peut pas l'insérer deux fois. Les autres tâches restent vertes.

Après le premier run, les 5 tâches sont vertes et la table contient :
```
   id    | firstname | lastname |           email            |     created_at
---------+-----------+----------+----------------------------+---------------------
 4815162 | Camille   | Martin   | camille.martin@example.com | 2026-11-13 09:33:57
```


## Bonus - Idempotence

J'ai choisi la piste 2 (table temporaire + ON CONFLICT DO NOTHING), parce qu'avec TRUNCATE on perd tous les anciens utilisateurs.

```python
@task
def store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.run("CREATE TABLE IF NOT EXISTS users_staging (LIKE users INCLUDING DEFAULTS); TRUNCATE users_staging;")
    hook.copy_expert(sql="COPY users_staging FROM STDIN WITH CSV HEADER", filename="/tmp/user_info.csv")
    hook.run("INSERT INTO users SELECT * FROM users_staging ON CONFLICT (id) DO NOTHING;")
```

J'ai d'abord essayé avec une vraie table TEMP mais ça ne marchait pas : copy_expert ouvre sa propre connexion, et une table temporaire n'existe que dans la connexion qui l'a créée. Du coup j'ai fait une table users_staging normale, que je vide à chaque fois.

C'est TRUNCATE qui perd l'historique. Pour une vraie API qui renvoie de nouveaux utilisateurs à chaque fois, il vaut mieux la piste 2, comme ça les nouveaux s'ajoutent et les doublons sont ignorés.

Pour mettre à jour l'utilisateur au lieu de l'ignorer :
```sql
INSERT INTO users SELECT * FROM users_staging
ON CONFLICT (id) DO UPDATE SET
    firstname = EXCLUDED.firstname,
    lastname = EXCLUDED.lastname,
    email = EXCLUDED.email,
    created_at = EXCLUDED.created_at;
```

create_table est idempotente grâce au IF NOT EXISTS : si la table existe déjà elle ne fait rien.

J'ai lancé le DAG 3 fois de suite, tout est vert et il n'y a toujours qu'une seule ligne dans la table.


## Bilan

1. Un opérateur c'est un modèle de tâche déjà codé (par exemple SQLExecuteQueryOperator pour le SQL). Quand on l'utilise dans un DAG avec un task_id, ça devient une tâche. Le DAG c'est l'ensemble des tâches et l'ordre dans lequel elles s'exécutent.

2. Pour la sécurité d'abord, le code va sur Git donc on ne met pas de mot de passe dedans. Et si les identifiants changent on modifie juste la connexion dans Airflow, pas le code. Le code connaît seulement le conn_id.

3. Non, c'est une tâche comme les autres, c'est le worker qui l'exécute. Il prend une place sur le worker pendant tout le temps où il attend.

4. Avec @task on écrit moins de code, pas besoin de task_id ni de python_callable, et les XComs se font tout seuls quand on passe le résultat d'une fonction à une autre. L'inconvénient c'est qu'on voit moins ce qui se passe, et tout ce qu'on retourne est mis en XCom.

5. Dans la base de métadonnées d'Airflow. Elle n'est pas faite pour des gros fichiers, ça ralentirait tout. Pour un gros fichier on le met ailleurs (S3 par exemple) et on passe juste le chemin.

6. Le hook c'est la connexion à un outil avec ses fonctions, l'opérateur c'est une tâche toute prête qui utilise un hook. On a eu besoin du hook dans la partie 7 pour copy_expert, que l'opérateur ne propose pas.

7. extract_user reçoit fake_user en paramètre, donc Airflow comprend tout seul l'ordre. create_table ne donne rien au sensor, donc Airflow ne peut pas savoir qu'elle doit passer avant, il faut l'écrire.

8. Le fichier dans /tmp, qui ne marchera pas avec plusieurs workers (il faudrait un stockage partagé). Et on stocke nos données dans la base de métadonnées d'Airflow avec airflow/airflow, il faudrait une base séparée avec de vrais identifiants. On pourrait aussi ajouter des retries et un schedule.
