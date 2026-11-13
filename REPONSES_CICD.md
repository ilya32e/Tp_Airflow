# TP CI/CD - GitHub Actions pour Airflow

Dépôt : https://github.com/ilya32e/Tp_Airflow (onglet Actions pour voir les runs)

Fichiers ajoutés pour ce TP :
- `.github/workflows/ci.yml` : le workflow, avec les jobs `lint` et `test-dags`
- `tests/test_dags.py` : les 3 tests pytest
- `.gitignore` : complété avec `.env` et `.pytest_cache/`

Remarque : mon projet est en Airflow 3.4.0 (ligne `image:` du docker-compose.yaml) et pas 3.1.0, donc j'ai mis `AIRFLOW_VERSION: "3.4.0"` dans le workflow. Dans cette version `DagBag` n'accepte plus l'argument `include_examples` (`TypeError: DagBag.__init__() got an unexpected keyword argument 'include_examples'`), je l'ai donc enlevé. Les DAGs d'exemple sont déjà coupés par la variable `AIRFLOW__CORE__LOAD_EXAMPLES: "False"` du job.


## Partie 1 - Mettre le projet sur GitHub

Le projet était déjà sur GitHub depuis le TP précédent, j'ai seulement complété le `.gitignore`. Le fichier `.env` était suivi par Git, je l'ai retiré du dépôt avec `git rm --cached .env` (il reste sur mon PC).

`git status` avant le commit sert à voir ce qui va vraiment partir : les fichiers ajoutés, modifiés, et ceux qui ne sont pas suivis. C'est le moment de vérifier qu'on n'envoie pas un fichier par erreur.

Sans le `.gitignore`, on aurait envoyé le dossier `.venv/` (des milliers de fichiers), les `logs/` d'Airflow, les `__pycache__/`, le `.pytest_cache/` et surtout le `.env`, qui peut contenir des secrets.

Vérification : sur GitHub on voit `dags/user_processing.py` et `docker-compose.yaml`, pas de dossier `logs`.


## Partie 2 - Premier workflow "Hello CI"

Le workflow s'est lancé tout seul au push et il est vert (run "Premier workflow CI").

Si on retire l'étape `actions/checkout@v4`, le runner reste vide : il n'a pas mon code. La commande `ls dags/` ne trouve pas le dossier (`No such file or directory`) et le job passe au rouge. C'est le checkout qui copie le dépôt sur la machine.

Si l'indentation YAML est fausse, GitHub n'arrive pas à lire le fichier : il affiche "Invalid workflow file" et aucun job ne démarre. Comme en Python, c'est l'indentation qui donne la structure, donc une étape mal décalée n'appartient plus au bon job. Il faut des espaces, pas de tabulations.


## Partie 3 - Lint avec flake8

En local :

```
uvx flake8 dags/ --select E9,F63,F7,F82 --show-source
uvx flake8 dags/ --max-line-length 120 --exit-zero
```

Les deux commandes n'affichent rien : pas d'erreur bloquante et pas de remarque de style sur mon DAG, donc je n'ai pas eu besoin d'autopep8.

- `F401` : un module est importé mais jamais utilisé (import inutile).
- `F821` : un nom est utilisé alors qu'il n'est défini nulle part (variable ou import oublié). Le code plante à l'exécution.

`ruff` fait la même chose que flake8 mais beaucoup plus vite, et il a des règles spéciales pour Airflow (les règles `AIR`), par exemple pour repérer les imports dépréciés d'Airflow 2.

Vérification : le job `lint` est vert sur GitHub.


## Partie 4 - Tester l'import des DAGs avec pytest

La ligne à compléter dans `test_user_processing_taches` :

```python
assert taches == attendues, f"Taches trouvees : {taches}"
```

`needs: lint` fait que `test-dags` ne démarre que si `lint` est vert. Si le lint échoue, `test-dags` n'est pas lancé du tout, il est marqué "skipped" (je l'ai vu dans le cas A de la partie 5). Ça évite d'installer Airflow pendant 2 minutes pour un code qui a déjà une erreur.

Le fichier de constraints donne les versions exactes de toutes les dépendances qui ont été testées avec cette version d'Airflow. Sans lui, pip prend les dernières versions de chaque librairie, et l'une d'elles peut être incompatible : l'installation ou l'import d'Airflow peut casser du jour au lendemain sans qu'on ait touché au code.

Le runner part de zéro, il n'utilise pas mon image Docker : il installe Airflow avec pip. Mon DAG importe `airflow.providers.postgres`, donc si le provider n'est pas installé dans la CI, le DAG ne se charge pas (`ModuleNotFoundError`) et le test d'import échoue.

Vérification : les deux jobs sont verts et le log de "Tester les DAGs" affiche `3 passed`.


## Partie 5 - Casser un DAG, lire l'échec, corriger

| Cas | Modification | Job qui échoue | Message à retrouver |
|-----|--------------|----------------|---------------------|
| A | Suppression de l'import de `PostgresHook` | `lint` (étape "Erreurs bloquantes"), `test-dags` est skipped | `dags/user_processing.py:94:16: F821 undefined name 'PostgresHook'` |
| B | Suppression de l'appel final `user_processing()` | `test-dags` (étape "Tester les DAGs"), `lint` reste vert | `AssertionError: assert 'user_processing' in {}` et `KeyError: 'user_processing'` (2 failed, 1 passed) |
| C | Tâche `store_user` renommée en `save_user` | `test-dags` (étape "Tester les DAGs"), `lint` reste vert | `AssertionError: Taches trouvees : {..., 'save_user'}` avec `Extra items in the left set: 'save_user'` et `Extra items in the right set: 'store_user'` (1 failed, 2 passed) |

Pour chaque cas j'ai poussé la version cassée, puis je l'ai annulée avec `git revert` et repoussé. Les trois commits cassés sont rouges, les trois reverts sont verts.

Remarque sur le cas A : c'est seulement flake8 qui le voit. Si je lance pytest sur cette version, les 3 tests passent, parce que `PostgresHook` n'est utilisé que dans le corps de la tâche `store_user`, donc au chargement du DAG rien ne plante. L'erreur ne serait apparue qu'au moment où la tâche tourne. Les deux vérifications ne se remplacent pas.

Cas B, pourquoi flake8 ne voit rien : flake8 lit le code sans l'exécuter. Définir une fonction sans l'appeler, c'est du Python correct, il n'y a aucune règle contre ça. Il ne peut pas savoir qu'Airflow a besoin de cet appel pour créer le DAG. Il n'y a d'ailleurs pas d'erreur d'import non plus (`test_aucune_erreur_import` passe) : le DAG n'existe simplement pas.

Cas C, pourquoi c'est utile que le test échoue : le DAG marcherait, mais le nom d'une tâche est utilisé ailleurs. L'historique et les logs de la tâche dans Airflow sont rattachés à son `task_id`, et d'autres tâches peuvent en dépendre (`xcom_pull(task_ids=...)`). Le test sert de contrat : si on renomme une tâche, on doit le faire exprès et mettre à jour le test, pas le faire sans s'en rendre compte.

Cas B dans l'interface Airflow : ça aurait été long à trouver, parce qu'il n'y a aucun message d'erreur. Rien dans "DAG Import Errors", le DAG disparaît juste de la liste. Je l'avais vu au TP précédent : le DAG était resté un moment dans la liste avant de disparaître. Avec la CI on a le message en 2 minutes, avec le nom du test qui échoue.

Vérification : après le dernier revert, le dernier commit est vert.


## Pour aller plus loin - Pull request et protection de branche

Mon binôme ne peut pas valider la pull request à ma place : sur GitHub l'auteur d'une pull request ne peut pas l'approuver lui-même, c'est forcément quelqu'un d'autre. Si le binôme utilisait mon compte, la relecture ne servirait plus à rien.

La CI vérifie seulement ce qu'on lui a demandé : le code est propre, les DAGs se chargent, les tâches existent. Elle ne sait pas si la logique est bonne (la bonne table, la bonne requête SQL, un schedule cohérent), si le code est lisible, ou si un mot de passe a été laissé dans le code. Une personne peut voir ça, et en plus une deuxième personne connaît le code.


## Bilan

1. Deux erreurs du TP Airflow que cette CI aurait détectées :
   - l'appel `user_processing()` oublié en bas du fichier : le DAG était introuvable sans aucun message d'erreur. Le test `test_user_processing_existe` l'attrape (cas B).
   - un provider qui n'est pas installé là où le code tourne (le provider Postgres installé dans le `.venv` mais pas dans les conteneurs) : dans la CI ça donne une erreur d'import, attrapée par `test_aucune_erreur_import`.

2. Le lint dure quelques secondes, alors que les tests demandent d'installer Airflow (2 à 3 minutes). On lance le moins cher d'abord : si le code a une erreur évidente, on le sait tout de suite et on ne lance pas les tests pour rien. Et un code qui a une erreur de syntaxe ferait échouer les tests de toute façon.

3. Dans un projet MLOps, la CI peut aussi vérifier le modèle : qu'il se charge et qu'il fait une prédiction sur un exemple, que ses métriques (accuracy, F1...) restent au-dessus d'un seuil et ne baissent pas par rapport à la version précédente, et que les données ont le bon format (colonnes, types, valeurs manquantes).
