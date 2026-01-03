# 🎬 CinéExplorer

**Projet Bases de Données Avancées - Polytech Marseille (4A)**

Plateforme d'exploration de films basée sur les données IMDb. Le projet met en œuvre une **architecture hybride** combinant la rigueur d'une base relationnelle (**SQLite**) pour la recherche et la flexibilité d'une base orientée documents (**MongoDB**) en cluster pour la haute disponibilité.

## 🏗 Phases du Projet

| Phase | Technologie | Objectif | État |
| :--- | :--- | :--- | :---: |
| **1** | **SQLite** | Modélisation, Import & Optimisation SQL | ✅ Fait |
| **2** | **MongoDB** | Migration & Enrichissement des données | ✅ Fait |
| **3** | **Replica Set** | Cluster Local (3 nœuds), Haute Disponibilité | ✅ Fait |
| **4** | **Django** | Interface Web, Search Engine & Data Viz | ✅ Fait |

---

## ⚙️ Architecture Technique

Le projet utilise une approche **Polyglot Persistence** :
* **SQLite :** Gestion du catalogue de base, filtres complexes (SQL), et recherche textuelle.
* **MongoDB (Replica Set) :** Stockage des fiches enrichies (Casting, Réalisateurs, Scénaristes, Titres alternatifs). Cluster de 3 instances locales (ports 27017, 27018, 27019).
* **Django :** Framework Web assurant la liaison entre les deux bases et l'interface.
* **Bootstrap 5 :** Interface Responsive (Mobile-First).

---

## 🚀 Installation & Démarrage

### 1. Pré-requis
* Python 3.9+
* MongoDB installé localement (`mongod` et `mongosh` accessibles dans le PATH)

### 2. Environnement Python

```bash
# Création et activation de l'environnement virtuel
python3 -m venv .venv
source .venv/bin/activate

# Installation des dépendances
pip install -r requirements.txt
```

### 3. Initialisation de MongoDB Replica Set

```bash
# Rendre le script exécutable
chmod +x scripts/phase3_replica/setup_replica.sh

# Lancer le cluster (Nettoie les anciens processus et démarre 3 nœuds)
sh scripts/phase3_replica/setup_replica.sh
```

### 4. Préparation des Données

Si la base est vide, voici comment importer les données :

```bash
# 1. Création de la structure de la base SQLite
# Attention : Cette commande réinitialise/écrase la base existante !
python3 scripts/phase1_sqlite/create_schema.py

# 2. Import des données depuis les fichiers TSV vers SQLite
# Ce processus peut prendre un certain temps en fonction de votre machine
python3 scripts/phase1_sqlite/import_data.py

# 3. Migration et Enrichissement vers MongoDB
# Connecte SQLite et injecte les données structurées dans le Cluster Mongo
python3 scripts/phase2_mongodb/migrate_enriched.py
```


### 5. Démarrage de l'Application Django

```bash
# Lancer le serveur de développement Django
python3 manage.py runserver
```

L'application sera accessible à l'adresse : http://127.0.0.1:8000