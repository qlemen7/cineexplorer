🎬 CinéExplorer

Plateforme Web de découverte de films basée sur les données IMDb, développée dans le cadre du module 4A-BDA (Polytech Marseille).

Ce projet explore le cycle de vie complet de la donnée : du relationnel (SQLite) au NoSQL (MongoDB), en passant par la distribution (Replica Set) et l'interface utilisateur (Django).

🚀 Installation & Prérequis

1. Environnement Virtuel

Il est conseillé de travailler dans un environnement isolé pour ne pas polluer votre système.

# Création
python3 -m venv .venv

# Activation (Mac/Linux)
source .venv/bin/activate

# Installation des dépendances
pip install -r requirements.txt


2. Données

Placez les fichiers CSV décompressés d'IMDb (movies.csv, persons.csv, etc.) dans le dossier :
cineexplorer/data/csv/

✅ Phase 1 : Exploration & Base SQLite (Terminée)

Cette phase consistait à maîtriser les données relationnelles et l'optimisation SQL.

📂 Structure des scripts (scripts/phase1_sqlite/)

create_schema.py : Initialise la base de données imdb.db avec un schéma normalisé en 3NF (gestion des clés étrangères, types de données).

import_data.py : Importe massivement les CSV dans SQLite.

Optimisations : Utilisation de transactions, désactivation temporaire des contraintes, nettoyage des valeurs \N.

Correctifs : Gestion automatique des en-têtes CSV malformés et inversion des colonnes ordering/pid dans principals.csv.

queries.py : Implémentation de 9 requêtes SQL avancées (CTE, Window Functions, Agrégations complexes).

benchmark.py : Mesure des performances et création automatique des index.

debug_phase1.py : Script utilitaire pour diagnostiquer l'intégrité des données (liens orphelins, tables vides).

📊 Exploration (data/exploration.ipynb)

Notebook Jupyter complet pour l'analyse exploratoire :

Statistiques descriptives (valeurs manquantes, types).

Visualisations (Distribution par année, Top Genres).

Vérification de l'intégrité référentielle avant import.

⚡ Résultats de performance

L'ajout d'index stratégiques sur les colonnes de filtrage et de jointure a permis des gains massifs (ex: +99.9% sur les requêtes de collaboration).

📝 Livrable

Le rapport PDF a été généré via LaTeX dans reports/livrable1/rapport.tex.

🔜 Phases Suivantes

Phase 2 : Migration MongoDB (Collections plates vs structurées).

Phase 3 : Distribution (Replica Set, tolérance aux pannes).

Phase 4 : Interface Web (Application Django complète).

Auteur : Le C (4A Info)