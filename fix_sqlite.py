import sqlite3
import os

# On s'assure que le dossier data existe
if not os.path.exists('data'):
    os.makedirs('data')

# Chemin exact attendu par Django
db_path = 'data/cineexplorer.db'

print(f"Création de la base de données : {db_path} ...")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Création de la table movies
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            year INTEGER
        )
    ''')

    # 2. Création de la table people
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    ''')

    # 3. Insertion de fausses données pour l'affichage
    cursor.execute("INSERT INTO movies (title, year) VALUES ('Film Test SQLite', 2025)")
    cursor.execute("INSERT INTO people (name) VALUES ('Clément le GOAT')")
    
    # On valide
    conn.commit()
    conn.close()
    print("Succès ! Le fichier data/cineexplorer.db est créé.")

except Exception as e:
    print(f"Erreur : {e}")