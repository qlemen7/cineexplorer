import sqlite3
import os

DB_PATH = 'data/cineexplorer.db'

print(f"🔍 INSPECTION DE : {DB_PATH}")

if not os.path.exists(DB_PATH):
    print("❌ Le fichier n'existe pas !")
    exit()

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 1. Lister les tables
print("\n📂 TABLES TROUVÉES :")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
print(tables)

# 2. Analyser la table principale (titles ou movies)
target_table = None
if 'titles' in tables: target_table = 'titles'
elif 'movies' in tables: target_table = 'movies'

if target_table:
    print(f"\n📋 STRUCTURE DE LA TABLE '{target_table}' :")
    cursor.execute(f"PRAGMA table_info({target_table})")
    columns = [row[1] for row in cursor.fetchall()]
    print(columns)
    
    print(f"\n🧪 EXEMPLE DE DONNÉE (1ère ligne) :")
    row = cursor.execute(f"SELECT * FROM {target_table} LIMIT 1").fetchone()
    if row:
        print(dict(row))
    else:
        print("⚠️ La table est vide !")
else:
    print("\n❌ Aucune table 'titles' ou 'movies' trouvée !")

conn.close()