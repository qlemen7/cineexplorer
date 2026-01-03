import sqlite3
import pymongo
from pymongo import MongoClient
import os
import time
import sys

# Configuration des chemins et bases
# On remonte de 2 niveaux depuis scripts/phase2_mongodb/ pour trouver data/imdb.db
SQLITE_DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB_NAME = 'imdb_project'

# Liste exacte des tables créées en Phase 1
TABLES_TO_MIGRATE = [
    'movies', 'persons', 'ratings', 'genres', 
    'principals', 'directors', 'writers', 
    'titles', 'characters', 'professions', 'known_for'
]

def get_sqlite_connection():
    """Crée une connexion SQLite qui renvoie des dictionnaires"""
    if not os.path.exists(SQLITE_DB_PATH):
        print(f"❌ ERREUR CRITIQUE : Base SQLite introuvable ici : {SQLITE_DB_PATH}")
        sys.exit(1)
        
    conn = sqlite3.connect(SQLITE_DB_PATH)
    # Row factory permet d'accéder aux colonnes par leur nom (clé -> valeur)
    conn.row_factory = sqlite3.Row 
    return conn

def migrate_table(table_name, db_mongo, cursor_sql):
    print(f"📦 Migration de la table '{table_name}'...", end=' ', flush=True)
    start_t = time.time()
    
    # 1. Vérifier combien de lignes on a dans SQLite
    try:
        count_sql = cursor_sql.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except sqlite3.OperationalError:
        print(f"\n⚠️  Table '{table_name}' introuvable dans SQLite. Ignorée.")
        return

    # 2. Préparer la collection Mongo (On vide avant pour éviter les doublons si on relance)
    collection = db_mongo[table_name]
    collection.drop() # Reset complet
    
    # 3. Lire et insérer par batch (Performant et économe en RAM)
    cursor_sql.execute(f"SELECT * FROM {table_name}")
    
    batch_size = 5000
    total_inserted = 0
    
    while True:
        rows = cursor_sql.fetchmany(batch_size)
        if not rows:
            break
            
        # Conversion row SQLite -> Dict Python
        documents = [dict(row) for row in rows]
        
        # Insertion massive dans MongoDB
        if documents:
            collection.insert_many(documents)
            total_inserted += len(documents)
            
    duration = time.time() - start_t
    
    # 4. Vérification d'intégrité simple
    if total_inserted == count_sql:
        print(f"✅ OK ({total_inserted} docs en {duration:.2f}s)")
    else:
        print(f"⚠️  ATTENTION : {total_inserted} insérés vs {count_sql} source")

def run_migration():
    print(f"--- DÉBUT MIGRATION SQLITE -> MONGO (Base: {MONGO_DB_NAME}) ---")
    start_global = time.time()
    
    # Connexions
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info() # Déclenche une erreur si pas connecté
        db_mongo = client[MONGO_DB_NAME]
    except Exception as e:
        print(f"❌ Impossible de se connecter à MongoDB : {e}")
        print("💡 Astuce : Lancez 'mongod --dbpath data/mongo/standalone' dans un autre terminal.")
        return

    conn_sql = get_sqlite_connection()
    cursor_sql = conn_sql.cursor()
    
    # Boucle sur les tables
    for table in TABLES_TO_MIGRATE:
        migrate_table(table, db_mongo, cursor_sql)
        
    conn_sql.close()
    client.close()
    
    print(f"\n🎉 MIGRATION TERMINÉE en {time.time() - start_global:.2f}s")
    print(f"👉 Vérifiez vos données avec MongoDB Compass ou le shell.")

if __name__ == "__main__":
    run_migration()