import sqlite3
import pymongo
import os
import sys

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQLITE_DB = os.path.join(BASE_DIR, 'data', 'cineexplorer.db')
MONGO_URI = 'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0'
DB_NAME = 'cineexplorer'

def migrate():
    print(f"🚀 Démarrage de la migration COMPLÈTE vers le Cluster...")
    
    # 1. Connexion MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        db = client[DB_NAME]
        collection = db['movies']
        print("✅ Connexion MongoDB : OK")
    except Exception as e:
        print(f"❌ Erreur connexion Mongo : {e}")
        return

    # 2. Connexion SQLite
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 3. Requête SANS LIMIT
    print("📥 Lecture de TOUS les films depuis SQLite (patience)...")
    try:
        query = """
            SELECT 
                m.movie_id as _id, 
                m.primary_title as title, 
                m.start_year as year,
                m.runtime_minutes as runtime,
                m.is_adult,
                r.average_rating,
                r.num_votes
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            WHERE m.title_type = 'movie'
        """
        # Note : On a enlevé le LIMIT ici !
        cursor.execute(query)
        # On utilise l'itérateur pour ne pas tout charger en RAM d'un coup si c'est énorme
    except Exception as e:
        print(f"❌ Erreur SQL : {e}")
        return

    # 4. Insertion par lots (Batch) pour la performance
    collection.delete_many({}) # On nettoie avant
    print("🧹 Collection MongoDB vidée.")

    batch = []
    count = 0
    BATCH_SIZE = 5000 # On envoie par paquets de 5000

    for row in cursor:
        data = dict(row)
        
        # Récupération des genres (requête séparée ou simplifiée ici pour la perf)
        # Pour aller vite, on mettra les genres à jour plus tard ou on fait une sous-requête.
        # Ici on fait simple :
        doc = {
            "_id": data['_id'],
            "title": data['title'],
            "year": data['year'],
            "runtime": data['runtime'],
            "is_adult": bool(data['is_adult']),
            "genres": [], # Idéalement il faudrait joindre les genres, mais pour le détail c'est déjà bien
            "rating": {
                "average": data['average_rating'],
                "votes": data['num_votes']
            }
        }
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch)
            count += len(batch)
            batch = []
            print(f"   ... {count} films migrés", end='\r')

    # Finir le dernier paquet
    if batch:
        collection.insert_many(batch)
        count += len(batch)

    print(f"\n🎉 MIGRATION TERMINÉE : {count} films transférés !")
    
    # Création des index
    print("🔨 Indexation...")
    collection.create_index("title")
    
    conn.close()
    client.close()

if __name__ == "__main__":
    migrate()