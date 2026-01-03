import pymongo
from pymongo import MongoClient, UpdateOne
import time
import sys

MONGO_DB_NAME = 'imdb_project'
SOURCE_COLLECTION = 'movies'
TARGET_COLLECTION = 'movies_complete'
BATCH_SIZE = 1000 # Nombre de films à traiter par lot

def get_db():
    client = MongoClient('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0')
    return client[MONGO_DB_NAME]

def migrate_structured():
    db = get_db()
    
    print(f"🚀 Démarrage de la structuration des données...")
    print(f"   Source: {SOURCE_COLLECTION} -> Cible: {TARGET_COLLECTION}")

    # 1. Nettoyage et Indexation préalable
    if TARGET_COLLECTION in db.list_collection_names():
        print("   ⚠️  Suppression de la collection cible existante...")
        db[TARGET_COLLECTION].drop()
    
    # 2. Récupération des IDs de films
    cursor = db[SOURCE_COLLECTION].find({"start_year": {"$ne": None}}, {"movie_id": 1})
    all_ids = [doc['movie_id'] for doc in cursor]
    total_movies = len(all_ids)
    print(f"   📋 {total_movies} films à traiter.")

    start_time = time.time()
    processed = 0

    # 3. Traitement par lot (Batch)
    for i in range(0, total_movies, BATCH_SIZE):
        batch_ids = all_ids[i : i + BATCH_SIZE]
        
        # --- LE CŒUR DU REACTEUR : LE PIPELINE D'AGRÉGATION ---
        pipeline = [
            # A. Filtrer le lot actuel
            { "$match": { "movie_id": { "$in": batch_ids } } },

            # B. Joindre les Notes (Rating) -> Objet simple
            { "$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "r"
            }},
            { "$unwind": { "path": "$r", "preserveNullAndEmptyArrays": True } },

            # C. Joindre les Genres -> Tableau de strings
            { "$lookup": {
                "from": "genres",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "g"
            }},
            
            # D. Joindre les Titres Alternatifs -> Tableau d'objets
            { "$lookup": {
                "from": "titles",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "titles_raw"
            }},

            # E. Joindre le Casting/Staff (Principals) + Noms (Persons)
            # C'est une jointure complexe (Lookup dans Lookup)
            { "$lookup": {
                "from": "principals",
                "let": { "mid": "$movie_id" },
                "pipeline": [
                    { "$match": { "$expr": { "$eq": ["$movie_id", "$$mid"] } } },
                    # Pour chaque membre du staff, on va chercher son nom
                    { "$lookup": {
                        "from": "persons",
                        "localField": "person_id",
                        "foreignField": "person_id",
                        "as": "p"
                    }},
                    { "$addFields": { 
                        "name": { "$arrayElemAt": ["$p.primary_name", 0] } 
                    }},
                    { "$project": { "_id": 0, "p": 0, "movie_id": 0 } } # Nettoyage
                ],
                "as": "crew"
            }},

            # F. PROJECTION FINALE (Structure du document)
            { "$project": {
                "_id": "$movie_id", # L'ID du film devient la clé primaire Mongo
                "title": "$primary_title",
                "original_title": "$original_title",
                "year": "$start_year",
                "runtime": "$runtime_minutes",
                "is_adult": "$is_adult",
                
                # Transformation des genres [{genre: "A"}, {genre: "B"}] -> ["A", "B"]
                "genres": "$g.genre",
                
                # Objet Rating imbriqué
                "rating": { 
                    "average": "$r.average_rating", 
                    "votes": "$r.num_votes" 
                },
                
                # Nettoyage des titres
                "titles": {
                    "$map": {
                        "input": "$titles_raw",
                        "as": "t",
                        "in": { "title": "$$t.title", "region": "$$t.region", "lang": "$$t.language" }
                    }
                },

                # Séparation du Crew en 3 tableaux distincts
                "directors": {
                    "$filter": { 
                        "input": "$crew", 
                        "as": "c", 
                        "cond": { "$in": ["$$c.category", ["director"]] } 
                    }
                },
                "writers": {
                    "$filter": { 
                        "input": "$crew", 
                        "as": "c", 
                        "cond": { "$in": ["$$c.category", ["writer"]] } 
                    }
                },
                "cast": {
                    "$filter": { 
                        "input": "$crew", 
                        "as": "c", 
                        "cond": { "$in": ["$$c.category", ["actor", "actress"]] } 
                    }
                }
            }}
        ]

        # Exécution du pipeline
        docs = list(db[SOURCE_COLLECTION].aggregate(pipeline))
        
        # Insertion dans la nouvelle collection
        if docs:
            db[TARGET_COLLECTION].insert_many(docs)
        
        processed += len(docs)
        print(f"   ... {processed}/{total_movies} films migrés ({processed/total_movies:.1%})", end='\r')

    # 4. Indexation finale
    print("\n🔨 Création des index sur 'movies_complete'...")
    db[TARGET_COLLECTION].create_index("year")
    db[TARGET_COLLECTION].create_index("genres")
    db[TARGET_COLLECTION].create_index("rating.votes")
    db[TARGET_COLLECTION].create_index("rating.average")
    # Index multikey pour chercher dans les sous-documents
    db[TARGET_COLLECTION].create_index("cast.person_id") 
    db[TARGET_COLLECTION].create_index("cast.name")

    duration = time.time() - start_time
    print(f"\n🎉 SUCCÈS ! Collection '{TARGET_COLLECTION}' prête.")
    print(f"⏱️ Temps total : {duration:.2f}s")

if __name__ == "__main__":
    migrate_structured()