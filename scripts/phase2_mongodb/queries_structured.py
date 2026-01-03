import pymongo
from pymongo import MongoClient
import time

MONGO_DB_NAME = 'imdb_project'
COLLECTION = 'movies_complete'

def get_db():
    client = MongoClient('mongodb://localhost:27017/')
    return client[MONGO_DB_NAME]

# --- REQUÊTES OPTIMISÉES (NoSQL style) ---

def q1_filmography_struct(db, actor_name):
    """Q1: Filmographie (Plus besoin de jointures !)"""
    # On cherche simplement dans le tableau 'cast'
    pipeline = [
        {"$match": {
            "cast.name": {"$regex": actor_name, "$options": "i"}
        }},
        {"$project": {
            "title": 1, 
            "year": 1, 
            "rating": "$rating.average",
            # On extrait juste le rôle concerné du tableau
            "role": {
                "$filter": {
                    "input": "$cast",
                    "as": "c",
                    "cond": {"$regexMatch": {"input": "$$c.name", "regex": actor_name, "options": "i"}}
                }
            }
        }},
        {"$sort": {"year": -1}}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q2_top_movies_struct(db, genre, year_min, year_max, limit=5):
    """Q2: Top N films (Tout est dans le document)"""
    # Plus aucun lookup, c'est une simple recherche
    query = {
        "genres": genre,
        "year": {"$gte": year_min, "$lte": year_max},
        "rating.votes": {"$gt": 1000}
    }
    projection = {"title": 1, "year": 1, "rating": 1, "_id": 0}
    
    return list(db[COLLECTION].find(query, projection)
                              .sort("rating.average", -1)
                              .limit(limit))

def q3_multi_roles_struct(db):
    """Q3: Acteurs multi-rôles"""
    # Difficile en document pur sans unwind, mais on le fait pour la démo
    pipeline = [
        {"$unwind": "$cast"},
        # On compte les doublons (film_id + person_id)
        {"$group": {
            "_id": {"mid": "$_id", "pid": "$cast.person_id", "name": "$cast.name"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q4_collaborations_struct(db, actor_name):
    """Q4: Collaborations (Toujours une agrégation nécessaire)"""
    pipeline = [
        # 1. Trouver les films de l'acteur
        {"$match": {"cast.name": {"$regex": actor_name, "$options": "i"}}},
        # 2. "Déplier" les réalisateurs
        {"$unwind": "$directors"},
        # 3. Grouper
        {"$group": {
            "_id": "$directors.name",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q5_popular_genres_struct(db):
    """Q5: Genres populaires"""
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres",
            "avg_rating": {"$avg": "$rating.average"},
            "count": {"$sum": 1}
        }},
        {"$match": {
            "avg_rating": {"$gt": 7.0},
            "count": {"$gt": 50}
        }},
        {"$sort": {"avg_rating": -1}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q6_career_struct(db, actor_name):
    """Q6: Carrière par décennie"""
    pipeline = [
        {"$match": {
            "cast.name": {"$regex": actor_name, "$options": "i"},
            "year": {"$ne": None}
        }},
        {"$project": {
            "decade": {
                "$multiply": [
                    {"$floor": {"$divide": ["$year", 10]}},
                    10
                ]
            },
            "rating": "$rating.average"
        }},
        {"$group": {
            "_id": "$decade",
            "count": {"$sum": 1},
            "avg_rating": {"$avg": "$rating"}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q7_rank_genre_struct(db):
    """Q7: Classement par genre"""
    # BEAUCOUP plus rapide car les notes sont déjà dans le document
    pipeline = [
        {"$match": {"rating.votes": {"$gt": 5000}}},
        {"$unwind": "$genres"},
        {"$setWindowFields": {
            "partitionBy": "$genres",
            "sortBy": {"rating.average": -1},
            "output": {
                "rank": {"$rank": {}}
            }
        }},
        {"$match": {"rank": {"$lte": 3}}},
        {"$project": {
            "genre": "$genres", "title": 1, "rating": "$rating.average", "rank": 1
        }}
    ]
    try:
        return list(db[COLLECTION].aggregate(pipeline))
    except:
        return ["Erreur version Mongo < 5.0"]

def q8_breakout_struct(db):
    """Q8: Breakout role"""
    pipeline = [
        {"$match": {"rating.votes": {"$gt": 200000}}},
        {"$unwind": "$cast"},
        {"$sort": {"year": 1}}, # Important : tri chronologique
        {"$group": {
            "_id": "$cast.person_id",
            "first_hit": {"$first": "$title"},
            "votes": {"$first": "$rating.votes"},
            "year": {"$first": "$year"},
            "name": {"$first": "$cast.name"}
        }},
        {"$sort": {"votes": -1}},
        {"$limit": 10}
    ]
    return list(db[COLLECTION].aggregate(pipeline))

def q9_complex_struct(db):
    """Q9: Complexe"""
    # Simple filtrage sur champs imbriqués et tableaux
    query = {
        "runtime": {"$gt": 120},
        "rating.average": {"$gt": 8.0},
        "titles": {
            "$elemMatch": {"lang": "fr"}
        }
    }
    return list(db[COLLECTION].find(query, {"title": 1, "rating": 1}).limit(20))

def run_benchmark():
    db = get_db()
    
    tests = [
        ("Q1 Filmographie", lambda: q1_filmography_struct(db, "Tom Hanks")),
        ("Q2 Top Films", lambda: q2_top_movies_struct(db, "Drama", 1990, 2000)),
        ("Q3 Multi-rôles", lambda: q3_multi_roles_struct(db)),
        ("Q4 Collaborations", lambda: q4_collaborations_struct(db, "Leonardo DiCaprio")),
        ("Q5 Genres Pop", lambda: q5_popular_genres_struct(db)),
        ("Q6 Carrière", lambda: q6_career_struct(db, "Brad Pitt")),
        ("Q7 Classement", lambda: q7_rank_genre_struct(db)),
        ("Q8 Propulsés", lambda: q8_breakout_struct(db)),
        ("Q9 Complexe", lambda: q9_complex_struct(db)),
    ]

    print("\n--- BENCHMARK MONGODB (Documents Structurés) ---")
    print(f"{'Requête':<20} | {'Temps (ms)':<10} | {'Résultat (ex)'}")
    print("-" * 65)

    for name, func in tests:
        start = time.time()
        try:
            res = func()
            duration = (time.time() - start) * 1000
            sample = str(res[0])[:40] + "..." if res else "Aucun résultat"
            print(f"{name:<20} | {duration:<10.2f} | {sample}")
        except Exception as e:
            print(f"{name:<20} | ERREUR     | {str(e)[:40]}")

if __name__ == "__main__":
    run_benchmark()