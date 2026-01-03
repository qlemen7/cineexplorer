import pymongo
from pymongo import MongoClient
import time
import sys

MONGO_DB_NAME = 'imdb_project'

def get_db():
    client = MongoClient('mongodb://localhost:27017/')
    return client[MONGO_DB_NAME]

def ensure_indexes(db):
    """Crée les index avec les BONS noms de champs (snake_case)"""
    print("🔨 Création des index MongoDB...", end=' ', flush=True)
    # Tables principales
    db.movies.create_index("movie_id")
    db.movies.create_index("start_year")
    db.persons.create_index("person_id")
    db.persons.create_index("primary_name")
    
    # Tables de liaison
    db.principals.create_index("movie_id")
    db.principals.create_index("person_id")
    db.ratings.create_index("movie_id")
    db.ratings.create_index("num_votes")
    db.ratings.create_index("average_rating")
    db.genres.create_index("movie_id")
    db.genres.create_index("genre")
    db.directors.create_index("movie_id")
    db.directors.create_index("person_id")
    db.titles.create_index("movie_id")
    print("✅ Index créés.")

# --- REQUÊTES CORRIGÉES (snake_case) ---

def q1_filmography(db, actor_name):
    """Filmographie (Champs: primary_name, person_id, movie_id)"""
    pipeline = [
        {"$match": {"primary_name": {"$regex": actor_name, "$options": "i"}}}, 
        {"$lookup": {
            "from": "principals",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        {"$lookup": {
            "from": "movies",
            "localField": "roles.movie_id",
            "foreignField": "movie_id",
            "as": "movie_info"
        }},
        {"$unwind": "$movie_info"},
        {"$lookup": {
            "from": "ratings",
            "localField": "roles.movie_id",
            "foreignField": "movie_id",
            "as": "rating_info"
        }},
        {"$project": {
            "title": "$movie_info.primary_title",
            "year": "$movie_info.start_year",
            "role": "$roles.job", # job ou category
            "rating": {"$arrayElemAt": ["$rating_info.average_rating", 0]}
        }},
        {"$sort": {"year": -1}}
    ]
    return list(db.persons.aggregate(pipeline))

def q2_top_movies(db, genre, year_min, year_max, limit=5):
    """Top N films (Champs: genre, start_year, average_rating)"""
    pipeline = [
        {"$match": {"genre": genre}},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$match": {"m.start_year": {"$gte": year_min, "$lte": year_max}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {"r.num_votes": {"$gt": 1000}}}, 
        {"$sort": {"r.average_rating": -1}},
        {"$limit": limit},
        {"$project": {
            "title": "$m.primary_title",
            "year": "$m.start_year",
            "rating": "$r.average_rating"
        }}
    ]
    return list(db.genres.aggregate(pipeline))

def q3_multi_roles(db):
    """Acteurs multi-rôles"""
    pipeline = [
        # Group sur movie_id et person_id (pas mid/pid)
        {"$group": {
            "_id": {"movie_id": "$movie_id", "person_id": "$person_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$limit": 10},
        {"$lookup": {
            "from": "persons",
            "localField": "_id.person_id",
            "foreignField": "person_id",
            "as": "p"
        }},
        {"$project": {
            "name": {"$arrayElemAt": ["$p.primary_name", 0]},
            "nb_roles": "$count"
        }}
    ]
    return list(db.characters.aggregate(pipeline))

def q4_collaborations(db, actor_name):
    """Collaborations (Champs: person_id, movie_id)"""
    actor = db.persons.find_one({"primary_name": {"$regex": actor_name, "$options": "i"}})
    if not actor: return []
    actor_pid = actor['person_id']

    pipeline = [
        {"$match": {"person_id": actor_pid}},
        {"$lookup": {
            "from": "directors",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "directors"
        }},
        {"$unwind": "$directors"},
        {"$group": {
            "_id": "$directors.person_id",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": "person_id",
            "as": "d_info"
        }},
        {"$project": {
            "director": {"$arrayElemAt": ["$d_info.primary_name", 0]},
            "collaborations": "$count"
        }}
    ]
    return list(db.principals.aggregate(pipeline))

def q5_popular_genres(db):
    """Genres populaires"""
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$group": {
            "_id": "$genre",
            "avg_rating": {"$avg": "$r.average_rating"},
            "count": {"$sum": 1}
        }},
        {"$match": {
            "avg_rating": {"$gt": 7.0},
            "count": {"$gt": 50}
        }},
        {"$sort": {"avg_rating": -1}},
        {"$limit": 10}
    ]
    return list(db.genres.aggregate(pipeline))

def q6_career_stats(db, actor_name):
    """Carrière par décennie"""
    pipeline = [
        {"$match": {"primary_name": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {
            "from": "principals",
            "localField": "person_id",
            "foreignField": "person_id",
            "as": "roles"
        }},
        {"$unwind": "$roles"},
        {"$lookup": {
            "from": "movies",
            "localField": "roles.movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$match": {"m.start_year": {"$ne": None}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "m.movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "decade": {
                "$multiply": [
                    {"$floor": {"$divide": ["$m.start_year", 10]}},
                    10
                ]
            },
            "rating": "$r.average_rating"
        }},
        {"$group": {
            "_id": "$decade",
            "count": {"$sum": 1},
            "avg_rating": {"$avg": "$rating"}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db.persons.aggregate(pipeline))

def q7_rank_genre(db):
    """Classement par genre"""
    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {"r.num_votes": {"$gt": 5000}}},
        {"$setWindowFields": {
            "partitionBy": "$genre",
            "sortBy": {"r.average_rating": -1},
            "output": {
                "rank": {"$rank": {}}
            }
        }},
        {"$match": {"rank": {"$lte": 3}}},
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "m"
        }},
        {"$project": {
            "genre": "$genre", 
            "title": {"$arrayElemAt": ["$m.primary_title", 0]},
            "rating": "$r.average_rating",
            "rank": 1
        }}
    ]
    try:
        return list(db.genres.aggregate(pipeline))
    except:
        return ["Erreur: Votre version MongoDB ne supporte pas $setWindowFields"]

def q8_breakout(db):
    """Carrière propulsée (>200k votes)"""
    pipeline = [
        {"$match": {"num_votes": {"$gt": 200000}}},
        {"$lookup": {
            "from": "principals",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$match": {"p.category": {"$in": ["actor", "actress"]}}},
        {"$group": {
            "_id": "$p.person_id",
            "first_hit_votes": {"$max": "$num_votes"}
        }},
        {"$limit": 10},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": "person_id",
            "as": "person"
        }},
        {"$project": {
            "name": {"$arrayElemAt": ["$person.primary_name", 0]},
            "votes": "$first_hit_votes"
        }}
    ]
    return list(db.ratings.aggregate(pipeline))

def q9_complex(db):
    """Requête libre (runtime_minutes, average_rating)"""
    pipeline = [
        {"$match": {"runtime_minutes": {"$gt": 120}}},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$match": {"r.average_rating": {"$gt": 8.0}}},
        {"$lookup": {
            "from": "titles",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "t"
        }},
        {"$unwind": "$t"},
        {"$match": {"t.language": "fr"}},
        {"$limit": 20},
        {"$project": {"primary_title": 1, "rating": "$r.average_rating"}}
    ]
    return list(db.movies.aggregate(pipeline))

def run_tests():
    db = get_db()
    ensure_indexes(db)
    
    tests = [
        ("Q1 Filmographie (Tom Hanks)", lambda: q1_filmography(db, "Tom Hanks")),
        ("Q2 Top Drama 1990-2000", lambda: q2_top_movies(db, "Drama", 1990, 2000)),
        ("Q3 Multi-rôles", lambda: q3_multi_roles(db)),
        ("Q4 Collaborations (DiCaprio)", lambda: q4_collaborations(db, "Leonardo DiCaprio")),
        ("Q5 Genres Pop", lambda: q5_popular_genres(db)),
        ("Q6 Carrière (Brad Pitt)", lambda: q6_career_stats(db, "Brad Pitt")),
        ("Q7 Classement Genre", lambda: q7_rank_genre(db)),
        ("Q8 Propulsés", lambda: q8_breakout(db)),
        ("Q9 Complexe", lambda: q9_complex(db)),
    ]

    print("\n--- BENCHMARK MONGODB (Collections Plates) ---")
    print(f"{'Requête':<30} | {'Temps (ms)':<10} | {'Résultat (ex)'}")
    print("-" * 75)

    for name, func in tests:
        start = time.time()
        try:
            res = func()
            duration = (time.time() - start) * 1000
            sample = str(res[0])[:60] + "..." if res else "Aucun résultat"
            print(f"{name:<30} | {duration:<10.2f} | {sample}")
        except Exception as e:
            print(f"{name:<30} | ERREUR     | {str(e)[:50]}")

if __name__ == "__main__":
    run_tests()