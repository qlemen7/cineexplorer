from pymongo import MongoClient
from django.conf import settings

# Connexion au Replica Set
MONGO_URI = 'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0'
DB_NAME = 'cineexplorer'

def get_mongo_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return client[DB_NAME]

def get_movie_details(movie_id):
    """
    Récupère le document complet d'un film depuis MongoDB.
    """
    db = get_mongo_db()
    collection = db['movies']
    
    try:
        movie = collection.find_one({"_id": movie_id})
        if movie:
            # FIX DJANGO : On crée un alias 'id' sans underscore pour le template
            movie['id'] = movie['_id']
        return movie
    except Exception as e:
        print(f"🚨 Erreur Mongo Détail: {e}")
        return None

def get_similar_movies(genres, exclude_id, limit=6):
    """
    Trouve des films du même genre via MongoDB.
    """
    if not genres: return []
    
    db = get_mongo_db()
    try:
        query = {
            "genres": {"$in": genres},
            "_id": {"$ne": exclude_id}
        }
        projection = {"title": 1, "year": 1, "rating": 1, "poster": 1}
        
        cursor = db['movies'].find(query, projection).sort("rating.average", -1).limit(limit)
        
        # Transformation en liste pour pouvoir modifier les dictionnaires
        movies = list(cursor)
        for m in movies:
            # FIX DJANGO : On crée l'alias ici aussi
            m['id'] = m['_id']
            
        return movies
    except Exception as e:
        print(f"🚨 Erreur Mongo Similaires: {e}")
        return []

def get_mongo_stats():
    """Stats simples pour la page /stats"""
    db = get_mongo_db()
    return {
        "count": db['movies'].count_documents({}),
        "avg_rating": 0 # À implémenter avec un aggregate si besoin
    }