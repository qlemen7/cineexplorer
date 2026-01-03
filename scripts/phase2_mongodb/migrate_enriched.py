import sqlite3
import pymongo
import os
import sys
import time

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQLITE_DB = os.path.join(BASE_DIR, 'data', 'cineexplorer.db')
MONGO_URI = 'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0'
DB_NAME = 'cineexplorer'

def get_column_name(cursor, table, possible_names):
    """Cherche quel nom de colonne est utilisé parmi une liste de possibilités."""
    try:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        for name in possible_names:
            if name in columns: return name
    except: pass
    return None

def detect_akas_config(cursor):
    """
    Détective privé : Trouve le nom de la table AKAS et ses colonnes.
    Retourne un dictionnaire de configuration ou None.
    """
    # 1. Chercher le nom de la table
    table_name = None
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    for candidate in ['akas', 'title_akas', 'movie_akas']:
        if candidate in tables:
            table_name = candidate
            break
    
    if not table_name:
        return None

    # 2. Chercher les colonnes clés
    col_fk = get_column_name(cursor, table_name, ['movie_id', 'titleId', 'tconst'])
    col_title = get_column_name(cursor, table_name, ['title', 'titleName', 'primary_title'])
    col_region = get_column_name(cursor, table_name, ['region', 'regionName', 'area'])

    if not col_fk or not col_title:
        print(f"⚠️  Table '{table_name}' trouvée mais colonnes introuvables.")
        return None

    print(f"🕵️‍♂️ Configuration AKAS détectée : Table='{table_name}', FK='{col_fk}', Title='{col_title}', Region='{col_region}'")
    
    return {
        'table': table_name,
        'fk': col_fk,
        'title': col_title,
        'region': col_region
    }

def migrate():
    print(f"🚀 Démarrage de la MIGRATION ULTIME (Détection automatique)...")
    
    # Connexions
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        db = client[DB_NAME]
        collection = db['movies']
    except:
        print("❌ Erreur Mongo.")
        return

    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Détection des noms de colonnes principaux
    mid_col = get_column_name(cursor, 'movies', ['movie_id', 'tconst']) or 'movie_id'
    pid_col = get_column_name(cursor, 'persons', ['person_id', 'nconst']) or 'person_id'
    name_col = get_column_name(cursor, 'persons', ['primary_name', 'primaryName', 'name']) or 'primary_name'
    
    # Détection AKAS
    akas_conf = detect_akas_config(cursor)

    # Lecture films
    print(f"📥 Lecture du catalogue...")
    query_movies = f"""
        SELECT 
            m.{mid_col} as _id, 
            m.primary_title as title, 
            m.start_year as year,
            m.runtime_minutes as runtime,
            m.is_adult,
            r.average_rating,
            r.num_votes
        FROM movies m
        LEFT JOIN ratings r ON m.{mid_col} = r.{mid_col}
        WHERE m.title_type = 'movie'
    """
    cursor.execute(query_movies)
    movies_rows = cursor.fetchall()
    total = len(movies_rows)
    
    # Nettoyage
    collection.delete_many({}) 
    batch = []
    start_time = time.time()

    for i, row in enumerate(movies_rows):
        movie_data = dict(row)
        movie_id = movie_data['_id']
        votes = movie_data['num_votes'] if movie_data['num_votes'] else 0
        
        cast = []
        directors = []
        writers = []
        akas = []

        if votes > 0: 
            # Casting (Simplifié pour la démo)
            try:
                q = f"SELECT p.{name_col} as name FROM principals pr JOIN persons p ON pr.{pid_col}=p.{pid_col} WHERE pr.{mid_col}=? AND pr.category IN ('actor','actress') LIMIT 6"
                cast = [dict(r) for r in conn.execute(q, (movie_id,)).fetchall()]
            except: pass

            # Réalisateurs
            try:
                q = f"SELECT p.{name_col} as name FROM directors d JOIN persons p ON d.{pid_col}=p.{pid_col} WHERE d.{mid_col}=?"
                directors = [dict(r) for r in conn.execute(q, (movie_id,)).fetchall()]
            except: pass

            # Scénaristes
            try:
                q = f"SELECT p.{name_col} as name FROM writers w JOIN persons p ON w.{pid_col}=p.{pid_col} WHERE w.{mid_col}=?"
                writers = [dict(r) for r in conn.execute(q, (movie_id,)).fetchall()]
            except: pass
            
            # Titres Alternatifs (Si config trouvée)
            if akas_conf:
                try:
                    # On construit la requête dynamiquement selon les noms détectés
                    # On filtre 'region IS NOT NULL' seulement si la colonne région existe
                    region_clause = f"AND {akas_conf['region']} IS NOT NULL" if akas_conf['region'] else ""
                    col_region_sel = akas_conf['region'] if akas_conf['region'] else "'' as region"
                    
                    q_akas = f"""
                        SELECT {akas_conf['title']} as title, {col_region_sel} as region
                        FROM {akas_conf['table']}
                        WHERE {akas_conf['fk']} = ? {region_clause}
                        LIMIT 5
                    """
                    akas = [dict(r) for r in conn.execute(q_akas, (movie_id,)).fetchall()]
                except Exception as e: 
                    # print(e) # Décommenter pour debug
                    pass

        # Genres
        try:
            q_genres = f"SELECT genre FROM genres WHERE {mid_col} = ?"
            genres = [r['genre'] for r in conn.execute(q_genres, (movie_id,)).fetchall()]
        except: genres = []

        doc = {
            "_id": movie_id,
            "title": movie_data['title'],
            "year": movie_data['year'],
            "rating": {"average": movie_data['average_rating'], "votes": votes},
            "genres": genres,
            "cast": cast,
            "directors": directors,
            "writers": writers,
            "titles": akas, # Stockage des AKAS
            "is_adult": bool(movie_data['is_adult']),
            "runtime": movie_data['runtime']
        }
        
        batch.append(doc)

        if len(batch) >= 500:
            collection.insert_many(batch)
            batch = []
            elapsed = time.time() - start_time
            percent = ((i+1) / total) * 100
            print(f"⏳ {percent:.1f}% ({i+1}/{total})", end='\r')

    if batch:
        collection.insert_many(batch)

    print(f"\n\n🎉 MIGRATION TERMINÉE.")
    collection.create_index("genres")
    conn.close()
    client.close()

if __name__ == "__main__":
    migrate()