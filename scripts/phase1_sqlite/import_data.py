import sqlite3
import csv
import os
import time

# Chemins
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')
CSV_DIR = os.path.join(os.path.dirname(__file__), '../../data/csv/')

# Configuration des fichiers
FILES_CONFIG = [
    ('movies.csv', 'movies', ['mid', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']),
    ('persons.csv', 'persons', ['pid', 'primaryName', 'birthYear', 'deathYear']),
    ('ratings.csv', 'ratings', ['mid', 'averageRating', 'numVotes']),
    ('genres.csv', 'genres', ['mid', 'genre']),
    ('principals.csv', 'principals', ['mid', 'ordering', 'pid', 'category', 'job']), # CSV: mid, ordering, pid
    ('directors.csv', 'directors', ['mid', 'pid']),
    ('writers.csv', 'writers', ['mid', 'pid']),
    ('titles.csv', 'titles', ['mid', 'ordering', 'title', 'region', 'language', 'types', 'attributes', 'isOriginalTitle']),
    ('characters.csv', 'characters', ['mid', 'pid', 'name']), 
    ('professions.csv', 'professions', ['pid', 'jobName']),
    ('knownformovies.csv', 'known_for', ['pid', 'mid']),
]

def clean_val(val):
    if val in [r'\N', '', 'NaN']: return None
    return val

def import_data():
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    
    # On recrée le schéma vide
    from create_schema import create_schema
    create_schema()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA foreign_keys = OFF") 

    print(f"\n--- DÉBUT DE L'IMPORT (Corrigé Le C) ---")
    total_start = time.time()

    for filename, table, cols in FILES_CONFIG:
        path = os.path.join(CSV_DIR, filename)
        if not os.path.exists(path):
            print(f"⚠️  Fichier {filename} manquant.")
            continue

        print(f"Importation de {table}...", end=' ', flush=True)
        start_t = time.time()
        
        with open(path, 'r', encoding='utf-8') as f:
            # Détection auto du séparateur sale ou propre
            first_line = f.readline()
            delimiter = ',' if "('" in first_line else '\t'
            f.seek(0)
            
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None) # Skip header

            # --- LOGIQUE DE MAPPING EXPLICITE ---
            # C'est ici qu'on répare le bug de PRINCIPALS
            if table == 'principals':
                # CSV : mid, ordering, pid, category, job
                # SQL : movie_id, person_id, ordering, category, job
                # On croise ordering et person_id dans l'ordre SQL
                insert_sql = "INSERT INTO principals (movie_id, ordering, person_id, category, job) VALUES (?, ?, ?, ?, ?)"
            
            elif table == 'characters':
                insert_sql = "INSERT INTO characters (movie_id, person_id, character_name) VALUES (?, ?, ?)"
            
            elif table == 'titles':
                insert_sql = "INSERT INTO titles (movie_id, ordering, title, region, language, types, attributes, is_original_title) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            
            else:
                # Cas standard (l'ordre CSV correspond à l'ordre de création de la table)
                placeholders = ','.join(['?'] * len(cols))
                insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"

            batch = []
            for row in reader:
                if len(row) < len(cols): continue
                batch.append([clean_val(v) for v in row[:len(cols)]])
                
                if len(batch) >= 50000:
                    conn.executemany(insert_sql, batch)
                    batch = []
            
            if batch: conn.executemany(insert_sql, batch)

        print(f"✅ ({time.time() - start_t:.2f}s)")

    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()
    conn.close()
    print(f"\n🎉 Import terminé. Tom Hanks devrait être là.")

if __name__ == "__main__":
    import_data()