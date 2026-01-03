import sqlite3
import time
import os
import sys

sys.path.append(os.path.dirname(__file__))
from queries import *

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def get_db_size():
    if os.path.exists(DB_PATH):
        return os.path.getsize(DB_PATH) / (1024 * 1024)
    return 0

def create_indexes(conn):
    print("   🔨 Création des index...", end=' ', flush=True)
    start = time.time()
    
    # --- Index pour Q1 (Recherche acteur) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(primary_name)")
    
    # --- Index pour Q2 (Genre, Année) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_genres_genre ON genres(genre)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(start_year)")
    
    # --- Index pour Q3 (Characters) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chars_mid ON characters(movie_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_chars_pid ON characters(person_id)")
    
    # --- Index pour Q4 (Collaborations / Directors) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_directors_mid ON directors(movie_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_directors_pid ON directors(person_id)")
    
    # --- Index pour Q5, Q7, Q8 (Ratings / Votes) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ratings_votes ON ratings(num_votes)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ratings_avg ON ratings(average_rating)")
    
    # --- Index pour Q9 (Titles / Language) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_titles_mid ON titles(movie_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_titles_lang ON titles(language)")

    # --- Index Clés Étrangères (Boost général des JOIN) ---
    conn.execute("CREATE INDEX IF NOT EXISTS idx_principals_mid ON principals(movie_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_principals_pid ON principals(person_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ratings_mid ON ratings(movie_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_genres_mid ON genres(movie_id)")

    conn.commit()
    print(f"Fait en {time.time()-start:.2f}s.")

def drop_indexes(conn):
    print("  Suppression des index...", end=' ', flush=True)
    # Liste exhaustive pour être sûr de bien comparer "Sans rien" vs "Optimisé"
    indexes = [
        'idx_persons_name', 'idx_genres_genre', 'idx_movies_year',
        'idx_chars_mid', 'idx_chars_pid',
        'idx_directors_mid', 'idx_directors_pid',
        'idx_ratings_votes', 'idx_ratings_avg',
        'idx_titles_mid', 'idx_titles_lang',
        'idx_principals_mid', 'idx_principals_pid', 'idx_ratings_mid', 'idx_genres_mid'
    ]
    for idx in indexes:
        conn.execute(f"DROP INDEX IF EXISTS {idx}")
    conn.commit()
    print("Fait.")

def run_benchmark():
    if not os.path.exists(DB_PATH):
        print("Erreur: DB introuvable.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # Liste complète des 9 requêtes avec des paramètres représentatifs
    tests = [
        ("Q1 Filmographie", lambda: query_actor_filmography(conn, "Brad Pitt")),
        ("Q2 Top Films", lambda: query_top_movies_by_genre(conn, "Drama", 1990, 2000, 5)),
        ("Q3 Multi-rôles", lambda: query_multi_role_actors(conn)),
        ("Q4 Collaborations", lambda: query_collaborations(conn, "Leonardo DiCaprio")),
        ("Q5 Genres Pop", lambda: query_popular_genres(conn)),
        ("Q6 Carrière", lambda: query_career_evolution(conn, "Tom Hanks")),
        ("Q7 Classement", lambda: query_rank_by_genre(conn)),
        ("Q8 Propulsés", lambda: query_breakout_role(conn)),
        ("Q9 Complexe", lambda: query_free_complex(conn))
    ]

    print(f"\n--- BENCHMARK PHASE 1 COMPLETE (DB: {get_db_size():.1f} MB) ---")

    # 1. Mesure Sans Index
    drop_indexes(conn)
    results = {}
    print("\nMesure SANS index (Patience...) :")
    for name, func in tests:
        start = time.time()
        func()
        duration = (time.time() - start) * 1000
        results[name] = {"no_index": duration}
        print(f"   - {name:<15} : {duration:.2f} ms")

    # 2. Mesure Avec Index
    print("\nMesure AVEC index :")
    create_indexes(conn)
    for name, func in tests:
        start = time.time()
        func()
        duration = (time.time() - start) * 1000
        results[name]["with_index"] = duration
        
        # Calcul du gain
        no_idx = results[name]["no_index"]
        if no_idx > 0:
            gain = ((no_idx - duration) / no_idx) * 100
        else:
            gain = 0
        results[name]["gain"] = gain
        print(f"   - {name:<15} : {duration:.2f} ms (Gain: {gain:.1f}%)")

    # 3. Tableau résumé
    print("\n\n--- TABLEAU POUR RAPPORT LATEX/MARKDOWN ---")
    print(f"{'Requête':<20} | {'Sans Index (ms)':<15} | {'Avec Index (ms)':<15} | {'Gain (%)':<10}")
    print("-" * 66)
    for name, res in results.items():
        print(f"{name:<20} | {res['no_index']:<15.1f} | {res['with_index']:<15.1f} | {res['gain']:<10.1f}")

    conn.close()

if __name__ == "__main__":
    run_benchmark()