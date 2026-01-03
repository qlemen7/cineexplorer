import sqlite3
import os
import unicodedata
import re
from django.conf import settings

# --- 2. FONCTION POUR ENLEVER LES ACCENTS ---
def strip_accents(text):
    """
    Nettoyage ULTIME pour le tri :
    1. Enlève les accents (É -> e).
    2. Enlève tout ce qui n'est pas une lettre ou un chiffre (#, $, -, espace).
    3. Met en minuscule.
    
    Exemple : '#Alive' -> 'alive' (Trie à A)
    Exemple : 'The $5.00 Movie' -> 'the500movie'
    """
    if not text:
        return ""
    try:
        # 1. On sépare les accents
        text = unicodedata.normalize('NFD', text)
        # 2. On garde les caractères de base
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        # 3. On passe en minuscule
        text = text.lower()
        # 4. MAGIE : On vire tout ce qui n'est PAS (^) une lettre (a-z) ou un chiffre (0-9)
        text = re.sub(r'[^a-z0-9]', '', text)
        return text
    except:
        return text.lower()

def get_db_connection():
    db_path = os.path.join(settings.BASE_DIR, 'data', 'cineexplorer.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # --- 3. ON INJECTE LA FONCTION DANS SQLITE ---
    # Maintenant, on pourra utiliser 'strip_accents(colonne)' dans nos requêtes SQL !
    conn.create_function("strip_accents", 1, strip_accents)
    
    return conn

def get_sqlite_stats():
    """Récupère les statistiques détaillées (Films, Acteurs, Réalisateurs)."""
    conn = get_db_connection()
    stats = {"source": "SQLite", "status": "OK"}
    try:
        # 1. Compter les Films
        cursor = conn.execute("SELECT count(*) FROM movies WHERE title_type='movie'")
        stats['count_movies'] = cursor.fetchone()[0]
        
        # 2. Compter les Acteurs (Uniques)
        # On regarde dans 'principals' ceux qui ont la catégorie 'actor' ou 'actress'
        cursor = conn.execute("""
            SELECT count(DISTINCT person_id) 
            FROM principals 
            WHERE category IN ('actor', 'actress')
        """)
        stats['count_actors'] = cursor.fetchone()[0]

        # 3. Compter les Réalisateurs (Uniques)
        # On regarde directement dans la table 'directors'
        cursor = conn.execute("SELECT count(DISTINCT person_id) FROM directors")
        stats['count_directors'] = cursor.fetchone()[0]

    except Exception as e:
        stats['status'] = f"Erreur: {e}"
        stats['count_movies'] = 0
        stats['count_actors'] = 0
        stats['count_directors'] = 0
    finally:
        conn.close()
    return stats

def get_top_movies(limit=12):
    conn = get_db_connection()
    movies = []
    try:
        # On joint movies et ratings
        # On renomme les colonnes pour matcher ce que le Template attend (primaryTitle, etc.)
        query = """
            SELECT 
                m.movie_id as tconst,
                m.primary_title as primaryTitle,
                m.start_year as startYear,
                r.average_rating as averageRating,
                '...' as genres
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            WHERE m.title_type = 'movie'
            ORDER BY r.average_rating DESC
            LIMIT ?
        """
        movies = [dict(row) for row in conn.execute(query, (limit,)).fetchall()]
    except Exception as e:
        print(f"🚨 Erreur Top Movies: {e}")
    finally:
        conn.close()
    return movies

def get_movies_list(page=1, per_page=20, filters=None):
    conn = get_db_connection()
    offset = (page - 1) * per_page
    movies = []
    has_next = False
    
    # Requête de base
    query = """
        SELECT 
            m.movie_id as tconst,
            m.primary_title as primaryTitle,
            m.start_year as startYear,
            r.average_rating as averageRating,
            m.title_type,
            (SELECT GROUP_CONCAT(genre, ', ') FROM genres WHERE movie_id = m.movie_id) as genres
        FROM movies m
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        WHERE m.title_type = 'movie'
    """
    params = []

    # --- Gestion des Filtres ---
    if filters:
        # RECHERCHE INTELLIGENTE (Titre OU Acteur)
        if filters.get('q'):
            search_term = f"%{filters['q']}%"
            query += """
                AND (
                    m.primary_title LIKE ? 
                    OR m.movie_id IN (
                        SELECT pr.movie_id 
                        FROM principals pr 
                        JOIN persons p ON pr.person_id = p.person_id 
                        WHERE p.primary_name LIKE ?
                    )
                )
            """
            # On passe le terme de recherche 2 fois (une pour le titre, une pour l'acteur)
            params.append(search_term)
            params.append(search_term)
        
        if filters.get('year'):
            query += " AND m.start_year >= ?"
            params.append(filters['year'])
        
        if filters.get('rating'):
            query += " AND r.average_rating >= ?"
            params.append(filters['rating'])

        if filters.get('genre'):
            query += " AND m.movie_id IN (SELECT movie_id FROM genres WHERE genre = ?)"
            params.append(filters['genre'])

    # --- Tri (Mise à jour pour ASC/DESC) ---
    sort = filters.get('sort', 'year_desc') if filters else 'year_desc'
    
    if sort == 'rating_desc':
        query += " ORDER BY r.average_rating DESC"
    elif sort == 'rating_asc':
        query += " ORDER BY r.average_rating ASC"
        
    elif sort == 'title_asc':
        query += " ORDER BY strip_accents(m.primary_title) ASC"
    elif sort == 'title_desc':
        query += " ORDER BY strip_accents(m.primary_title) DESC"
        
    elif sort == 'year_asc':
        query += " ORDER BY m.start_year ASC"
    else:
        # Par défaut : Année décroissante (Plus récents d'abord)
        query += " ORDER BY m.start_year DESC"

    # --- Pagination ---
    query += " LIMIT ? OFFSET ?"
    params.extend([per_page, offset])

    try:
        cursor = conn.execute(query, params)
        movies = [dict(row) for row in cursor.fetchall()]
        has_next = len(movies) == per_page
        
    except Exception as e:
        print(f"🚨 ERREUR LISTE: {e}")
    finally:
        conn.close()
    
    return movies, has_next

def get_all_genres():
    """Récupère les genres distincts depuis la table genres."""
    conn = get_db_connection()
    genres = []
    try:
        cursor = conn.execute("SELECT DISTINCT genre FROM genres ORDER BY genre")
        genres = [row[0] for row in cursor.fetchall()]
    except:
        # Fallback si la table genres est vide ou erreur
        genres = ["Action", "Drama", "Comedy", "Thriller", "Romance"]
    finally:
        conn.close()
    return genres

# ... (Garde tes fonctions existantes get_db_connection, get_movies_list, etc.) ...

def get_stats_for_charts():
    """Récupère les données agrégées pour les graphiques."""
    conn = get_db_connection()
    data = {'genres': {}, 'decades': {}, 'ratings': {}}
    
    try:
        # 1. Films par Genre (Top 10)
        # Attention : comme les genres sont stockés dans une table 'genres' ou CSV, on adapte
        cursor = conn.execute("""
            SELECT genre, COUNT(*) as count 
            FROM genres 
            GROUP BY genre 
            ORDER BY count DESC 
            LIMIT 10
        """)
        data['genres'] = {row['genre']: row['count'] for row in cursor.fetchall()}

        # 2. Films par Décennie
        # On divise l'année par 10, on arrondit, puis on multiplie par 10 (ex: 1994 -> 1990)
        cursor = conn.execute("""
            SELECT (CAST(start_year AS INTEGER) / 10) * 10 as decade, COUNT(*) as count
            FROM movies 
            WHERE title_type='movie' AND start_year IS NOT NULL
            GROUP BY decade 
            ORDER BY decade ASC
        """)
        data['decades'] = {str(row['decade']): row['count'] for row in cursor.fetchall() if row['decade']}

        # 3. Distribution des notes (Arrondi à l'entier)
        cursor = conn.execute("""
            SELECT CAST(average_rating AS INTEGER) as note, COUNT(*) as count
            FROM ratings
            GROUP BY note
            ORDER BY note ASC
        """)
        data['ratings'] = {str(row['note']): row['count'] for row in cursor.fetchall() if row['note']}
        
        # --- NOUVEAU : 4. Top 10 Acteurs Prolifiques ---
        # On joint 'principals' et 'persons' pour avoir le nom
        cursor = conn.execute("""
            SELECT p.primary_name, COUNT(*) as count
            FROM principals pr
            JOIN persons p ON pr.person_id = p.person_id
            WHERE pr.category IN ('actor', 'actress')
            GROUP BY p.person_id
            ORDER BY count DESC
            LIMIT 10
        """)
        data['actors'] = {row['primary_name']: row['count'] for row in cursor.fetchall()}

    except Exception as e:
        print(f"🚨 Erreur Stats Charts: {e}")
    finally:
        conn.close()
    
    return data

def get_random_movies(limit=5):
    """Récupère des films au hasard (Version Robuste)."""
    conn = get_db_connection()
    movies = []
    try:
        # On enlève le filtre sur les votes pour être SÛR d'avoir des résultats
        query = """
            SELECT 
                m.movie_id as tconst,
                m.primary_title as primaryTitle,
                m.start_year as startYear,
                r.average_rating as averageRating
            FROM movies m
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            WHERE m.title_type = 'movie'
            ORDER BY RANDOM()
            LIMIT ?
        """
        movies = [dict(row) for row in conn.execute(query, (limit,)).fetchall()]
        
        # Petit message dans ton terminal pour vérifier
        print(f"🎲 DEBUG: {len(movies)} films aléatoires trouvés.")
        
    except Exception as e:
        print(f"🚨 Erreur Random Movies: {e}")
    finally:
        conn.close()
    return movies