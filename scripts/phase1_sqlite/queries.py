import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def get_conn():
    return sqlite3.connect(DB_PATH)

# Q1: Filmographie d'un acteur
def query_actor_filmography(conn, actor_name):
    """
    Retourne la filmographie d'un acteur.
    """
    sql = """
    SELECT m.primary_title, m.start_year, c.character_name, r.average_rating
    FROM movies m
    JOIN principals p ON m.movie_id = p.movie_id
    JOIN persons pe ON p.person_id = pe.person_id
    LEFT JOIN characters c ON m.movie_id = c.movie_id AND pe.person_id = c.person_id
    LEFT JOIN ratings r ON m.movie_id = r.movie_id
    WHERE pe.primary_name LIKE ? 
    ORDER BY m.start_year DESC
    """
    # Note les '%' ajoutés autour du nom pour le LIKE
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

# Q2: Top N films par genre
def query_top_movies_by_genre(conn, genre, start_year, end_year, n):
    """
    Les N meilleurs films d'un genre sur une période.
    """
    sql = """
    SELECT m.primary_title, m.start_year, r.average_rating, r.num_votes
    FROM movies m
    JOIN genres g ON m.movie_id = g.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE g.genre = ? 
      AND m.start_year BETWEEN ? AND ?
      AND r.num_votes > 1000 -- Filtre bruit
    ORDER BY r.average_rating DESC
    LIMIT ?
    """
    return conn.execute(sql, (genre, start_year, end_year, n)).fetchall()

# Q3: Acteurs multi-rôles
def query_multi_role_actors(conn):
    """
    Acteurs ayant joué plusieurs personnages dans un même film.
    """
    sql = """
    SELECT pe.primary_name, m.primary_title, COUNT(c.character_name) as nb_roles
    FROM characters c
    JOIN persons pe ON c.person_id = pe.person_id
    JOIN movies m ON c.movie_id = m.movie_id
    GROUP BY c.movie_id, c.person_id
    HAVING nb_roles > 1
    ORDER BY nb_roles DESC
    LIMIT 20
    """
    return conn.execute(sql).fetchall()

# Q4: Collaborations Réalisateur-Acteur
def query_collaborations(conn, actor_name):
    """
    Réalisateurs ayant travaillé avec un acteur spécifique.
    """
    sql = """
    SELECT d_pe.primary_name as director, COUNT(*) as collaborations
    FROM principals p_actor
    JOIN persons pe_actor ON p_actor.person_id = pe_actor.person_id
    JOIN directors d ON p_actor.movie_id = d.movie_id
    JOIN persons d_pe ON d.person_id = d_pe.person_id
    WHERE pe_actor.primary_name = ?
    GROUP BY d.person_id
    ORDER BY collaborations DESC
    """
    return conn.execute(sql, (actor_name,)).fetchall()

# Q5: Genres populaires
def query_popular_genres(conn):
    """
    Genres avec note > 7.0 et +50 films.
    """
    sql = """
    SELECT g.genre, AVG(r.average_rating) as avg_rat, COUNT(*) as nb_films
    FROM genres g
    JOIN ratings r ON g.movie_id = r.movie_id
    GROUP BY g.genre
    HAVING avg_rat > 7.0 AND nb_films > 50
    ORDER BY avg_rat DESC
    """
    return conn.execute(sql).fetchall()

# Q6: Évolution de carrière (CTE)
def query_career_evolution(conn, actor_name):
    """
    Films par décennie pour un acteur.
    """
    sql = """
    WITH career AS (
        SELECT 
            (m.start_year / 10) * 10 as decade,
            r.average_rating
        FROM movies m
        JOIN principals p ON m.movie_id = p.movie_id
        JOIN persons pe ON p.person_id = pe.person_id
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        WHERE pe.primary_name = ? AND m.start_year IS NOT NULL
    )
    SELECT decade, COUNT(*) as count, AVG(average_rating)
    FROM career
    GROUP BY decade
    ORDER BY decade
    """
    return conn.execute(sql, (actor_name,)).fetchall()

# Q7: Classement par genre (Window Function)
def query_rank_by_genre(conn):
    """
    Top 3 films par genre avec RANK().
    """
    sql = """
    WITH ranked AS (
        SELECT 
            g.genre, 
            m.primary_title, 
            r.average_rating,
            RANK() OVER (PARTITION BY g.genre ORDER BY r.average_rating DESC) as rk
        FROM movies m
        JOIN genres g ON m.movie_id = g.movie_id
        JOIN ratings r ON m.movie_id = r.movie_id
        -- WHERE r.num_votes > 5000
    )
    SELECT genre, primary_title, average_rating, rk
    FROM ranked
    WHERE rk <= 3
    """
    return conn.execute(sql).fetchall()

# Q8: Carrière propulsée
def query_breakout_role(conn):
    """
    Personnes ayant percé grâce à un film.
    Logique : On cherche le PREMIER film chronologique d'un acteur qui a dépassé 200k votes.
    Implique que tous les films précédents avaient < 200k votes.
    """
    sql = """
    WITH FirstHit AS (
        SELECT 
            pe.primary_name,
            m.primary_title,
            m.start_year,
            r.num_votes,
            -- On classe par année SEULEMENT les films > 200k pour chaque acteur
            ROW_NUMBER() OVER (
                PARTITION BY pe.person_id 
                ORDER BY m.start_year ASC
            ) as hit_rank
        FROM persons pe
        JOIN principals p ON pe.person_id = p.person_id
        JOIN movies m ON p.movie_id = m.movie_id
        JOIN ratings r ON m.movie_id = r.movie_id
        WHERE r.num_votes > 200000
          AND p.category IN ('actor', 'actress') -- On cible les acteurs
    )
    SELECT primary_name, primary_title, start_year, num_votes
    FROM FirstHit
    WHERE hit_rank = 1 -- On prend uniquement leur PREMIER gros succès
    ORDER BY num_votes DESC -- On trie par les plus gros succès pour l'affichage
    LIMIT 20
    """
    return conn.execute(sql).fetchall()

# Q9: Requête libre (3 jointures)
def query_free_complex(conn):
    """
    Quels films de plus de 2h, notés > 8, sont en Français ?
    """
    sql = """
    SELECT m.primary_title, m.runtime_minutes, r.average_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    JOIN titles t ON m.movie_id = t.movie_id
    WHERE m.runtime_minutes > 120 
      AND r.average_rating > 8.0 
      AND t.language = 'fr'
    LIMIT 20
    """
    return conn.execute(sql).fetchall()

# ... (Tes fonctions Q1 à Q9 restent au dessus) ...

if __name__ == "__main__":
    conn = get_conn()

    def print_test(title, data, limit=10):
        print(f"\n{'='*60}")
        print(f"TEST {title}")
        print(f"{'='*60}")
        
        if not data:
            print("⚠️  Aucun résultat trouvé (Vérifie les paramètres ou les données).")
            return

        for i, row in enumerate(data[:limit]):
            # Nettoyage de l'affichage du tuple
            formatted_row = " | ".join([str(item) for item in row])
            print(f"{i+1}. {formatted_row}")
        
        if len(data) > limit:
            print(f"... et {len(data) - limit} autres résultats.")

    # --- Lancement des 9 Tests ---

    # Q1 : Filmographie
    # Param : Tom Hanks
    print_test("Q1: Filmographie de Tom Hanks", 
               query_actor_filmography(conn, "Tom Hanks"))

    # Q2 : Top N Films
    # Params : Genre=Drama, 1990-2000, Top 5
    print_test("Q2: Top 5 Films 'Horror' (1980-1990)", 
               query_top_movies_by_genre(conn, "Horror", 1980, 1990, 5))

    # Q3 : Acteurs Multi-rôles
    print_test("Q3: Acteurs jouant plusieurs rôles dans un film", 
               query_multi_role_actors(conn))

    # Q4 : Collaborations
    # Param : Leonardo DiCaprio (On veut voir Scorsese !)
    print_test("Q4: Réalisateurs ayant collaboré avec Johnny Depp", 
               query_collaborations(conn, "Johnny Depp"))

    # Q5 : Genres Populaires
    print_test("Q5: Genres populaires (>7.0, >50 films)", 
               query_popular_genres(conn))

    # Q6 : Évolution Carrière
    # Param : Brad Pitt
    print_test("Q6: Évolution carrière de Tom Hanks (par décennie)", 
               query_career_evolution(conn, "Tom Hanks"))

    # Q7 : Classement par Genre
    print_test("Q7: Top 3 films par genre (Rank)", 
               query_rank_by_genre(conn))

    # Q8 : Carrière Propulsée
    print_test("Q8: Films propulseurs de carrière (>200k votes)", 
               query_breakout_role(conn))

    # Q9 : Requête Libre
    print_test("Q9: Films >2h, Note >8, Langue FR", 
               query_free_complex(conn))

    conn.close()