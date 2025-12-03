import sqlite3
import os

# Chemin relatif vers la DB
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def create_schema():
    # Suppression de l'ancienne base pour repartir à propre
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;") # Intégrité référentielle [cite: 146]

    print("Création des tables...")

    # 1. Movies (Parent)
    c.execute('''CREATE TABLE IF NOT EXISTS movies (
        movie_id TEXT PRIMARY KEY,
        title_type TEXT,
        primary_title TEXT,
        original_title TEXT,
        is_adult INTEGER,
        start_year INTEGER,
        end_year INTEGER,
        runtime_minutes INTEGER
    )''')

    # 2. Persons (Parent)
    c.execute('''CREATE TABLE IF NOT EXISTS persons (
        person_id TEXT PRIMARY KEY,
        primary_name TEXT,
        birth_year INTEGER,
        death_year INTEGER
    )''')

    # 3. Ratings (Enfant de Movies)
    c.execute('''CREATE TABLE IF NOT EXISTS ratings (
        movie_id TEXT PRIMARY KEY,
        average_rating REAL,
        num_votes INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )''')

    # 4. Genres (Table de liaison normalisée)
    c.execute('''CREATE TABLE IF NOT EXISTS genres (
        movie_id TEXT,
        genre TEXT,
        PRIMARY KEY (movie_id, genre),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )''')

    # 5. Principals (Casting principal)
    c.execute('''CREATE TABLE IF NOT EXISTS principals (
        movie_id TEXT,
        person_id TEXT,
        ordering INTEGER,
        category TEXT,
        job TEXT,
        PRIMARY KEY (movie_id, person_id, ordering),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    )''')

    # 6. Directors (Relation N-N)
    c.execute('''CREATE TABLE IF NOT EXISTS directors (
        movie_id TEXT,
        person_id TEXT,
        PRIMARY KEY (movie_id, person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    )''')

    # 7. Writers (Relation N-N)
    c.execute('''CREATE TABLE IF NOT EXISTS writers (
        movie_id TEXT,
        person_id TEXT,
        PRIMARY KEY (movie_id, person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    )''')

    # 8. Titles (Titres alternatifs)
    c.execute('''CREATE TABLE IF NOT EXISTS titles (
        title_id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id TEXT,
        ordering INTEGER,
        title TEXT,
        region TEXT,
        language TEXT,
        types TEXT,
        attributes TEXT,
        is_original_title INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )''')

    # 9. Characters (Détail des rôles - issu de characters.csv)
    # Note: CSV a mid, pid, name. 
    c.execute('''CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movie_id TEXT,
        person_id TEXT,
        character_name TEXT,
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    )''')

    # 10. Professions (jobs des personnes)
    c.execute('''CREATE TABLE IF NOT EXISTS professions (
        person_id TEXT,
        job_name TEXT,
        PRIMARY KEY (person_id, job_name),
        FOREIGN KEY (person_id) REFERENCES persons(person_id)
    )''')
    
    # 11. KnownForMovies (Pour quels films une personne est connue)
    c.execute('''CREATE TABLE IF NOT EXISTS known_for (
        person_id TEXT,
        movie_id TEXT,
        PRIMARY KEY (person_id, movie_id),
        FOREIGN KEY (person_id) REFERENCES persons(person_id),
        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
    )''')

    conn.commit()
    conn.close()
    print("Schéma créé avec succès (3NF).")

if __name__ == "__main__":
    create_schema()