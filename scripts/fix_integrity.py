import sqlite3
import os

# Chemin vers la base de données
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'cineexplorer.db')

def fix_integrity():
    print(f"🚑 Démarrage du nettoyage de la base : {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. On désactive temporairement la vérification pour pouvoir supprimer
    cursor.execute("PRAGMA foreign_keys = OFF;")

    # --- NETTOYAGE CHARACTERS ---
    print("🧹 Nettoyage des orphelins dans 'characters'...")
    cursor.execute("DELETE FROM characters WHERE person_id NOT IN (SELECT person_id FROM persons)")
    print(f"   👉 {cursor.rowcount} orphelins (personnes) supprimés.")
    cursor.execute("DELETE FROM characters WHERE movie_id NOT IN (SELECT movie_id FROM movies)")
    print(f"   👉 {cursor.rowcount} orphelins (films) supprimés.")

    # --- NETTOYAGE PRINCIPALS ---
    print("🧹 Nettoyage des orphelins dans 'principals'...")
    cursor.execute("DELETE FROM principals WHERE person_id NOT IN (SELECT person_id FROM persons)")
    p_count = cursor.rowcount
    cursor.execute("DELETE FROM principals WHERE movie_id NOT IN (SELECT movie_id FROM movies)")
    print(f"   👉 {p_count + cursor.rowcount} orphelins supprimés.")

    # --- NETTOYAGE WRITERS (C'est lui qui bloquait !) ---
    print("🧹 Nettoyage des orphelins dans 'writers'...")
    cursor.execute("DELETE FROM writers WHERE person_id NOT IN (SELECT person_id FROM persons)")
    w_p_count = cursor.rowcount
    cursor.execute("DELETE FROM writers WHERE movie_id NOT IN (SELECT movie_id FROM movies)")
    print(f"   👉 {w_p_count + cursor.rowcount} orphelins supprimés.")

    # --- NETTOYAGE DIRECTORS (Prévention) ---
    print("🧹 Nettoyage des orphelins dans 'directors'...")
    cursor.execute("DELETE FROM directors WHERE person_id NOT IN (SELECT person_id FROM persons)")
    d_p_count = cursor.rowcount
    cursor.execute("DELETE FROM directors WHERE movie_id NOT IN (SELECT movie_id FROM movies)")
    print(f"   👉 {d_p_count + cursor.rowcount} orphelins supprimés.")

    # 4. On réactive et on commit
    conn.commit()
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 5. Vérification ultime
    print("🔍 Vérification de l'intégrité...")
    try:
        cursor.execute("PRAGMA foreign_key_check;")
        errors = cursor.fetchall()
        if not errors:
            print("✅ Base de données SAINE ! Aucune erreur d'intégrité.")
        else:
            print(f"⚠️  Il reste {len(errors)} erreurs. Voici les premières :")
            for e in errors[:5]:
                print(e)
    except Exception as e:
        print(f"Erreur lors du check: {e}")

    conn.close()

if __name__ == "__main__":
    fix_integrity()