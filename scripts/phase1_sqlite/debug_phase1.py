import sqlite3
import os

# Chemin vers la DB
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def debug_db():
    if not os.path.exists(DB_PATH):
        print(f"❌ La base de données est introuvable à : {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"--- DIAGNOSTIC DE LA BASE IMDB ---")

    # 1. Vérifier le nombre de lignes (Est-ce que l'import a marché ?)
    tables = ['movies', 'persons', 'principals']
    for t in tables:
        count = cursor.execute(f"SELECT Count(*) FROM {t}").fetchone()[0]
        print(f"Table '{t.upper()}' : {count} lignes")
        if count > 0:
            # On affiche une ligne brute pour voir la gueule des données
            sample = cursor.execute(f"SELECT * FROM {t} LIMIT 1").fetchone()
            print(f"   -> Exemple : {sample}")
        else:
            print("   ⚠️  ATTENTION : TABLE VIDE !")

    print("\n--- TEST SPECIFIQUE TOM HANKS ---")
    # 2. Chercher Tom Hanks (large)
    # On cherche n'importe quoi qui ressemble à Hanks
    res = cursor.execute("SELECT person_id, primary_name FROM persons WHERE primary_name LIKE '%Hanks%' LIMIT 5").fetchall()
    
    if not res:
        print("❌ 'Tom Hanks' introuvable dans la table PERSONS.")
        print("   -> Si la table persons n'est pas vide, c'est peut-être un problème d'encodage ou de format (ex: '('Tom Hanks',)' )")
    else:
        print(f"✅ Trouvé dans PERSONS : {res}")
        pid = res[0][0] # On prend le premier
        
        # 3. Vérifier ses films dans PRINCIPALS
        roles = cursor.execute("SELECT * FROM principals WHERE person_id = ?", (pid,)).fetchall()
        print(f"🔍 Recherche rôles pour PID={pid} dans PRINCIPALS...")
        if not roles:
            print("❌ Aucun rôle trouvé dans PRINCIPALS pour cet ID.")
            print("   -> Problème : Le lien Personne <-> Film est rompu (Table principals mal importée ?)")
        else:
            print(f"✅ {len(roles)} rôles trouvés. Exemple : {roles[0]}")

    conn.close()

if __name__ == "__main__":
    debug_db()