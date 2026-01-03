import sqlite3

conn = sqlite3.connect('data/cineexplorer.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("📋 COLONNES DE LA TABLE 'movies' :")
cursor.execute("PRAGMA table_info(movies)")
columns = [row[1] for row in cursor.fetchall()]
print(columns)

print("\n🧪 EXEMPLE DE LIGNE :")
row = cursor.execute("SELECT * FROM movies LIMIT 1").fetchone()
print(dict(row) if row else "Vide")

conn.close()