import pymongo
from pymongo import MongoClient
import sys

def test_connection():
    print("🔌 Tentative de connexion à MongoDB (localhost:27017)...")
    
    try:
        # Connexion au client (délai court pour ne pas attendre 30s si ça plante)
        client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=2000)
        
        # Vérification active du serveur
        info = client.server_info()
        print(f"✅ SUCCÈS ! Connecté à MongoDB version {info['version']}")
        
        # Test d'écriture/lecture
        db = client['test_db']
        collection = db['test_col']
        
        # Nettoyage préventif
        collection.delete_many({})
        
        # Insertion
        doc = {"message": "Hello Polytech", "phase": 2}
        result = collection.insert_one(doc)
        print(f"📝 Document inséré avec l'ID : {result.inserted_id}")
        
        # Lecture
        found = collection.find_one({"phase": 2})
        print(f"🔍 Document relu : {found}")
        
        # Nettoyage
        client.drop_database('test_db')
        print("🧹 Base de test nettoyée.")
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("❌ ERREUR : Impossible de se connecter à MongoDB.")
        print("   -> Vérifie que 'mongod' tourne bien dans un autre terminal.")
    except Exception as e:
        print(f"❌ ERREUR inattendue : {e}")

if __name__ == "__main__":
    test_connection()