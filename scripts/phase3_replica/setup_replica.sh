#!/bin/bash

# Définition des chemins de données
DB1="./data/mongo/db-1"
DB2="./data/mongo/db-2"
DB3="./data/mongo/db-3"
LOG_DIR="./data/mongo/logs"

# Création des dossiers si nécessaire
mkdir -p $DB1 $DB2 $DB3 $LOG_DIR

echo "--- Arrêt des instances MongoDB existantes... ---"
pkill -f mongod
sleep 2

echo "--- Démarrage des 3 nœuds du Replica Set 'rs0' ---"

# Sur macOS, on ne peut pas utiliser --fork. On utilise le "&" du shell.
# On redirige stdout et stderr vers le fichier de log.

# Nœud 1 (Port 27017)
mongod --replSet rs0 --port 27017 --dbpath $DB1 --bind_ip localhost --logpath "$LOG_DIR/mongo1.log" &
echo "   [OK] Nœud 1 démarré sur le port 27017"

# Nœud 2 (Port 27018)
mongod --replSet rs0 --port 27018 --dbpath $DB2 --bind_ip localhost --logpath "$LOG_DIR/mongo2.log" &
echo "   [OK] Nœud 2 démarré sur le port 27018"

# Nœud 3 (Port 27019)
mongod --replSet rs0 --port 27019 --dbpath $DB3 --bind_ip localhost --logpath "$LOG_DIR/mongo3.log" &
echo "   [OK] Nœud 3 démarré sur le port 27019"

echo "--- Attente de 5 secondes pour l'initialisation... ---"
sleep 5

echo "--- Configuration du Replica Set ---"
mongosh --port 27017 --eval '
  try {
    rs.initiate({
      _id: "rs0",
      members: [
        { _id: 0, host: "localhost:27017" },
        { _id: 1, host: "localhost:27018" },
        { _id: 2, host: "localhost:27019" }
      ]
    })
    print("   [SUCCESS] Replica Set initié !");
  } catch (e) {
    print("   [INFO] Replica Set déjà configuré ou erreur : " + e);
  }
'

echo "--- Terminé ! Vérifie le statut avec : rs.status() ---"