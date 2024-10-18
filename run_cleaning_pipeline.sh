#!/bin/bash

# Exécution des scripts de nettoyage et transformation des données
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar Clean_customers2.py
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar clean_bikes.py
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar clean_bikeshops.py
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar clean_orders2.py

# Exécution du script pour créer les tables en étoile (star schema)
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar create_star_schema2.py

# Exécution du script pour créer la table de lookup calendrier
spark-submit --jars /path/to/spark-csv.jar,/path/to/commons-csv.jar create_calendar_lookup2.py
