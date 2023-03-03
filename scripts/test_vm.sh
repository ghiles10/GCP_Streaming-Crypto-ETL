#!/bin/bash

# Vérifier si Docker est installé
if command -v docker 
then
    echo "Docker est installé"
    
else
    echo "Docker n'est pas installé"
fi

echo "---------------------"

# Vérifier si Docker Compose est installé
if command -v docker-compose 
then
    echo "Docker Compose est installé"
else
    echo "Docker Compose n'est pas installé"
fi

echo "---------------------"

# Vérifier si Python est installé
if command -v python3 
then
    echo "Python 3 est installé"
else
    echo "Python 3 n'est pas installé"
fi
