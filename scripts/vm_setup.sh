#!/bin/bash
 
echo "step 1 ---> update and upgrade"
echo "---------------------------"
sudo apt-get update

echo "step 2 ---> install python3"
echo "---------------------------"
sudo apt-get -y install python3 python3-pip 


echo "step 3 : ----> install Docker..."
echo "---------------------------"
sudo apt-get -y install docker.io

echo "step 4 : ----> install Docker Compose..."
echo "Instal docker-compose..."
echo "---------------------------"
cd ~ 
mkdir -p bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.3.3/docker-compose-linux-x86_64 -O docker-compose
sudo chmod +x docker-compose

echo "Setup .bashrc..."
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
eval "$(cat ~/.bashrc | tail -n +10)" # A hack because source .bashrc doesn't work inside the script
