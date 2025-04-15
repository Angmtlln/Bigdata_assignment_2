#!/bin/bash
service ssh restart 
bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt  
venv-pack -o .venv.tar.gz

bash prepare_data.sh
bash index.sh




#bash search.sh "this is a query!"
#bash search.sh "How to learn to do backflip?"
bash search.sh "Hip-hop evolution in USA"