#!/usr/bin/env bash

# Should only trigger when requirements have changed.
pip install -r GIT-repository/dev-requirements.txt
# update 
apt-get update
# install curl 
apt-get install curl
# get install script and pass it to execute: 
curl -sL https://deb.nodesource.com/setup_5.x | bash
# and install node 
apt-get install nodejs
# confirm that it was successful 
node -v
# npm installs automatically 
npm -v