#!/usr/bin/env bash

set -ex

sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

sudo apt-get update
sudo apt-get install -y postgresql-14

sudo /etc/init.d/postgresql start
sudo -u postgres psql --command "CREATE USER solana WITH SUPERUSER PASSWORD 'solana';"
sudo -u postgres createdb -O solana solana