#!/usr/bin/env bash

set -e
sudo /etc/init.d/postgresql start
PGPASSWORD=solana psql -U solana -p 5432 -h localhost -w -d solana -f scripts/create_schema.sql
