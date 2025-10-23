#!/usr/bin/env bash
set -euo pipefail

dbx format < fruits.dbx > fruits_formatted.dbx
mv fruits_formatted.dbx fruits.dbx
dbx golang -p fruitsdbx -d pgx fruits.dbx .
dbx schema -d pgx fruits.dbx .
