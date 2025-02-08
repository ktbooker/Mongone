# Mongone

Mongone for TartanHacks2025

Mongone is a ETL program for migrating away from MongoDB to a true red-blooded relation database of your choice.

## Entrypoint

After doing all the setup described in the `Installation` section, the ETL program can be run with the command:

```sh
usage: main.py [-h] [--conn_str CONN_STR] [--db DB] [--collection COLLECTION]

Script to parse database and collection names.

options:
  -h, --help            show this help message and exit
  --conn_str CONN_STR   Mongo connection string.
  --db DB               Name of the database.
  --collection COLLECTION
                        Name of the collection.
```

## Installation (macOS)

### Install MongoDB

```sh
brew tap mongodb/brew
brew install mongodb-community
```

### Starting MongoDB

```sh
mkdir -p ./data/db
mongod --dbpath ./data/db
```

### Using the Mongo shell

```sh
mongosh
```

## Generating Sample Data

```sh
Mongone/       $ cd utils
Mongone/utils/ $ node generate_test_data.js
```

## Load the data into Mongo

```sh
Mongone/ $ ./scripts/load_data.sh
```

## Python Setup

All from the `Mongone/` directory

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```
