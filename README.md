# WomboDB

WomboDB for TartanHacks2025

WomboDB is a ETL program for migrating away from MongoDB to a true red-blooded relation database of your choice.

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
