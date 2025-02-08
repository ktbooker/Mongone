#!/bin/bash

URI="mongodb://localhost:27017"
DB="testDB"
COLLECTION="users"
generated_data_path="${PWD}/utils/largeDataset.json"

echo $PWD
if [ -f "${generated_data_path}" ]; then 
    mongoimport --uri="${URI}" --db="${DB}" --collection="${COLLECTION}" --type=json --file="${generated_data_path}" --jsonArray
else
    echo "Did not find generated data at '${generated_data_path}'"
fi
