"""
Exports documents from a MongoDB collection to a file using the `mongoexport` command,
then loads it into memory and returns the collection as a list of dictionaries.
"""
import json
import os
import tempfile

from logger import logger

"""
Sample usage:
download_collection("mongodb://localhost:27017", "testDB", "users", tempfile.NamedTemporaryFile(mode="w+"))
"""
def download_collection(mongo_conn_string: str, db: str, collection: str, output_file: tempfile.NamedTemporaryFile):
    cmd = f"mongoexport --uri='{mongo_conn_string}' --db='{db}' --collection='{collection}' --out='{output_file.name}'"

    # Run the mongoexport command and raise on nonzero exit code
    logger.info(f"Loading the collection '{collection}' into memory...")
    if os.system(cmd) != 0:
        logger.error("mongoexport failed.")
        raise Exception('mongoexport failed')

    output_file.seek(0)
    collection = [json.loads(object_str) for object_str in output_file.readlines()]

    return collection
