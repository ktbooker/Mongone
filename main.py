
'''
Goals:
1. ETL a single collection into a single table.
1.a: Resolve dynamic types to varchars.
2. ETL a single collection into multiple normalized tables.
3. 

Main Problems:
1. Dynamic Types
2. Nested Fields / JSON
3. 
'''

import argparse
import tempfile
import csv
from collections import defaultdict
from datetime import datetime

import art
import ast
import download_client
import duckdb
from logger import logger

def get_args():
    """Parse command-line arguments for database and collection names."""
    parser = argparse.ArgumentParser(description="Script to parse database and collection names.")

    parser.add_argument("--conn_str", required=True, help="Mongo connection string.", default="mongodb://localhost:27017")
    parser.add_argument("--db", required=True, help="Name of the database.", default="testDB")
    parser.add_argument("--collection", required=True, help="Name of the collection.", default="users")

    return parser.parse_args()

def main():
    # Read in user parameters from the command line
    args = get_args()
    art.print_startup_art()

    # Load the collection into memory
    with tempfile.NamedTemporaryFile(mode='w+') as output_file:
        collection = download_client.download_collection(
            mongo_conn_string=args.conn_str,
            db=args.db,
            collection=args.collection,
            output_file=output_file
        )

    logger.info("Computing collection stats...")
    field_names_to_types, field_name_count, flattened_coll = collection_stats(collection)

    # schema = [(key,value) for key, value in field_names_to_types.items()]

    emit_duckdb(field_names_to_types, flattened_coll, args)

    '''
    Let us now figure out the schema(s)!
    '''


'''
 Given a collection (list of document JSONs)
    - flatten all documents
    - find all fields with their frequency count and type
    - perform type inferencing for datetime and booleans, and resolve conflicts between types

 returns: dict('str' -> 'str'), dict('str' -> 'int'), array of JSONS
'''
def collection_stats(collection: list) -> tuple:
    field_names_to_types = {}
    field_name_count = defaultdict(int)
    flattened_coll = []
    possible_bool_fields = defaultdict(lambda: True)
    possible_datetime_fields = defaultdict(lambda: True)

    for doc in collection:
        flattened_doc = flatten_nested_json(doc)

        # extract the fields and types and counts
        for field, value in flattened_doc.items():
            value_type = type(value).__name__
            if field not in field_names_to_types:
                field_names_to_types[field] = value_type
            elif field_names_to_types[field] != value_type:
                existing_type = field_names_to_types[field]

                # conflict resolution logic
                if {existing_type, value_type} == {"int", "float"} or {existing_type,value_type} == {"float", "int"}:
                    field_names_to_types[field] = "float" # float wins over int
                elif existing_type != value_type:
                    field_names_to_types[field] = 'str' # convert to string for other conflicts

            field_name_count[field] += 1

            # extract bool if all values for field_name is str of 'true' or 'false'
            if isinstance(value, str) and value.lower() in {"true", "false"}:
                possible_bool_fields[field] &= True
            else:
                possible_bool_fields[field] = False

            # extract date if all values are in the mongo datetime form
            if isinstance(value, str) and is_datetime(value):
                possible_datetime_fields[field] &= True
            else:
                possible_datetime_fields[field] = False

        flattened_coll.append(flattened_doc)

    # update types for booleans
    for field, is_boolean_type in possible_bool_fields.items():
        if is_boolean_type:
            field_names_to_types[field] = "bool"

    # update types for dates
    for field, is_datetime_type in possible_datetime_fields.items():
        if is_datetime_type:
            field_names_to_types[field] = "datetime"

    return field_names_to_types, field_name_count, flattened_coll

'''
Given a value, determine if it's in datetime form
'''
def is_datetime(value):
    """
    Checks if a value is a valid datetime.
    - Direct MongoDB `datetime.datetime` → True
    - String formatted as ISO 8601 (`YYYY-MM-DDTHH:MM:SS[Z]`) → True
    """
    if isinstance(value, datetime):
        return True  # Already a valid MongoDB datetime

    if isinstance(value, str):
        formats = [
            "%Y-%m-%dT%H:%M:%S", # Local time (MongoDB converts to UTC)
            "%Y-%m-%dT%H:%M:%S.%f", # Local time with milliseconds
            "%Y-%m-%dT%H:%M:%SZ", # Explicit UTC time
            "%Y-%m-%dT%H:%M:%S.%fZ" # Explicit UTC with milliseconds
        ]
        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
    return False

''' Parse a list back from a string'''
def try_parse_list(value):
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return parsed
    except (ValueError, SyntaxError):
        return None

'''
Given a document (JSON) flatten by bringing all nested fields to the top level
returns: dict_items['str', value]
'''
def flatten_nested_json(doc, parent_key=''):
    flattened_doc = {}
    for key, value in doc.items():
        new_key = f"{parent_key}\\{key}" if parent_key else key

        if new_key == "_id\\$oid": #mongo ObjectID field
            new_key = "id"

        if isinstance(value, dict): # recurse for nested dicts
            flattened_doc.update(flatten_nested_json(value, new_key))
        elif isinstance(value, list): # turn arrays into strings
           flattened_doc[new_key] = str(value)
        else:
            flattened_doc[new_key] = value

    return flattened_doc


# Takes a csv as a string array and emits it to the file at file name.
def emit_csv(csv_array, filename):
    csv.writer(open(filename.name, 'w', newline=''), delimiter='|').writerows(csv_array)

def emit_duckdb(field_names_to_types, flattened_coll, args):

    # DuckDB LIST<TYPE>
    def add_list_type(field_names_to_types, flattened_coll):
        possible_list = defaultdict(lambda: True)
        field_array_types = defaultdict(set)
        for doc in flattened_coll:
            for field, value in doc.items():
                if isinstance(value, str):
                    parsed_value = try_parse_list(value)
                    if parsed_value is not None: # if it's a list
                        possible_list[field] &= True
                        for i, item in enumerate(parsed_value):
                            field_array_types[field].add(type(item).__name__)
                            parsed_value[i] = str(item) # convert each item in the list to a string
                        doc[field] = parsed_value # update this doc in flattened_col to make all elements a string
                    else:
                        possible_list[field] = False
                else:
                    possible_list[field] = False

        # update if there are lists
        for field, is_list_type in possible_list.items():
            if is_list_type:
                element_types = field_array_types[field]

                if not element_types: #empty list is string
                    field_names_to_types[field] = "LIST<STRING>"
                elif len(element_types) == 1: #all elements in the array are the same type
                    element_type = next(iter(element_types)).upper()
                    if element_type in {"STRING", "FLOAT", "BOOL", "INT"}:
                        field_names_to_types[field] = f"LIST<{element_type}>"
                    else: #anything but a simple primitive is a string
                        field_names_to_types[field] = "LIST<STRING>"
                else: #only handle conflict resolution for floats and ints
                    if {"int", "float"} == set(element_types):
                        field_names_to_types[field] = "LIST<FLOAT>"
                    else:
                        field_names_to_types[field] = "LIST<STRING>"

        return field_names_to_types, flattened_coll

    def type_to_duckdb(tau):
        match tau.split():
            case ["int"]:
                return "INT"
            case ["str"]:
                return "VARCHAR"
            case ["float"]:
                return "DOUBLE"
            case ["bool"]:
                return "BOOLEAN"
            case["datetime"]:
                return "TIMESTAMP"
            case["LIST<STRING>"]:
                return "LIST<VARCHAR>"
            case _:
                return tau

    def convert_schema_to_duckdb(schema: dict) -> dict:
        return {key : type_to_duckdb(value) for key, value in schema.items()}

    '''
    Given a document {"field name": "value"} and 
          a schema {"field name" : "type"}
    Return a list of values of that row 
    append null if field does not exist 
    '''
    def convert_row(row1, schema) -> list:
        row_items = []
        for field, value in row1.items():
            if field not in schema.keys():
                row_items.append("NULL")
            else:
                row_items.append(value)
        return row_items

    preemit_csv = []

    field_names_to_types, flattened_coll = add_list_type(field_names_to_types, flattened_coll)
    duckdb_schema = convert_schema_to_duckdb(field_names_to_types)
    # Bad style! See Course Staff!
    preemit_csv.append(duckdb_schema.keys())
    for row in flattened_coll:
        preemit_csv.append(convert_row(row, duckdb_schema))

    # generate temp file
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv') as output_file:
        emit_csv(preemit_csv, output_file)
        # TODO: Make duckdb import the csv data.
        conn = duckdb.connect(database=f"{args.db}.duckdb")
        conn.sql(f"CREATE TABLE {args.collection} AS FROM read_csv('{output_file.name}', delim = '|', header = true);")
        data = conn.sql(f"SELECT * FROM '{args.collection}';").fetchall()
        print(duckdb.__version__)
        conn.close()



    '''
    Place schema as first row, all names are valid.
    Line up the flattened with the schemas.
    '''


if __name__ == '__main__':
    main()