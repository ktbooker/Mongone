from collections import defaultdict

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
import os
import json

def get_args():
    """Parse command-line arguments for database and collection names."""
    parser = argparse.ArgumentParser(description="Script to parse database and collection names.")

    parser.add_argument("--conn_str", required=True, help="Mongo connection string.", default="mongodb://localhost:27017")
    parser.add_argument("--db", required=True, help="Name of the database.", default="testDB")
    parser.add_argument("--collection", required=True, help="Name of the collection.", default="users")

    return parser.parse_args()

def main():
    pass
    # Read in user parameters from the command line
    args = get_args()

    # Create format string to invoke mongoexport
    def export_command(mongo_conn_string : str, db: str, collection : str, output_file : str) -> str:
        return f"mongoexport --uri='{mongo_conn_string}' --db='{db}' --collection='{collection}' --out='{output_file}'"

    temp = tempfile.NamedTemporaryFile(mode='w')
    cmd = export_command(mongo_conn_string=args.conn_str, db=args.db, collection=args.collection, output_file=temp.name)
    os.system(cmd)
    temp.seek(0)

    collection = [json.loads(object_str) for object_str in temp.readlines()]

    field_names_to_types, field_name_count, flattened_coll = collection_stats(collection)

    schema = [(key,value) for key, value in field_names_to_types.items()]





    '''
    Let us now figure out the schema(s)!
    '''




    #extended_json_string = '{"_id": {"$oid": "61e9bfb3b7c3b7d3b7b7b7b7"}, "name": "Example", "count": {"$numberInt": "42"}}'
    #python_object = loads(extended_json_string)
    #field_names_to_types, field_name_count, array_of_docs = collection_stats(python_object)

    temp.close()

#take in JSON object representing the collection
def collection_stats(collection):
    field_names_to_types = {}
    field_name_count = defaultdict(int)

    '''
          MAIN LOGIC LOOP: 
              For each collection:
                  Parse all of the documents to create statistics.
                      { fieldname : type }
                      Collect all fields, get counts for each field, if more than 1 type then get all of the type

                  Determine schema(s) for potentially multiple tables if were doing auto table normalization.

                  Transpile
          '''
    flattened_coll = []
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

        flattened_coll.append(flattened_doc)

    return field_names_to_types, field_name_count, flattened_coll


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


def emit_duckdb():
    pass


    '''
    Place schema as first row, all names are valid.
    Line up the flattened with the schemas.
    '''


if __name__ == '__main__':
    main()