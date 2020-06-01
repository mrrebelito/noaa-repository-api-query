import os
import sqlite3
from api_query import RepositoryQuery


"""Populate sqlite database with repo data"""


def brief_collection_names(name):
    'Create brief collection names based on api collection keys'

    split_name = name.split(' ')
    split_name = [x[0] for x in split_name if x.upper() and x[0] is not '(']
    
    return ''.join(split_name)


def create_and_insert_table_info(table_name,data):
    """Execute sqlite CREATE and INSERT statements in order to create
    table and insert rows"""

    #create table
    c.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}
                (link text,title text, doc_type text, facets text, doi text)""")

    #add rows
    c.executemany(f'INSERT INTO {table_name} VALUES(?,?,?,?,?);',data)


def export_collection_to_db(table_name, data):
    "populate sqlite database using repo data"

    #call create_insert_table_info function
    create_and_insert_table_info(table_name, data)

    #commit the changes to db			
    conn.commit()
    #close the connection
    conn.close()


def export_all_collections_to_db(collection_info,repository_query):
    
    for name, pid in collection_info.items():
        json_d = repository_query.get_json(pid)
        data = repository_query.get_collection_data(json_d)
        table_name = brief_collection_names(name)

        create_and_insert_table_info(table_name, data)

        #commit the changes to db			
        conn.commit()
    
    #close the connection
    conn.close()


if __name__ == "__main__":

    q = RepositoryQuery()
    db_name = 'test.db'

    # remove db file
    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite3.connect(db_name)
    c = conn.cursor()





