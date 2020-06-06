import os, subprocess
import sqlite3
from api_query import RepositoryQuery


"""Populate sqlite database with ir data"""


class Database():


    def __init__(self, db_name,table_fields, ir_api):
        # remove db file if exists
        self.db_name = check_if_db_exists(db_name)            
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.table_fields = table_fields
        self.columns = ','.join(list(self.table_fields))
        self.implicit_vals = generate_implicit_values(f.values())        
        self.ir_api = ir_api

        
    def __repr__(self):
        return f"""
            ->    Database: {self.db_name}
            ->    Fields: {self.table_fields}
                """

    def __str__(self):
        return f"""
            ->    Database: {self.db_name}
            ->    Fields: {self.table_fields}
                """

    def create_table(self, table_name):
        """
        Execute sqlite CREATE statement in order to create
        table.
        Parameters:
            table_name: db table name.
        """

        #create table
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{table_name}`
                     ({self.columns})""")


    def close_db(self):
        """Close sqlite db"""

        #close the connection
        self.conn.close()


    def insert_rows(self, table_name, data):
        """
        Execute sqlite INSERT statement to add multiple rows.
        Parameters:
            table_name: db table name
            data: ir api collection names and associated pids
        """

        #add rows
        self.cursor.executemany(f"""INSERT INTO `{table_name}`
                VALUES ({self.implicit_vals})""",data)

        #commit the changes to db			
        self.conn.commit()

 
    def insert_single_collection(self, table_name, data):
        "populate sqlite database using repo data"

        print(data)

        #call insert rows function
        self.insert_rows(table_name, data)

  
    def insert_all_repo_collections(self, collection_info):
        """
        Insert all ir collections into database instance.
        Parameters:
            table_name: table name of database
            collection_info: repository query attribute containing
            collection names (values) and pids (keys)
        """
        
        for name, pid in collection_info.items():
            json_d = self.ir_api.get_collection_json(pid)
            data = self.ir_api.filter_collection_json(json_d)
            table_name = brief_collection_names(name)

            # create table for each collection
            self.create_table(table_name)

            #call insert rows function
            self.insert_rows(table_name, data)

        
    def launch_datasette_locally(self):
        """
        Launch datasette instance of populated db on local web server.
        """
        subprocess.call(['datasette',self.db_name])


    def launch_datasette_heroku(self):
        """
        Launch datasette instance of populated db on heroku. Must have
        account and creditentials for this to be possible.
        """

        #publish sqllite datasette on heroku
        subprocess.call(['datasette','publish', 'heroku', self.db_name,
            '-n','rey-ir-collections'])


def check_if_db_exists(db_name):
    """
    Check if database exists. If it does delete file and
    create new instance.
    Parameters:
        db_name: name of database instance
    """
    if os.path.exists(db_name):
        os.remove(db_name)
    
    return db_name


def brief_collection_names(name):
        """
        Create brief collection names based on api collection keys
        Paramters:
            name: collection name
        """

        brief = (name[:25] + '...') \
            if len(name) > 25 else name
        return brief


def generate_implicit_values(fields):
    """
    dynamically generate values
    Parameters:
        fields: database field attribute
    """

    implicit = []

    for field in fields:
        if 'autoincrement' in field:
            implicit.append('NULL')
        elif 'autoincrement' not in field: 
            implicit.append('?') 

    return ','.join(implicit)



if __name__ == "__main__":

    # initialize table class
    f = {'id': 'integer primary key autoincrement', 'link':'text',
        'title':'text', 'doc_type': 'text', 'facets': 'text', 'doi': 'text'}

    q = RepositoryQuery()
    db = Database('ir_data.db',f, q)
    # db.create_table('oer')

    # json_data = q.get_collection_json('4')
    # data = q.filter_collection_json(json_data)

    # db.insert_single_collection('oer', data)

    # db.insert_all_repo_collections(q.pid_dict)
