import os, json, subprocess, re, sys, shutil
import pandas as pd
from glob import glob
from datetime import datetime
import requests
from api_query import RepositoryQuery, DataExporter


"""
Generate SQLite database of NOAA IR
holdings.
"""

def normalize_names(name):
    """
    Normalizes colleciton names, removing parentheses and
    replacing spaces with unscores. 

    Parameters:
        name:  NOAA IR collection name string

    """

    name = name.replace(' ','_').lower()
    name = re.sub('[(].+','',name)
    name = re.sub(r'_$','',name)
    return name


def write_metadata_json(database, repository_query):
    """
    Generate metadata.json file.

    """
    
    tables = {}
    for name in repository_query.pid_dict.keys():
        #  call normalize function, 
        #  allowing metadata.json file be diplayed for each collection
        tables.update({normalize_names(name) : 
            {'description': f'Items in {name} collection'}})

    meta = {'databases': 
    { database.replace('.db', '') : 
    {'title': 'NOAA Institutional Reposistory data',
    'description': 
    'Table for each collection in NOAA IR as well as a table for all unique items.',
    'tables': tables
    }}}

    with open('metadata.json', 'w') as f:
        json.dump(meta, f, indent=2)


def new_db(ir_fields, app_name):

    # WARNING: before before changing this dir!!
    # moving it can cause you to delete files!!
    data_dir = 'output'

    # remove dir along with all files
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.mkdir(data_dir)
    
    # create database name
    database = "collections-" + datetime.now().strftime('%m-%d-%Y') + '.db'

    # create database path
    database_path = os.path.join(data_dir, database)

    # donwload ir_data
    q = RepositoryQuery(ir_fields)
    de = DataExporter()

    write_metadata_json(database, q)

    collection_info = q.pid_dict
    for name, pid in collection_info.items():
        name = normalize_names(name) + '.csv'

        # query each collection individually,
        # add to collection data dir
        de.export_collection_as_csv(
            q,
            pid,
            export_path=data_dir,
            col_fname=name)

    # locate all csv files in collection_data/
    csv_files = glob(os.path.join(data_dir,'*.csv'))

    # concat all csvs into new csv
    all_items= pd.concat([pd.read_csv(f, sep='\t',
        quoting=3) for f in csv_files], sort=False)
    
    all_items = all_items.drop_duplicates()
    all_items.to_csv(
        os.path.join(data_dir,"all_unique_items.csv"),
        sep='\t',
        index=False,
        encoding='utf-8')

    # create sqlite db and populate w/ csvs with FTS on title and document_type
    for csv in glob(os.path.join(data_dir,'*.csv')):
        print(f"WRITING: {csv} to DB...")
        subprocess.call(f"csvs-to-sqlite {csv} {database_path} -s $'\t' -q 3" ,
        shell=True)

    # publish heroku app
    subprocess.call(f'datasette publish heroku {database_path} \
        --name {app_name} --metadata metadata.json', shell=True)

   
if __name__ == "__main__":
    fields = [ 'PID', 'mods.title','mods.type_of_resource',
    'fgs.createdDate','mods.sm_digital_object_identifier',
    'mods.related_series']

    app_name = sys.argv[1]
    new_db(fields, app_name)