import os, json, subprocess, re, sys, shutil
import pandas as pd
from glob import glob
from datetime import datetime
import requests
from api_query import RepositoryQuery, DataExporter


"""
Script used to create a Datasette docker image.

"""


def main():

    # run bash script to check if Docker is runnning
    # if not, run Docker
    subprocess.call('./is_docker_running.sh')

    #assign docker tag info from command line arg
    data_dir = sys.argv[1]
    tag_info = sys.argv[2]

    # remove dir along with all files
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.mkdir(data_dir)
    

    # create database name
    database = "collections-" + datetime.now().strftime('%m-%d-%Y') + '.db'

    # donwload ir_data
    q = RepositoryQuery()
    de = DataExporter()

    collection_info = q.pid_dict
    for name, pid in collection_info.items():
        name = name.replace(' ', '_').lower()
        name = re.sub('[(].+','',name) + '.csv'

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
    all_items= pd.concat([pd.read_csv(f, sep='|',
        quoting=3) for f in csv_files], sort=False)
    
    all_items = all_items.drop_duplicates()
    all_items.to_csv(
        os.path.join(data_dir,"all_unique_items.csv"),
        sep='|',
        index=False,
        encoding='utf-8')

    # create sqlite db and populate w/ csvs with FTS on title and document_type
    for csv in glob(os.path.join(data_dir,'*.csv')):
        print(f"WRITING: {csv} to DB...")
        subprocess.call(f"csvs-to-sqlite {csv} {os.path.join(data_dir,database)} -s $'|' -q 3" ,
        shell=True)

    # create docker image using datasete
    subprocess.call(f'datasette package {os.path.join(data_dir,database)} -t {tag_info}',
        shell=True)


if __name__ == "__main__":
    main()