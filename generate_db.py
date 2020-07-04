import os, json, subprocess, re, sys
import pandas as pd
from glob import glob
from datetime import datetime
import requests
from api_query import RepositoryQuery, DataExporter


def main():

    #assign docker tag info from command line arg
    tag_info = sys.argv[1]

    # if CSV files exists delete them
    csvs = [f for f in os.listdir('.') if f.endswith('.csv') or f.endswith('.db')]
    for f in csvs:
        os.remove(f)

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
        de.export_collection_as_csv(q, pid,col_fname=name)


    # locate all csv files in collection_data/
    csv_files = glob('*.csv')

    # concat all csvs into new csv
    all_items= pd.concat([pd.read_csv(f, sep='|',
        quoting=3) for f in csv_files], sort=False)
    
    all_items = all_items.drop_duplicates()
    all_items.to_csv( "all_unique_items.csv",sep='|',
        index=False, encoding='utf-8')


    # create sqlite db and populate w/ csvs with FTS on title and document_type
    for csv in glob('*.csv'):
        print(f"WRITING: {csv} to DB...")
        subprocess.call(f"csvs-to-sqlite {csv} {database} -s $'|' -q 3" ,
        shell=True)

    # create docker image using datasete
    subprocess.call(f'datasette package {database} -t {tag_info}', shell=True)


if __name__ == "__main__":
    main()