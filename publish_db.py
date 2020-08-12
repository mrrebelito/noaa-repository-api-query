import subprocess, os
from glob import glob
import sys
from generate_db import new_db, write_metadata_json


""" 
CLI used to publish a Datasette instance either to:
  1) directly to Heroku;
  2) as a Docker image.

Must have a Heroku account set up as well as the CLI configured to publish
to Heroku.
"""


# pub_types: heroku or docker 
print("Choose between either publishing type: 'docker' or 'heroku'")
pub_type = input('Publish type: ')

# verify pub_types
while (pub_type.lower() != 'docker' and pub_type.lower() != 'heroku'):
        print("Choose between either publishing type: 'docker' or 'heroku'")
        pub_type = input('Publish type: ')

data_dir = input('DB directory location: ')

# run bash script to check if docker is running
if pub_type == 'docker':
    subprocess.call('./is_docker_running.sh')

# generate db
new_db(data_dir)

database = glob(os.path.join(data_dir,'*.db'))[0]

if pub_type == 'docker':
    
    tag_info = input('Docker tag info: ')  

    subprocess.call(f'datasette package {database} \
        -t {tag_info} --metadata metadata.json', shell=True)

elif pub_type == 'heroku':

    app_name = input('Heroku app name: ')

    subprocess.call(f'datasette publish heroku {database} \
        -n {app_name} --metadata metadata.json', shell=True)
