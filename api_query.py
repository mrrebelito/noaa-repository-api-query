import os, csv
from datetime import datetime
import requests

""" 
Module used to query NOAA Institutional Repository.

Contains Query class which will return IR collection data in the 
following ways: 
1) individual collections in CSV
2) all collections in CSV
"""

class RepositoryQuery():
    """Query class used to interact with NOAA Repository JSON API"""

    api_url = "https://repository.library.noaa.gov/fedora/export/download/collection/"
    item_url = "https://repository.library.noaa.gov/view/noaa/"

    # dictionary containing NOAA Repository collections and associated PIDS
    pid_dict = { 
                "National Environmental Policy Act (NEPA)" : "1",
                "Weather Research and Forecasting Innovation Act" : "23702",
                "Coral Reef Conservation Program (CRCP)" : "3",
                "Ocean Exploration Program (OER)" : "4",
                "National Marine Fisheries Service (NMFS)" : "5",
                "National Weather Service (NWS)": "6",
                "Office of Oceanic and Atmospheric Research (OAR)" : "7",
                "National Ocean Service (NOS)" : "8",                
                "National Environmental Satellite and Data Information Service (NESDIS)" : "9",
                "Sea Grant Publications" : "11",
                "Education and Outreach" : "12",
                "NOAA General Documents" : "10031",
                "NOAA International Agreements" : "11879",
                "Office of Marine and Aviation Operations (OMAO)" : "16402",
                "Integrated Ecosystem Assessment (IEA)":"22022",
                "NOAA Cooperative Institutes": "23649"
            }

    def __init__(self):
        self.pid = ''


    def get_json(self,pid):
        """
        Collection is queried via API, returning JSON.
        Parameters: 
            pid: collection pid
        """
        #first query
        full_url = self.api_url + check_pid(self.pid_dict, pid)
        r = check_url(full_url)
        return r.json()
        
    
    def get_collection_data(self,json_data):
        """
        Individual collection is iterated over to return
        title and item link in form of list of lists.
        Parameters:
            json_data: JSON data from an individual collection.
        """
        collection_info = []
        for row in json_data['response']['docs']:
            link = row['PID'].replace('noaa:', self.item_url)
            try:
                title = row['mods.title'].replace('\n','')
            except KeyError:
                title = ''
            try:
                doc_type = row['mods.type_of_resource'][0]
            except KeyError:
                doc_type = ''
            try:
                facets = row['mods.sm_localcorpname']
                facets = ';'.join(facets)
            except KeyError:
                facets = ''
            try:
                doi = row['mods.sm_digital_object_identifier'][0]
            except KeyError:
                doi = ''
            
            collection_info.append([link,title, doc_type,
                facets, doi])
            
        return collection_info


    def get_all_repo_data(self):
        """ 
        Get data all collections in IR using collection name and PID 
        dictionary. 
        
        Utilize generator function to call function to
        retrieve all IR collections.
        
        Collection data is returned in JSON format.
        """
        for collection in self.pid_dict.values():
            yield self.get_json(collection)  


class DataExporter():
    """Class for exporting data. """

    date_info = datetime.now().strftime("%Y_%m_%d") + ".csv"
    headers = ['link', 'title', 'doc_type','facets', 'doi']

    def export_collection_as_csv(self, repository_query, collection_pid):
        """
        DataExporter Method returns individually selected collection in 
        form of CSV which includes fields for title and item link.
        Parameters:
            reposistory_query: ReposistoryQuery class
            collection_pid: collection pid
        """
        
        data = repository_query.get_json(collection_pid)
        records = repository_query.get_collection_data(data)
        
        collection_file = "noaa_collection_" + self.date_info
        with open(collection_file, 'w', newline='', encoding='utf-8') as fh:
            csvfile = csv.writer(fh, delimiter='|')
            csvfile.writerow(self.headers)
            for row in records:
                csvfile.writerow(row)


    def export_all_collections_as_csv(self, repository_query, repo_data):
        """
        DataExporter method creates a deduplicated title and link list of all 
        items in the IR.
        Parameters:
            repository_query: ReposistoryQuery class
            repo_data: RepositoryQuery get_all_repo_data method, which returns 
            JSON and then is looped through.
        """

        # calls api.get method  which call JSON API to retrieve all collections 
        collections_file = "noaa_collections_" + self.date_info
        deduped_collections_file = "noaa_collections_final_" + self.date_info     
        
        with open(collections_file, 'w', newline='', encoding='utf-8') as fh:
            csvfile = csv.writer(fh, delimiter='|')

            # loop through all reposistory data
            for collection in repo_data:
                # call RepositoryQuery method get_collection_data'
                records = repository_query.get_collection_data(collection)
                for row in records:
                    csvfile.writerow(row)

        # deduplicate files
        f = list(set(open(collections_file,encoding='utf-8').readlines()))
        f.insert(0,'|'.join(self.headers) + '\n')
        open(deduped_collections_file,'w', encoding='utf-8').writelines(f)
        os.remove(collections_file)


def check_url(url):
    """
    Check if url returns 200. Returns response, if not return
    message and quit program.
    Parameters:
        url: url string.
    """
    r = requests.get(url)
    if r.status_code != 200:
        return 'status code did not return 200'
        sys.exit(1)
    return r


def check_pid(collection_info, pid):
    """
    Checks to see if pid is a valid pid repo collection pid. Return 
    error message and exit program is value isn't valid; return pid passed in
    if value is valid.
    Parameters:
        pid: sting value
    """
    for collection_pid in collection_info.values():
        if pid == collection_pid:
            return pid
    return f'{pid} is not a valid pid'


if __name__ == "__main__":
    import csv
    # example
    q = RepositoryQuery()
    # de = DataExporter()
    #de.export_collection_as_csv(q,'4')
    # de.export_all_collections_as_csv(q,q.get_all_repo_data())
                
