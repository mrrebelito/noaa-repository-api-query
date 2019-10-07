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

class RepositoryQuery:
    """Query class used to interact with NOAA Repository JSON API"""

    api_url = "https://repository.library.noaa.gov/fedora/export/view/collection/"
    item_url = "https://repository.library.noaa.gov/view/noaa/"

    # dictionary containing NOAA Repository collections and associated PIDS
    pid_dict = { 
                "National Environmental Policy Act (NEPA)" : "1",
                "Deepwater Horizon Materials (DWH)" : "2",
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
                "Office of Marine and Aviation Operations (OMAO)" : "16402"
            }

    def __init__(self):
        self.pid = ''

    def get_collection_pid(self, collection):
        """
        Taking collection name as argument, returns collection's
        pid.
        """
        for key in self.pid_dict.keys():
            if key == collection:
                self.pid = self.pid_dict[key]
                return self.pid


    def get_json(self,pid):
        """
        Collection is queried twice. First query returns row number.
        Second query utilizes row information to return url
        to retrieve all rows.

        Cehcks status of requests both times. 
        """
        #first query
        full_url = self.api_url + pid
        r = requests.get(full_url)
        status = r.status_code 
        if status != 200:
            return 'Request not successful. Try again'
        json_data = r.json()
        rows = json_data['response']['numFound']
        
        #second query
        full_url = self.api_url + pid + '?rows=' + str(rows)
        r = requests.get(full_url)
        status = r.status_code
        if status != 200:
            return 'Request not successful. Try again' 
        json_data = r.json()

        return json_data


    def get_collection(self,collection):
        """
        Individual collection is iterated over to return
        title and item link in form of list of lists.
        """
        title_link = []
        for row in collection['response']['docs']:
            link = row['PID'].replace('noaa:', self.item_url)
            try:
                title = row['mods.title']
            except KeyError:
                title = ''
            
            title_link.append([link,title])
            
        return title_link


    def get_all_collections(self):
        """ 
        Get all collection data from IR using collection name and PID 
        dictionary. 
        
        Utilize generator function to call function to
        retrieve all IR collections.
        
        Collection data is returned in JSON format."""
        for collection in self.pid_dict.values():
            yield self.get_json(collection)  


class DataExporter():

    data_info_json = datetime.now().strftime("%Y_%m_%d") + ".json"
    date_info_csv = datetime.now().strftime("%Y_%m_%d") + ".csv"

    def export_collection(self, repository_query, collection):
        """
        Method returns individually selected collection in form of CSV
        which includes fields for title and item link.

        params include ReposistoryQuery class, collection pid (accessed
        through ReposistoryQuery .pid method)
        """
        
        data = repository_query.get_json(collection)
        title_link = repository_query.get_collection(data)
        
        csvfile = "noaa_collection_" + self.date_info_csv
        with open(csvfile, 'w', newline='') as f:
            fh = csv.writer(f, delimiter='\t')
            fh.writerow(["Title", "Link"])
            for t,l in title_link:
                fh.writerow([t,l])

        print("")
        print("CSV file created: " + csvfile)


    def export_all_collections(self, repository_query, collection_data):
        """
        Method creates a deduplicated title and link list of all items in 
        the IR.

        params include ReposistoryQuery class, and RepositoryQuery 
        .get_all_collections method
        """
        # calls api.get method which call JSON API to retrieve all collections 
        csvfile = "noaa_collections_" + self.date_info_csv
        deduped_csvfile = "noaa_collections_final_" + self.date_info_csv     
        with open(csvfile, 'w', newline='') as fw:
            fh = csv.writer(fw, delimiter='\t')
            for collection in collection_data:
                # call RepositoryQuery 
                title_link = repository_query.get_collection(collection)
                for t,l in title_link:
                    fh.writerow([t,l])

        # deduplicate files
        f = set(open(csvfile).readlines())
        open(deduped_csvfile,'w').writelines(f)
        os.remove(csvfile)

        print("")
        print("CSV file created: " + deduped_csvfile)        



if __name__ == "__main__":
    import csv
    # example
    q = RepositoryQuery()
    pid = q.get_collection_pid('NOAA International Agreements')
    de = DataExporter()
    # de.export_collection(q, q.pid)
    de.export_all_collections(q,q.get_all_collections())
                
