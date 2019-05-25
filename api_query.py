import requests

""" 
Module used to query NOAA Institutional Repository.

Contains Query class which will return IR collection data in the 
following ways: 
1) individual collections via JSON
2) individual collections (title and item link) via list of lists
3) all collections via JSON
"""


class Query:
    """Query class used to interact with NOAA Repository JSON API"""

    url = "https://repository.library.noaa.gov/fedora/export/view/collection/"
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


    def query_collection(self,pid):
        """
        Collection is queried twice. First query returns row number.
        Second query utilizes row information to return url
        to retrieve all rows.
        """
        #first query
        full_url = self.url + pid
        r = requests.get(full_url)
        status = r.status_code 
        if status != 200:
            return 'Request not successful. Try again'
        json_data = r.json()
        rows = json_data['response']['numFound']
        
        #second query
        full_url = self.url + pid + '?rows=' + str(rows)
        r = requests.get(full_url)
        status = r.status_code
        if status != 200:
            return 'Request not successful. Try again' 
        json_data = r.json()

        return json_data


    def query_collection_by_title_and_link(self,collection):
        """
        Individual collection is iterated over to return
        title and item link in form of list of lists.
        """
        title_list = []
        for row in collection['response']['docs']:
            try:
                title = row['mods.title']
                title_list.append(title)
            except KeyError:
                title = ['No title, check IR']
                title_list.append(title)
            
        PID = [row['PID'] for row in collection['response']['docs']]
        link = [record.replace('noaa:',\
            'https://repository.library.noaa.gov/view/noaa/')\
            for record in PID]

        title_link = []
        for t,l in zip(title_list, link):
            title_link.append([t, l])

        return title_link


    def get_collections(self):
        """ 
        Get all collection data from IR using collection name and PID 
        dictionary. 
        
        Utilize generator function to call function to
        retrieve all IR collections.
        
        Collection data is returned in JSON format."""
        for collection in self.pid_dict.values():
            yield self.query_collection(collection)  

        
if __name__ == "__main__":
    # example
    q = Query()
    pid = q.get_collection_pid('NOAA International Agreements')
    print(pid)
    collection = q.query_collection(pid)
