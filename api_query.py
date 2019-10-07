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


    def get_json(self,pid):
        """
        Collection is queried twice. First query returns row number.
        Second query utilizes row information to return url
        to retrieve all rows.

        Cehcks status of requests both times. 
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


    def query_collection(self,collection):
        """
        Individual collection is iterated over to return
        title and item link in form of list of lists.
        """
        title_link = []
        for row in collection['response']['docs']:
            link = row['PID'].replace('noaa:', self.url)
            try:
                title = row['mods.title']
            except KeyError:
                title = ''
            
            title_link.append([link,title])
            
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
    import csv
    # example
    q = Query()
    pid = q.get_collection_pid('NOAA International Agreements')
    data = q.get_collections()
    # count = 0
    # with open('IR-collections.csv','w',newline='') as f:
    #     fh = csv.writer(f,delimiter='|')
    #     fh.writerow(["Collection Number", "Title", "Link","PID"])
    #     for collection in data:
    #         title_link = q.query_collection(collection)
    #         count += 1
    #         for t,l in title_link:
    #             pi = l.replace('https://repository.library.noaa.gov/view/noaa/','')
    #             fh.writerow([str(count),t,l,pi])

                
