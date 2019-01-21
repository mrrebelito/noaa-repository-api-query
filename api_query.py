import requests

class Query:
    """Query class used to interact with NOAA Repository JSON API"""

    url = "https://repository.library.noaa.gov/fedora/export/view/collection/"
    # dictionary containing NOAA Repository collections and associated PIDS
    pid_dict = { "National Environmental Policy Act (NEPA)" : "3",
                "Deepwater Horizon Materials" : "3",
                "Coral Reef Conservation Program (CRCP)" : "3",
                "Ocean Exploration Program" : "4",
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
        for key in self.pid_dict.keys():
            if key == collection:
                self.pid = self.pid_dict[key]
                return self.pid  

    def query_collection(self,pid):
        """Query collection twice. With first query, query without rows for
        the purpose of retrieving row number from requestHeader.
        With second query add row information to url to retrieve all rows.
        """
        #first query
        full_url = self.url + pid + '?rows=0'
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
        try:
            title = [row['mods.title'] for row in collection['response']['docs']]
        except:
            title = "No title listed"

        PID = [row['PID'] for row in collection['response']['docs']]
        link = [record.replace('noaa:',\
            'https://repository.library.noaa.gov/view/noaa/') for record in PID]

        title_link = []
        for t,l in zip(title, link):
            title_link.append([t, l])

        return title_link

    def query_all_collections(pid):
        """ Select all collections"""
        pids = self.pid_dict.values()
        for pid in pids:
            collections = query_collection(pid)
            collections['response']['docs']

if __name__ == "__main__":
    q = Query()
    pid = q.get_collection_pid('NOAA International Agreements')
    print(pid)
    collection = q.query_collection(pid)

    records = q.query_collection_by_title_and_link(collection)
    for record in records:
         print(record[1])
