import requests

class Query:
    url = "https://repository.library.noaa.gov/fedora/export/view/collection/"

    def __init__(self):
        self.pid = ''

    def get_collection_pid(self, collection):
        if collection == "National Environmental Policy Act (NEPA)":
            self.pid = '1'
            return self.pid
        elif collection == "Deepwater Horizon Materials":
            self.pid = '2'
            return self.pid
        elif collection == "Coral Reef Conservation Program (CRCP)":
            self.pid = '3'
            return self.pid
        elif collection == "Ocean Exploration Program":
            self.pid = '4'
            return self.pid
        elif collection == "National Marine Fisheries Service (NMFS)":
            self.pid = '5'
            return self.pid
        elif collection == "National Weather Service (NWS)":
            self.pid = '6'
            return self.pid
        elif collection == "Office of Oceanic and Atmospheric Research (OAR)":
            self.pid = '7'
            return self.pid
        elif collection == "National Ocean Service (NOS)":
            self.pid = '8'
            return self.pid
        elif collection == "National Environmental Satellite and Data Information Service (NESDIS)":
            self.pid =  '9'
            return self.pid
        elif collection == "Sea Grant Publications":
            self.pid = '11'
            return self.pid
        elif collection == "Education and Outreach":
            self.pid = '12'
            return self.pid
        elif collection == "NOAA General Documents":
            self.pid = '10031'
            return self.pid
        elif collection == "NOAA International Agreements":
            self.pid = '11879'
            return self.pid  
        elif collection == "Office of Marine and Aviation Operations (OMAO)":
            self.pid = '16402'
            return self.pid    

    def query_collection(self,pid):
        """Query collection twice. With first query, query without rows for
        the purpose of retrieving row information. With second query add row information
        to url to retrieve all rows
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
        title = [row['mods.title'] for row in collection['response']['docs'] if not None]
        PID = [row['PID'] for row in collection['response']['docs']]
        link = [record.replace('noaa:',\
            'https://repository.library.noaa.gov/view/noaa/') for record in PID]

        title_link = []
        for t,l in zip(title, link):
            title_link.append([t, l])

        return title_link

if __name__ == "__main__":
    q = Query()
    pid = q.get_collection_pid('NOAA International Agreements')
    print(pid)
    collection = q.query_collection(pid)

    records = q.query_collection_by_title_and_link(collection)
    for record in records:
         print(record[1])
