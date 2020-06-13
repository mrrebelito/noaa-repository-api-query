import os, csv
from datetime import datetime
import requests

""" 
Classes used to query IR and export output:
- Fields
- RepositoryQuery
- DataExporter

"""

class Fields():

    fields = [ 'PID', 'mods.title', 'mods.type_of_resource',
    'mods.sm_localcorpname', 'mods.sm_digital_object_identifier']
    
    def append_field(self, value):
        """Append a single string value to list"""
        if not isinstance(value, str):
            print('Value is not a string. Try again.')
        else:
            self.fields.append(value)


class RepositoryQuery(Fields):
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
        super().__init__() # inherit fields
        self.pid = ''
        self.collection_data = []   
        self.all_collection_data = []

    def get_collection_json(self,pid):
        """
        Collection is queried via API, returning JSON.
        Parameters: 
            pid: collection pid
        """
        # set instance attribute, automatically convert ints
        self.pid = str(pid)

        full_url = self.api_url + check_pid(self.pid_dict, self.pid)
        r = check_url(full_url)
        return r.json()
        
    
    def filter_collection_data(self, json_data):
        """
        Dynamically filters json based on fields list passed into function.
        Parameters:
            json_data: JSON data of collection
        """

        all_field_data = []

        # fields is an attribute
        for field in self.fields:
            # call transform_json_data function
            all_field_data.append(transform_json_data(json_data, field))
                        
        self.collection_data = []

        while all_field_data: # loop though field data until list is 0

            interleaved_field_data = []

            try:
                for item in all_field_data:
                    field = item.pop()
                    interleaved_field_data.append(field)
            except IndexError: # silences error caused by list running out
                break

            self.collection_data.append(interleaved_field_data)

        return self.collection_data


    def get_all_ir_data(self):
        """
        Get data all collections in IR using collection name and PID 
        dictionary. 
        
        Utilize generator function to call function to
        retrieve all IR collections.
        
        Collection data is returned in JSON format.
        """
        for collection in self.pid_dict.values():
            yield self.get_collection_json(collection)  


class DataExporter(Fields):
    """Class for exporting data. """

    date_info = datetime.now().strftime("%Y_%m_%d") + ".csv"

    def export_collection_as_csv(self, repository_query, collection_pid):
        """
        DataExporter Method returns individually selected collection in 
        form of CSV which includes fields for title and item link.
        Parameters:
            reposistory_query: ReposistoryQuery class
            json_data: json_data from a collection
            fields: instance attribute
        """
        
        data = repository_query.get_collection_json(collection_pid)
        records = repository_query.filter_collection_data(data)
        
        collection_file = "noaa_collection_" + self.date_info
        with open(collection_file, 'w', newline='', encoding='utf-8') as fh:
            csvfile = csv.writer(
                fh,
                delimiter='|',
                quoting=csv.QUOTE_NONE,
                quotechar=''
                )
            csvfile.writerow(self.fields)
            for row in records:
                csvfile.writerow(row)


    def export_all_collections_as_csv(self, repository_query, all_ir_data):
        """
        DataExporter method creates a deduplicated title and link list of all 
        items in the IR.
        Parameters:
            repository_query: ReposistoryQuery class
            all_ir_data: RepositoryQuery get_all_ir_data method, which returns 
            JSON and then is looped through.
        """

        # calls api.get method  which call JSON API to retrieve all collections 
        collections_file = "noaa_collections_" + self.date_info
        deduped_collections_file = "noaa_collections_final_" + self.date_info     
        
        with open(collections_file, 'w', newline='', encoding='utf-8') as fh:
            csvfile = csv.writer(
                fh,
                delimiter='|',
                quoting=csv.QUOTE_NONE,
                quotechar=''
                )

            # loop through all reposistory data
            for collection in all_ir_data:
                # call RepositoryQuery method filter_collection_data'
                records = repository_query.filter_collection_data(collection)
                for row in records:
                    csvfile.writerow(row)

        # deduplicate files
        f = list(set(open(collections_file,encoding='utf-8').readlines()))
        f.insert(0,'|'.join(self.fields) + '\n')
        open(deduped_collections_file,'w', encoding='utf-8').writelines(f)
        os.remove(collections_file)


def transform_json_data(json_data, field):
    """
    Transform JSON data from collection into a list. Helper function used in
    combination with 'filter_collection_data' by passing into the said
    function as an argument.
    json_data: JSON collection data
    field: 
    """

    filtered_data = []

    docs = json_data['response']['docs'] 

    for doc in docs:
        try:
            # if field is instance join with semicolon
            if isinstance(doc[field], list):
                filtered_data.append(';'.join(doc[field]))
            else:
                # remove new lines, carriage returns and noaa:
                doc[field] = doc[field].replace('\n','').replace('\r', '')
                doc[field] = doc[field].replace('noaa:','')
                filtered_data.append(doc[field])
        except KeyError:
            doc[field] = ''
            filtered_data.append(doc[field])

    return filtered_data


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
    
    
    de = DataExporter()
    # de.export_collection_as_csv(q,'3')
    de.export_all_collections_as_csv(q,q.get_all_ir_data())
                
