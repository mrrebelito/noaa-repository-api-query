import os, csv, sys, re
from datetime import datetime
import requests

""" 
Classes used to query IR and export output:
- Fields
- RepositoryQuery
- DataExporter

"""

class Fields():

    fields = [ 'PID', 'mods.title','mods.type_of_resource',
    'mods.ss_publishyear','mods.sm_digital_object_identifier',
    'fgs.createdDate']
    
    def append_field(self, value):
        """Append a single string value to list"""
        if not isinstance(value, str):
            print('Value is not a string. Try again.')
        else:
            self.fields.append(value)


class RepositoryQuery(Fields):
    """Query class used to interact with NOAA Repository JSON API"""

    item_url = "https://repository.library.noaa.gov/view/noaa/"
    today = datetime.now().strftime('%Y-%m-%d')

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
                "NOAA Cooperative Institutes": "23649",
                "Cooperative Science Centers": "24914"
            }

    def __init__(self):
        super().__init__() # inherit fields
        self.pid = ''
        self.collection_data = []   
        self.all_collection_data = []
        self.api_url =  "https://repository.library.noaa.gov/fedora/export/download/collection/"
        self.date_params = {}

    
    def date_filter(self, from_d, until_d=today):
        """
        Update api_url attribute to provide a date filter.

        From date parameter is required. Until isn't. If until
        is not entered, the current date generated via datetime.

        'YYYY-MM-DD' sting is the required format. 

        Parameters:
            from_d: from date, required
            until_d: end date, optional. 

        Returns: 
            updated api_url with date filter.
        """
        
        #validate dates
        from_d, until_d = date_param_format(from_d), date_param_format(until_d)

        from_d = f'{from_d}T00:00:00Z'
        until_d = f'{until_d}T00:00:00Z'

        self.date_params = {
            'from': from_d,
            'until': until_d
            }

        return self.date_params 

        
    def get_collection_json(self,pid):
        """
        IR collection is queried via REST API.
        
        Parameters: 
            pid: collection pid
        
        Returns:
            Response header and Documents from an IR collection in JSON.
        """
        # set instance attribute, automatically convert ints
        self.pid = str(pid)

        full_url = self.api_url + check_pid(self.pid_dict, self.pid)
        r = check_url(full_url, params=self.date_params) # helper function 
        return r.json()
        
    
    def filter_collection_data(self, json_data):
        """
        Filters JSON based on fields list passed into function.

        Parameters:
            json_data: JSON data of collection

        Returns:
            Documents of from an IR collection in JSON
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
        Get data all collection data in IR.
        
        Utilize generator function to call function to
        retrieve all IR collections.
        
        Returns:
            All collection data in JSON.
        """
        for collection in self.pid_dict.values():
            yield self.get_collection_json(collection)  


class DataExporter(Fields):
    """Class used to export data."""

    date_info = datetime.now().strftime("%Y_%m_%d") + ".csv"
    col_fname = "noaa_collection_" + date_info

    def export_collection_as_csv(self, repository_query, collection_pid,
        export_path='.', col_fname=col_fname):
        
        """
        Export single collection in a CSV.

        Parameters:
            reposistory_query: ReposistoryQuery class instance
            collection_pid: collection pid value
            export_path: path to download collection file to. Default is
                set to current working directory
            col_fname: DataExporter class attribute ued as 
                keyword default param 

        Returns:
            CSV of a single IR collection.
        """
        # creates directory if it doesn't exists
        make_dir(export_path)
        
        data = repository_query.get_collection_json(collection_pid)
        records = repository_query.filter_collection_data(data)
        
        with open(
            os.path.join(export_path,col_fname),'w', newline='',
            encoding='utf-8') as fh:

            csvfile = csv.writer(fh,
                delimiter='|',
                quoting=csv.QUOTE_NONE,
                quotechar='')

            csvfile.writerow(self.fields)
            for row in records:
                csvfile.writerow(row)


    def export_all_collections_as_csv(self, repository_query, all_ir_data,
        export_path='.'):
        """
        Creates a deduplicated title and link list of all 
        items in the IR.

        Parameters:
            repository_query: ReposistoryQuery class instance
            all_ir_data: RepositoryQuery get_all_ir_data method, which returns 
            JSON and then is looped through.
            export_path: path to download collection file to. Default is
                set to current working directory
        
        Returns:
            CSV of all IR collections.        
        """
        # creates directory if it doesn't exists
        make_dir(export_path)

        # calls api.get method  which call JSON API to retrieve all collections 
        collections_file = "noaa_collections_" + self.date_info
        collections_full_path = os.path.join(export_path, collections_file)
        deduped_collections_file = "noaa_collections_final_" + self.date_info
        deduped_collections_full_path = os.path.join(export_path,
            deduped_collections_file)     
        
        with open(collections_full_path, 'w',
            newline='', encoding='utf-8') as fh:
            
            csvfile = csv.writer(fh,
                delimiter='|',
                quoting=csv.QUOTE_NONE,
                quotechar='')

            # loop through all reposistory data
            for collection in all_ir_data:
                # call RepositoryQuery method filter_collection_data'
                records = repository_query.filter_collection_data(collection)
                for row in records:
                    csvfile.writerow(row)

        # deduplicate files

        f = list(set(open(collections_full_path,encoding='utf-8').readlines()))
        f.insert(0,'|'.join(self.fields) + '\n')
        open(deduped_collections_full_path,'w', encoding='utf-8').writelines(f)
        os.remove(collections_full_path)


def transform_json_data(json_data, field):
    """
    Transform JSON data into a list. 
    
    A Helper function used in combination with 'filter_collection_data' 
    by passing into the said function as an argument.

    Parameters:
        json_data: JSON collection data
        field: field from api

    Returns:
        List of collection data.
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


def check_url(url,params=None):
    """
    Check URL if it returns 200. If not exits
    script with sys.exit.  
    
    Parameters:
        url: api url string.
    
    Returns:
        Returns response, if not returns
        message and quit program.
    """
    r = requests.get(url,params=params)
    if r.status_code != 200:
        return 'status code did not return 200'
        sys.exit(1)
    return r


def check_pid(collection_info, pid):
    """
    Checks to see if pid is a valid pid repo collection pid.

    Parameters:
        pid: sting value

    Returns:
        Error message and exit program is value isn't valid; pid 
        passed in if value is valid.
    """
    for collection_pid in collection_info.values():
        if pid == collection_pid:
            return pid
    return f'{pid} is not a valid pid'


def make_dir(filepath):
    """
    Creates directory in current working
    directory if it doesn't exists

    Paramaters:
        filepath: filepath
    
    """
    if os.path.exists(filepath) == False:
        os.mkdir(filepath)


def date_param_format(date):
    """
    Check if date param format is valid.

    format: 'YYYY-MM-DDT00:00:00Z'
    
    """
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    finally:
        return date


if __name__ == "__main__":
    import csv
    # example
    
    q = RepositoryQuery()
    
    
    de = DataExporter()
    # de.export_collection_as_csv(q,'3')