import os, csv, sys, re, json, math
from itertools import accumulate, chain
from datetime import datetime
import requests

""" 
Classes used to query IR and export output:
- Fields
- RepositoryQuery
- DataExporter

"""


class RepositoryQuery():
    """Query class used to interact with NOAA Repository JSON API"""

    item_url = "https://repository.library.noaa.gov/view/noaa/"
    today = datetime.now().strftime('%Y-%m-%d')

    # dictionary containing NOAA Repository collections and associated PIDS
    pid_dict = { 
                "NOAA IR collection":"noaa",
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

    def __init__(self, fields):
        self.api_url =  "https://repository.library.noaa.gov/fedora/export/view/collection/"
        self.fields = fields
        self.pid = ''
        self.collection_data = []   
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

        
    def get_single_collection_json(self,pid):
        """
        IR collection dataset is queried via REST API.
        
        Multiple functions are utilized to generate information in order
        JSON, including: row_total and API URL(s). This info is
        passed into a function a final function collection_data which 
        returns JSON.

        Parameters: 
            pid: collection pid
        
        Returns:
            Response header and Documents from an IR collection in JSON.
        """
        
        self.pid = str(pid)

        full_url = self.api_url + check_pid(self.pid_dict, self.pid)
        row_total = get_row_total(self.api_url, self.pid) # function
        api_url_info = iterate_rows(self.api_url, self.pid, row_total)

        # call concat_json function
        self.collection_data = concat_json(api_url_info, full_url, self.date_params)


    def get_all_collections_json(self):
        """
        Entire IR dataset is queried via REST API.

        'noaa' serves as the endpoint for entire IR collection.

        Multiple functions are utilized to generate information in order
        JSON, including: row_total and API URL(s). This info is
        passed into a function a final function collection_data which 
        returns JSON.

        Returns:
            Resonse header and entire Dataset collection in JSON.
        """

        all_ir_json = 'noaa'
        full_url = f'{self.api_url}{all_ir_json}'
        row_total = get_row_total(self.api_url, all_ir_json) # function
        api_url_info = iterate_rows(self.api_url, all_ir_json, row_total)

        # call concat_json function
        self.collection_data = concat_json(api_url_info, full_url, self.date_params)
        

    def filter_on_fields(self):
        """
        Filters JSON based on fields list passed into function.        

        Returns:
            Documents of from an IR collection in JSON
        """

        filtered_data = []

        for doc in self.collection_data:
            filtered_data.append(
                field_iterator(doc, self.fields))
             
        self.collection_data = filtered_data


    def convert_multivals_to_one(self, field, delimiter='~'):
        """
        Converts multivalued column values into a single value, 
        generating a new row, carrying over associated value to 
        newly created row.

        Default delimiter is a tilda symbol.

        Parameters:
            field: collection data field

        Returns: 
            Updates RepositoryQuery collection_data attribute
            with new values.
        """

        data = []

        for item in self.collection_data:
            if ';' in item[field]:
                for multi_item in item[field].split(delimiter):
                    data.append({
                        'PID': item['PID'],
                        field : multi_item
                        })
            else:
                data.append({
                        'PID': item['PID'],
                        field: item[field]
                        })

        # remove entries where fields equal ''    
        data = [x for x in data if x[field] != '']

        self.collection_data = data


    def search_field(self, field, search_value):
        """ 
        Search on collection data. 

        Collection data must already be pull and stored in 
        collection data instance variable. Exception will be thrown if not.

        Simple search is performed on selected field. 
        Search is converted to lower lower as is field to be searched on.

        Parameters:
            field: field to be searched on
            search_value: value that searches against field

        Returns:
            list of dicts.
        """
        
        if len(self.collection_data) == 0:
            raise Exception('No Collection data present. Make sure to pull data (single collection or entire dataset)')

        result_list = []

        for record in self.collection_data:
            try:
                if search_value.lower() in record[field].lower():
                    result_list.append(record)
            except KeyError:
                raise Exception('field not present. Check your RepositoryQuery instance fields')

        return result_list     


class DataExporter():
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
        
        data = repository_query.get_single_collection_json(collection_pid)
        records = repository_query.filter_on_fields()

        collection_full_path = os.path.join(export_path, col_fname)

        delimiter = '\t'
        write_dict_list_to_csv(records,collection_full_path,
            delimiter, repository_query.fields)


    def export_all_collections_as_csv(self, repository_query, all_ir_data,
        export_path='.'):
        """ 
        Creates a unique file of all its in the IR.

        List of Python Dictionaries written to CSV.

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

        data = repository_query.get_all_collections_json()
        records = repository_query.filter_on_fields()

        # calls api.get method  which call JSON API to retrieve all collections 
        collections_file = "noaa_collections_" + self.date_info
        collections_full_path = os.path.join(export_path, collections_file)
        deduped_collections_file = "noaa_collections_final_" + self.date_info
        deduped_collections_full_path = os.path.join(export_path,
            deduped_collections_file)     
        
        delimiter = '\t'
        write_dict_list_to_csv(records, collections_full_path, 
            delimiter, repository_query.fields)

        # deduplicate files
        f = list(set(open(collections_full_path,encoding='utf-8').readlines()))
        f.insert(0,delimiter.join(repository_query.fields) + '\n')
        open(deduped_collections_full_path,'w', encoding='utf-8').writelines(f)
        os.remove(collections_full_path)


def field_iterator(json_data, fields):
    """
    Helper function. 

    Return dict
    """

    data_dict = {}

    delimiter = '~'
    for field in fields:
        if json_data.get(field) is None:
            data_dict.update({field: ''})
        elif isinstance(json_data.get(field), list): 
            # delimter
            data_dict.update({field: clean_text(delimiter.join(json_data.get(field)))})
        else:
            data_dict.update({field: clean_text(json_data.get(field))})

    return data_dict


def make_request(url,params=None):
    """
    Make request. Check for 200 status code. If not exit
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


def get_row_total(api_url, pid):
    """
    Get row total from collection. 

    Returns row total for each collection, 
    including entire NOAA IR collection.
    """

    r = requests.get(f'{api_url}{pid}')
    if r.status_code != 200:
        return 'status code did not return 200'
        sys.exit(1)
    data = r.json()
    return data['response']['numFound']


def iterate_rows(api_url, col_pid, total, row_num=5000): 
    """
    If total number of rows is less than 
    chunk val a list of URLS is generated with 
    a num appended with a query string
    """

    # append collection col_pid to api_url
    url = f'{api_url}{col_pid}'

    if total < row_num:
        return url
    else:
        chunk_array = split_equal(total, row_num)
        # insert 0 at beginning of list
        chunk_array.insert(0,0) 
        cumsum_chunk_array = list(accumulate(chunk_array))

        chunk_link_array = []
        for chunk in cumsum_chunk_array:
            if chunk != total:
                chunk_link_array.append(
                    f'{url}?rows={str(row_num)}&start={str(chunk)}')
                continue
        return chunk_link_array

    
def split_equal(total, row_num):
    """
    Helper function for iterate_rows function
    """
    li = [row_num] * math.floor((total / row_num))
    return li


def concat_json(api_url_info, full_url, date_params=None):
    """
    Function utilized to handle multiple or single api URL requests.

    If multiple API URL requests are occur, requests are made
    using list comprehensions resulting in lists of dicts. Lists
    are combined using itertools chain. 

    If single API request is made, only list of dicts is returned.

    Returns:
        list of IR records. Response header is removed in the process. 
        Neccessary for concating JSON.  
    """
    
    # if api_url_info contains multiple links are present
    if isinstance(api_url_info, list):
        r = [make_request(url, params=date_params) for url in api_url_info]
        data = [x.json() for x in r]
        docs = [x['response']['docs'] for x in data]
        #use itertools chain to concat lists together
        docs = list(chain(*docs))
    
    # if a single link is present
    elif isinstance(api_url_info, str):
        r = make_request(full_url, params=date_params)
        data = r.json()
        docs = data['response']['docs']

    return docs


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


def clean_text(text):
    """
    Clean text data.
    """
    text = text.replace('\n', '').replace('\r','')
    return text


def write_dict_list_to_csv(dict_li,file_path, delimiter, fieldnames):
    """
    Write Python dict list to CSV

    Parameters:
        dict_li: Python list of dictionaries
        file_path: abs or relative file path. use to save CSV
        delimiter: choose type of delimiter
        fieldnames: function utilizes csv.DictWriter. Currently
        written to require header.
    """

    with open(file_path, 'w',
        newline='', encoding='utf-8') as fh:
        
        # list of dictionaries written to CSV
        csvfile = csv.DictWriter(fh,
            delimiter=delimiter,
            fieldnames=fieldnames
            )

        csvfile.writeheader()
        csvfile.writerows(dict_li) 


if __name__ == "__main__":
    fields = [ 'PID', 'mods.title','mods.type_of_resource',
    'fgs.createdDate','mods.sm_digital_object_identifier',
    'mods.related_series','mods.ss_publishyear', 'mods.sm_localcorpname']
    q = RepositoryQuery(fields)
    de = DataExporter()