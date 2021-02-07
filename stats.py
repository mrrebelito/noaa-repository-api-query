import sys, csv, os
from datetime import datetime
import requests
import pandas as pd

pd.options.display.max_rows = 10 


"""Class used to create statistical data."""

class StatsData():


    # class variables
    baseurl = 'https://repository.library.noaa.gov/fedora/export/download/collection/'

    #instance variables
    def __init__(self, *resource, pid_info=None):
        # pid info
        self.pid_info = pid_info

        if len(resource) == 1:
            self.resource = resource[0]
        else:
            self.resource = list(resource)


    def get_df(self,pid):
        """
        Gets all collection data. Returns a df

        Parameters:
            pid: collection pid.

        Returns:
            pandas DataFrame of collection data
        """

        url = self.baseurl + get_pid(pid, self.pid_info)
        r = check_url_status(url) # check url status
    
        df = get_dataframe_from_response(r, self.resource)

        df['PID'] = df['PID'].str.replace('noaa:', '')

        return df

    def facet_count_single(self,df):
        """
        Provides facet count for individual collections.

        Parameters:
            df: Pandas dataframe.
        """
        
        try:
            # ensures values are converted from strings from list
            df = convert_df_list_to_str(df, 'mods.sm_localcorpname').copy()

            split_df = split_facets(df, 'mods.sm_localcorpname')
    
            facet_count = pd.melt(split_df)['value'].value_counts()

        except TypeError:
            print('You must pass a DataFrame as an argumet')
            facet_count = None
        
        return facet_count


    def get_facet_pid_info(self,df):
        """
        Lists Single facet alongside PID. 

        Parameters:
            df: Pandas dataframe.

        """

        try:
            # ensures values are converted from strings from list
            df = convert_df_list_to_str(df, 'mods.sm_localcorpname').copy()

            split_df = split_facets(df, 'mods.sm_localcorpname')
    
            # joins split and df on index
            join_df = split_df.join(df['PID'])

            melt_df = me = pd.melt(join_df, id_vars='PID')

            melt_df = melt_df.drop(columns=['variable'])


        except TypeError:
            print('You must pass a DataFrame as an argumet')
            melt_df = None
        
        return melt_df[melt_df['value'].notnull()]

    
    def facet_count_multiple(self):
        """
        Provides facet counts for all collections.

        """

        df_list = []

        for pid in self.pid_info.values():
            df = self.get_df(pid)
            df_list.append(df)

        concated_dfs = pd.concat(df_list, sort=False)

        split_df = split_facets(concated_dfs, 'mods.sm_localcorpname')

        facet_count = pd.melt(split_df)['value'].value_counts()

        return facet_count


# functions used in Class methods
def normalize_doc_types(x):
    """
    Use as a Pandas custom function within get_collection data function
    Parameters:
        x: pandas column value.
    """
    x = x.lower()
    return x.title()
  

def check_if_list(x):
    """Pandas custom function to check if column values are list objects
    Parameters:
        x: pandas column value.
    """
    return isinstance(x, list)


def check_url_status(url):
    """
    Function to check url status. Exit script if status code not 200.
    Parameter:
        url: queried by requests.
    """

    r = requests.get(url)
    if r.status_code != 200:
        sys.exit(1)
    elif r.status_code == 200:
        return r


def get_dataframe_from_response(response, resource):
    """
    Use to request response object to retrieve json, then retrieve Pandas df.
    Called within 'get_df' StatsData method.
    Parameters:
        response: requests reponse object
        resource: pandas daframe column name
    """

    data = response.json()
    docs = data['response']['docs']

    df = pd.DataFrame(docs)

    # if there is more than one column, loop through
    # convert_df_list_to_str function
    if isinstance(resource,str):
        df = convert_df_list_to_str(df, resource)
    if isinstance(resource, list):
        for column in resource:
            df = convert_df_list_to_str(df, column)
    return df


def convert_df_list_to_str(df, resource):
    """
    Converts Pandas dataframe column values to strings. Called within
    'get_dataframe_from_response' function.
    Parameters:
        df: pandas dataframe.
        resource: dataframe column name.
    """

    # convert lists to strings using string join accessor
    if any(df[resource].apply(check_if_list)) == True:
        df[resource] = df[resource].str.join(',')

    
    if resource == 'mods.type_of_resource': 

        # new rows are created if there are more than document type
        # associated with a record
        if any(df[resource].str.contains(',')) == True:
            multi_docs = df[df[resource].str.contains(',')].copy()

            multi_docs[resource] = multi_docs[resource].str.split(',')
            one, two = multi_docs.copy(), multi_docs.copy()

            one[resource] =  one[resource].str[0]
            two[resource] =  two[resource].str[1]

            # delete multidocs
            df.drop(index=multi_docs.index, axis=1, inplace=True)

            df = df.append([one, two], ignore_index=True).copy()

        # function is only applicable for doc types
        df[resource] = df[resource].apply(normalize_doc_types)

    return df


def split_facets(df, resource):
    """
    Split facets into a their own separate columns.
    Parameters:
        df: pandas dataframe
        resource: IR 'mods.sm_localcorpname' column
    """

    # update delimiter between facets
    df[resource] = df[resource].str.replace(", "," ")
    df[resource] = df[resource].str.replace(",","; ") 

    return df[resource].str.split('; ',expand=True)


def get_pid(value, pid_info):
    """
    Returns pid collection value whether collection name or 
    pid number is entered.
    Paramters:
        value: collection name or pid number of collection
        pid_info: dict of collection names and associated pids
    """

    count, match = 0, ''
    while match == '':
        for k,v in pid_info.items():
            if value == k:
                match = pid_info[value]
            elif value == v:
                match = value

            count += 1
            if count > len(pid_info):
                raise ValueError('Invalid pid. Enter value from pid_info')
           
    return match


if __name__ == "__main__":
    s = StatsData('mods.sm_localcorpname',
        pid_info={'OAR': '7','NWS':'6', 'NESDIS':'9'})
  