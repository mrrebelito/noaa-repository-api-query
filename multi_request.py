import sys, math, json
from itertools import accumulate
import requests



def get_row_total(api_url, pid):

    url = "https://repository.library.noaa.gov/fedora/export/view/collection/"
    r = requests.get(f'{url}{pid}')
    if r.status_code != 200:
        return 'status code did not return 200'
        sys.exit(1)
    data = r.json()
    return data['response']['numFound']


def iterate_rows(api_url, col_info, total, chunk_val=1500): 
    """
    If total number of rows is less than 
    chunk val a list of URLS is generated with 
    a num appended with a query string
    """

    # append collection pid to api_url
    url = f'{api_url}{col_info}'

    if total < chunk_val:
        return url
    else:
        chunk_array = split_equal(total, chunk_val)
        # insert 0 at beginning of list
        chunk_array.insert(0,0) 
        cumsum_chunk_array = list(accumulate(chunk_array))

        chunk_link_array = []
        for chunk in cumsum_chunk_array:
            if chunk != total:
                chunk_link_array.append(
                    f'{url}?rows={str(chunk_val)}&start={str(chunk)}')
                continue
        return chunk_link_array


def split_equal(total, chunk_val):
        li = [chunk_val] * math.floor((total / chunk_val))
        return li


if __name__ == "__main__":
    with open('json_files/three.json', 'r') as f:
        three = json.load(f)

    with open('json_files/four.json','r') as f:
        four = json.load(f)

    three_docs = three['response']['docs']
    four_docs = four['response']['docs']