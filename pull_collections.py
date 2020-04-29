import re
import requests
from lxml import html


def get_collections():
    """
    Generate collection title, pid dict using Python requests,
    lxml libraries.
    """

    url = "https://repository.library.noaa.gov/browse/collections"
    r = requests.get(url)
    doc = html.fromstring(r.content)
    
    #retrieve collection titles
    titles = doc.xpath('//div[@class="collections-title"]/a/text()')
    titles = [x.strip() for x in titles]
    titles = [x for x in titles if x is not '']

    # retrieve collection pids
    pid_links = doc.xpath('//div[@class="collections-title"]/a/@href')
    pids = [re.findall(r'[0-9]{1,5}$',x)[0] for x in pid_links]
    

    pid_dict = dict(zip(titles, pids))

    return pid_dict

print(get_collections())