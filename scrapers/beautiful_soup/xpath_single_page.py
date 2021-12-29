import sys
sys.path.insert(0, '..')

import requests

from bs4 import BeautifulSoup#, NavigableString

import pandas as pd 

import lxml
import lxml.html
import lxml.etree

from seoworkflows_lib.scrapers.UserAgents import GET_UA

def xpath_single_page(urls, xpath_selector=None):
    
    crawl_data = []

    for row in urls['URL']:

        USER_AGENT = GET_UA()
        headers = {'user-agent': USER_AGENT}
        print(row)

        resp = requests.get(row, headers=headers)

        # if page is live
        if resp.status_code == 200:
            
            # check if xpath exists
            if xpath_selector is not None and xpath_selector != '':
                tree = lxml.html.fromstring(resp.content)
                # Get element using XPath 
                xpath_selection_content = tree.xpath(xpath_selector)
                selected_content = b'\n'.join([lxml.etree.tostring(elem) for elem in xpath_selection_content])
                bs = BeautifulSoup(selected_content, "html.parser")
            
            # else extract entire pages content
            else:
                bs = BeautifulSoup(resp.content, "html.parser")
            
            output = [row, bs]

            crawl_data.append(output)
    
    # crawl_data currently returns a list of lists - convert to df
    df = pd.DataFrame(crawl_data, columns = ['url', 'crawl_data'])

    return df




