import sys
sys.path.insert(0, '..')

import requests

from bs4 import BeautifulSoup#, NavigableString

import pandas as pd 

import lxml
import lxml.html
import lxml.etree

from seoworkflows_lib.bs4_scrapers.UserAgents import GET_UA

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












#         for i, r in filtered_df.iterrows():
#             search_query = r.to_dict()['query']
#             search_domain = r.to_dict()['domain']
#             search_page = r.to_dict()['page']
#             search_gsc_dataset_id = r.to_dict()['gsc_datasetID']
#             # find all occurrences for the query term 
#             # in the Beautifulsoup parsed page content
#             occ = bs.find_all(text=lambda x: x and search_query in x.lower())
#             occ_text = []
#             # if we have found at least one occurrence of the query term 
#             # we just check the Beautifulsoup item class to manage correctly
#             # the text where each occurrence has been found
#             if len(occ) > 0:
#                 for o in occ:
#                     if isinstance(o, NavigableString):
#                         occ_text.append(str(o))
#                     else:
#                         occ_text.append(o.text)
                
#                 crawl_res.append(dict(
#                     query=search_query,  # query term we have found in the page content
#                     domain=search_domain,  # domain from gsc
#                     page_crawl=search_page,  # page from gsc we have scraped
#                     gsc_datasetID=search_gsc_dataset_id,  # dataset id from gsc
#                     text = occ_text,  # list of texts where we have found the query term
#                     occurrences = len(occ_text)  # number of occurrences for the query term
#                 ))

# # transform the list of dictionaries into a dataframe to be able to work with the exisiting dataframes
# crawl_df = pandas.DataFrame([c for c in crawl_res])


# output = joined_df.merge(crawl_df, how="left", left_on=["gsc_datasetID", "domain","page","query"], right_on=["gsc_datasetID", "domain","page_crawl","query"])


# select_cols_final = ["gsc_datasetID","domain","page","query","clicks_sum","impressions_sum", 
#         "position_size","position_max","position_min","position_mean","page_crawl","text","occurrences"]

# output = output[select_cols_final]
# output = output.rename(
#     mapper={
#         "gsc_datasetID": "gsc_datasetID",
#         "domain": "domain",
#         "page": "page_gsc",
#         "query": "query_gsc",
#         "clicks_sum": "clicks_sum_gsc",
#         "impressions_sum": "impressions_sum_gsc",
#         "ctr_mean": "ctr_mean_gsc",
#         "position_size": "count_instances_gsc",
#         "position_max": "position_max_gsc",
#         "position_min": "position_min_gsc",
#         "position_mean": "position_mean_gsc",
#         "page_crawl": "page_crawl",
#         "text": "text_crawl",
#         "occurrences": "occurrences_crawl"
#     }, axis="columns")

# output = output.fillna("Not in scraped text")