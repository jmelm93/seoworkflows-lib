import sys
sys.path.insert(0, '..')

import time
import pandas as pd 
import logging

urls = pd.read_csv('./test_files/4000_unique_urls.csv')

starttime = time.time()
task_name = 'bs4 test scraper'

from seoworkflows_lib.bs4_scrapers.xpath_single_page import xpath_single_page

crawl_data = xpath_single_page(urls=urls.head(50), xpath_selector='//head//title')

print(crawl_data.head(3))

endtime = str(round((time.time() - starttime), 2))
len_final = len(crawl_data)
logging.debug(f'{task_name} - Runtime Seconds: {endtime}; Rows Returned: {len_final}')