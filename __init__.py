import pandas as pd

import sys
sys.path.insert(0, '..')

from seoworkflows_lib.logging.timers import Timer
t = Timer()

# import time
# import logging
# logging.basicConfig(level=logging.DEBUG ,datefmt='%d-%b-%y %H:%M:%S' , format='%(filename)s - %(asctime)s - %(levelname)s: [Message] - %(message)s')


__version__ = '0.0.1'

def run_all_semrush_processes(data, brand_variants, search_volume_exclusions):
    from seoworkflows_lib.data_cleaning import run_all_semrush_processes

    task_name='Semrush Processes'
    t.start(name=task_name)
    
    final = run_all_semrush_processes(data, brand_variants=brand_variants, search_volume_exclusions=search_volume_exclusions)

    t.stop(name=task_name, output_len=len(final))
    
    return final


class CustomJoins:

    def __init__(self, full_values, matching_criteria):
        self.full_values = full_values
        self.matching_criteria = matching_criteria

    def partial_match_join_first_match_returned(self):
        from seoworkflows_lib.data_cleaning import partial_match_join_first_match_returned

        task_name='Join on Partial Match [Max 1 Match Returned]'
        t.start(name=task_name)

        output = partial_match_join_first_match_returned(full_values=self.full_values, matching_criteria=self.matching_criteria)
        final = pd.concat(output)
        
        t.stop(name=task_name, output_len=len(final))
        
        return final

    def partial_match_join_all_matches_returned(self):
        from seoworkflows_lib.data_cleaning import partial_match_join_all_matches_returned

        task_name = 'Join on Partial Match [All Match Returned]'
        t.start(name=task_name)

        output = partial_match_join_all_matches_returned(full_values=self.full_values, matching_criteria=self.matching_criteria)
        final = pd.concat(output)

        t.stop(name=task_name, output_len=len(final))

        return final



class NgramAnalysis:

    def url_ngrams(self, input_series, ngram_type, characters):
        """
        The url_ngrams() function takes a series of URLs and select imputs and 
        returns ngrams and ngram frequencies in a dataframe.

        Parameters
        ----------
        characters: int 
            int of '1' for unigram, '2' for bigram, etc...
        ngram_type: str 
            str that'll be returned in 'ngram_type' column
        input_series: series
            series that contains urls

        Returns
        -------
        A dataframe with 4 columns: 'ngram', 'frequency', 'type', 'ngram_cleaned' 

        """
        from seoworkflows_lib.data_cleaning import url_ngrams

        task_name = 'url_ngrams'
        t.start(name=task_name)

        final = url_ngrams(input_series=input_series, ngram_type=ngram_type, characters=characters)

        t.stop(name=task_name, output_len=len(final))

        return final


class UrlCleaning:
    """
    A class for cleaning series objects of urls.

    Parameters
    ----------
    url_series : series
        Urls for data cleaning.

    Methods
    -------
    get_url_path():
        Takes a Url series and returns the input URLs + URL paths.

    get_domain():
        Takes a Url series and returns the input URLs + domain

    get_url_parts():
        Takes a Url series and returns all URL parts (<scheme>://<netloc>/<path>;<params>?<query>).

    get_path_and_directories():
        Takes a Url series and returns the path and directories (directories are in single sell list).

    get_all_directories_1_per_row():
        Takes a Url series and returns the path and directories (1 directory per column - duplicating the path).
    """

    def __init__(self, url_series): # No timers on all below as conflicts with ngram timers
        self.url_series = url_series
    
    def get_url_path(self):
        from seoworkflows_lib.data_cleaning import get_url_path
        final = get_url_path(url_series=self.url_series)
        return final

    def get_domain(self):
        from seoworkflows_lib.data_cleaning import get_domain
        final = get_domain(url_series=self.url_series)
        return final

    def get_url_parts(self):
        from seoworkflows_lib.data_cleaning import get_url_parts
        final = get_url_parts(url_series=self.url_series)
        return final

    def get_path_and_directories(self):
        from seoworkflows_lib.data_cleaning import get_path_and_directories
        final = get_path_and_directories(url_series=self.url_series)
        return final

    def get_all_directories_1_per_row(self):
        from seoworkflows_lib.data_cleaning import get_all_directories_1_per_row
        final = get_all_directories_1_per_row(url_series=self.url_series)        
        return final
