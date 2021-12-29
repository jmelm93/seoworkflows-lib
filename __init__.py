import pandas as pd

from dataclasses import dataclass

import sys
sys.path.insert(0, '..')

from seoworkflows_lib.logging.timers import Timer
t = Timer()

__version__ = '0.0.1'

@dataclass
class CustomJoins:
    full_values: list
    matching_criteria: list

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


@dataclass
class NgramAnalysis:
    input_list: list
    ngram_type: str
    characters: int

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

    def url_ngrams(self):

        from seoworkflows_lib.data_cleaning import url_ngrams

        task_name = 'url_ngrams'
        t.start(name=task_name)

        final = url_ngrams(input_list=self.input_list, ngram_type=self.ngram_type, characters=self.characters)

        t.stop(name=task_name, output_len=len(final))

        return final

@dataclass
class UrlCleaning:
    url_list: list

    """
    A class for cleaning series objects of urls.

    Parameters
    ----------
    url_list : list
        Urls for data cleaning.

    Methods
    -------
    get_url_path():
        Takes a Url list and returns the input URLs + URL paths.

    get_domain():
        Takes a Url list and returns the input URLs + domain

    get_url_parts():
        Takes a Url list and returns all URL parts (<scheme>://<netloc>/<path>;<params>?<query>).

    get_path_and_directories():
        Takes a Url list and returns the path and directories (directories are in single sell list).

    get_all_directories_1_per_row():
        Takes a Url list and returns the path and directories (1 directory per column - duplicating the path).
    """
    
    def get_url_path(self):
        from seoworkflows_lib.data_cleaning import get_url_path # No timers on all below as conflicts with ngram timers
        final = get_url_path(url_list=self.url_list)
        return final

    def get_domain(self):
        from seoworkflows_lib.data_cleaning import get_domain # No timers on all below as conflicts with ngram timers
        final = get_domain(url_list=self.url_list)
        return final

    def get_url_parts(self):
        from seoworkflows_lib.data_cleaning import get_url_parts # No timers on all below as conflicts with ngram timers
        final = get_url_parts(url_list=self.url_list)
        return final

    def get_path_and_directories(self):
        from seoworkflows_lib.data_cleaning import get_path_and_directories # No timers on all below as conflicts with ngram timers
        final = get_path_and_directories(url_list=self.url_list)
        return final

    def get_all_directories_1_per_row(self):
        from seoworkflows_lib.data_cleaning import get_all_directories_1_per_row # No timers on all below as conflicts with ngram timers
        final = get_all_directories_1_per_row(url_list=self.url_list)        
        return final



def run_all_semrush_processes(data, brand_variants, search_volume_exclusions):
    from seoworkflows_lib.data_cleaning import run_all_semrush_processes

    task_name='Semrush Processes'
    t.start(name=task_name)
    
    final = run_all_semrush_processes(data, brand_variants=brand_variants, search_volume_exclusions=search_volume_exclusions)

    t.stop(name=task_name, output_len=len(final))
    
    return final



def send_email(subject_line, content):
    from seoworkflows_lib.send_email.sendgrid import send_email
    send_email(subject_line=subject_line, content=content)