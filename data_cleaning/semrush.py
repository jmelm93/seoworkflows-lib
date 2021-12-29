

import sys
sys.path.insert(0, '..')


from seoworkflows_lib.data_cleaning.url_cleaning_helpers import *
from seoworkflows_lib.data_cleaning.semrush_mods_helpers import *

import logging
logging.basicConfig(level=logging.DEBUG ,datefmt='%d-%b-%y %H:%M:%S' , format='%(filename)s - %(asctime)s - %(levelname)s: [Message] - %(message)s')


class SemrushCleaning:
    """
    A class for cleaning series objects of urls.

    Attributes
    ----------
    data : DataFrame
        DataFrame object of the standard SEMRush csv export from `/analytics/organic/overview`.

    Methods
    -------
    semrush_analysis_cols():
        Takes a DataFrame object and returns ....

    position_range():
        Takes a DataFrame object and returns ....

    top20():
        Takes a DataFrame object and returns ....

    traffic_exists():
        Takes a DataFrame object and returns ....

    msv_filter():
        Takes a DataFrame object and returns ....
    
    brand_filters():
        Takes a DataFrame object and returns ....
    """

    def __init__(self, data, brand_variants=None, search_volume_exclusions=None):
        self.data = data
        self.brand_variants = brand_variants
        self.search_volume_exclusions = search_volume_exclusions


    def semrush_analysis_cols(self):
        """The semrush_analysis_cols() function takes in a DataFrame of SEMRush data and returns a new DataFrame.
        Returns: A dataframe with the following columns added:
            - Position_Range
            - Top20
            - Traffic_Exists
            - Traffic_Rank
            - Domain
        """
        data = self.data
        domain_name = get_domains_helper(input=data['URL']) 
        data = (
            data.assign(
                    Position_Range=data["Position"].apply(position_range_helper),
                    Top20=data["Position"].apply(top20_helper),
                    Traffic_Exists=data["Traffic"].apply(traffic_exists_helper),
                    Traffic_Rank=data["Traffic"].rank(ascending = False).astype(int),
                    Domain=domain_name
                    )
                .sort_values(by='Traffic', ascending=False)
                )
        # cols = list(data.columns.values)
        # print(cols)
        data = data[['URL','Domain','Keyword', 'Position', 'Previous position', 'Search Volume', 'Keyword Difficulty', 'CPC', 'Traffic', 'Traffic (%)', 'Traffic Cost', 'Competition', 'Number of Results', 'Trends', 'Timestamp', 'SERP Features by Keyword', 'Keyword Intents', 'Position_Range', 'Top20', 'Traffic_Exists', 'Traffic_Rank', ]]

        return data

    def brand_filter(self):
        data = self.data
        brand_variants = self.brand_variants

        logging.debug(f"brand_variants = {brand_variants}")
        if brand_variants != "undefined" and brand_variants != "" and brand_variants != None:
            logging.debug(f'PASSED: brand_variants != "undefined" and brand_variants != "" and brand_variants != None')

            if type(brand_variants) == list:
                brand_variants = "|".join(brand_variants)
            else:
                pass

            brand_variants = str(brand_variants)
            
            mask = ~data["Keyword"].str.contains(f"{brand_variants}", case = False, regex=True)
            data = data[mask]
            
            return data


    def msv_filter(self):
        data = self.data
        search_volume_exclusions = self.search_volume_exclusions

        logging.debug(f"search_volume_exclusions = {search_volume_exclusions}")
        if search_volume_exclusions != "undefined" and search_volume_exclusions != "" and search_volume_exclusions != None:
            logging.debug(f'PASSED: search_volume_exclusions != "undefined" and search_volume_exclusions != "" and search_volume_exclusions != None')
            search_volume_exclusions = str(search_volume_exclusions)

            filter_VolumeLessThan = data["Search Volume"] > int(f"{search_volume_exclusions}")
            data = data[filter_VolumeLessThan]

            return data

def run_all_semrush_processes(data, brand_variants, search_volume_exclusions):
    volume_exclusions = SemrushCleaning(data=data,brand_variants=brand_variants,search_volume_exclusions=search_volume_exclusions).msv_filter()
    brand_exclusions = SemrushCleaning(data=volume_exclusions,brand_variants=brand_variants,search_volume_exclusions=search_volume_exclusions).brand_filter()
    final = SemrushCleaning(data=brand_exclusions,brand_variants=brand_variants,search_volume_exclusions=search_volume_exclusions).semrush_analysis_cols()
    return final


