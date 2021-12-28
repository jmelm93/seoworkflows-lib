
import sys
sys.path.insert(0, '..')

from seo_workflows.data_cleaning.url_cleaning_helpers import *

def get_url_path(url_series):
    df = url_series.to_frame('inputs')
    df = df.drop_duplicates(subset = 'inputs')
    df['paths'] = get_paths_helper(input = df['inputs'].tolist())
    return df

def get_domain(url_series):
    df = url_series.to_frame('inputs')
    df = df.drop_duplicates(subset = 'inputs')
    df['domains'] = get_domains_helper(input = df['inputs'].tolist())
    return df

def get_url_parts(url_series):
    df = url_series.to_frame('inputs')
    df = df.drop_duplicates(subset = 'inputs')
    df = df.assign(
        scheme=get_schemes_helper(input = df['inputs'].tolist()),
        domain=get_domains_helper(input = df['inputs'].tolist()),
        path=get_paths_helper(input = df['inputs'].tolist()),
        params=get_params_helper(input = df['inputs'].tolist()),
        query=get_queries_helper(input = df['inputs'].tolist())

    )
    return df

def get_path_and_directories(url_series):
    df = url_series.to_frame('inputs')
    df = df.drop_duplicates(subset = 'inputs')
    df['path'] = get_paths_helper(input = df['inputs'].tolist())
    df['directories'] = get_directories_from_path_helper(paths = df['path'].tolist())
    return df

def get_all_directories_1_per_row(url_series):
    df = url_series.to_frame('inputs')
    df = df.drop_duplicates(subset = 'inputs')
    df['path'] = get_paths_helper(input = df['inputs'].tolist())
    df['directories'] = get_directories_from_path_helper(paths = df['path'].tolist())
    df = (df.drop(columns=['path'])
        .explode('directories')
    )
    return df

