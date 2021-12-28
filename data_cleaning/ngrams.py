from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text
import pandas as pd

import sys
sys.path.insert(0, '..')

from seoworkflows_lib import UrlCleaning

# class NgramAnalysis:

# def run_ngrams(self, characters, input_series):
def run_ngrams(characters, input_series):
    my_additional_stop_words = ['does', 'gets', 'like', 'got']
    characters = characters
    word_vectorizer = CountVectorizer(ngram_range=(characters, characters), analyzer='word',strip_accents = 'unicode' , stop_words=text.ENGLISH_STOP_WORDS.union(my_additional_stop_words))
    sparse_matrix = word_vectorizer.fit_transform(input_series['stripped'])
    frequencies = sum(sparse_matrix).toarray()[0]
    df = pd.DataFrame(frequencies, index=word_vectorizer.get_feature_names(), columns=['frequency'])
    return df

def url_ngrams(input_series, ngram_type, characters):
    num_of_inputs_allowed = 1000
    try:
        ### Convert series to frame + extracts paths using `data_cleaning` module
        ### Strip URL paths to be singular words - `\.[^.]*$` at the end removes everything aftere the last '.' (e.g., '.html')
        input_series = input_series.drop_duplicates()
        df = input_series.to_frame('inputs')
        df = UrlCleaning(url_series=df['inputs']).get_url_path()
        df["stripped"] = df["paths"].str.replace('.*.com/|/|-|\.[^.]*$', ' ', regex=True)

        all_ngrams = run_ngrams(characters=characters, input_series=df.head(num_of_inputs_allowed))
        
        all_ngrams = (all_ngrams.reset_index()
                                .sort_values(by=['frequency'], ascending=[False])
                                .rename(columns={'index': 'ngram'}))
        all_ngrams.assign(
            Type= ngram_type,
            ngram_cleaned= all_ngrams["ngram"].str.replace(' ', '-', regex=False)
            )

        all_ngrams = all_ngrams.loc[all_ngrams['frequency'] > int(5)]

        return all_ngrams

    except BaseException as e:
        print("UnidentifiedError: {0}".format(e)) 


