'''
classes for performing web searches
Intended for EPA project, geo-locating addresses of facilities and parents, and finding tickers of parents. Specifically here I am just determining if the parent company has a ticker.


'''
import os
import dill
import pickle
import regex
import finnhub # https://github.com/Finnhub-Stock-api_key/finnhub-python

import sys

sys.path.append("/Users/stephankoehler/Dropbox/projects/python/fuzzy_lookup")
import fuzzy_regex
import rapidfuzz
import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
from collections import defaultdict
from geopy.geocoders import GoogleV3

from tqdm.auto import tqdm
tqdm.pandas(desc="progess")
lower_strip_non_char = lambda s: regex.sub('\s+', ' ', regex.compile('\W').sub(' ', regex.compile( "((?<=^|\w)[\.\-/,']+)|([\.\-/,']+(?=\w|$))").sub('', str(s).lower()))).strip()

class web_cache:
    '''
    class that caches the web results, and saves them with dill
    main function is fetch, which will either retrieve from cache or web
    '''
    def __init__( self, pickle_file = 'default', description = None, api_key = None, string_clean = lower_strip_non_char,
        buffer_size = 10 ):
        self.pickle_file = pickle_file
        try:
            self.api_key, self.buffer_size, self.description, self.string_clean = dill.load(open(self.pickle_file+'.pkl', 'rb'))
            self.data = dill.load(open(self.pickle_file+'_data.pkl', 'rb'))
        except:
            self.api_key, self.buffer_size, self.data, self.description, self.string_clean = \
                api_key, buffer_size, {}, description, string_clean
            dill.dump( [self.api_key, self.buffer_size, self.description, self.string_clean], open(self.pickle_file+'.pkl', 'wb') )
            dill.dump( self.data, open(self.pickle_file + '_data.pkl', 'wb'))
        self.last_save = len( self.data )

    def save(self, force_save = True):
        if force_save or self.last_save < len( self.data ) - self.buffer_size:
            dill.dump( self.data, open(self.pickle_file + '_data.pkl', 'wb'))
            self.last_save = len( self.data )

    def fetch(self, query, force_save = True, cleaned_query = False ):
        if not cleaned_query:
            query_ = self.string_clean( query )
        else:
            query_ = query
        if not query_ in self.data:
            # GoogleV3(api_key='AIzaSyA88neTs4bkjdA3BsG_PNZyjTfi9UUf4a8').geocode( query_, exactly_one=False )
            try:
                self.data[query_] = self.client_fnc( query )
            except:
                self.data[query_] = None
            self.save(force_save)
        return self.data[query_]

    def DataFrameAppend(self, df, query_column, the_func = None ):
        if the_func == None:
            the_func = lambda v: self.fetch( v, False )
        val = []
        for name in tqdm(df[query_column]):
            val.append( the_func(name ) )
        df['fetch %s'%query_column] = val
        self.save(force_save=True)

class ticker_cache( web_cache ):
    '''
    find ticker (if it exists) using finnhub
    '''
    fuzzy_business = fuzzy_regex.fuzzy_standardize().read(
        source='common_abbreviations.xlsx',
        sheet_name=['business'])

    def __init__(self, **kwargs ):
        if not 'pickle_file' in kwargs:
            kwargs['pickle_file'] = 'finnhub_ticker'
        if not 'description' in kwargs:
            kwargs['description'] = 'stock ticker using finnhub'
        assert 'api_key' in kwargs, 'get API key from finnhub'

        self.client = finnhub.Client(api_key=kwargs['api_key'])
        super().__init__(  **kwargs )
        self.string_clean = lambda s: ticker_cache.fuzzy_business.remove_suffix( s.lower() )

    def client_fnc(self, query):
        query_clean = self.string_clean(query)
        return list( np.unique( [r['description'].lower() for r in self.client.symbol_lookup(query_clean)['result']] ) )

    def best_one(self, query, suffix = True, force_save = True ):
        description = self.fetch(query, force_save)
        if description == []:
            return None
        else:
            description = [regex.sub('-', ' ', d) for d in description]
            if suffix:
                query_ = ticker_cache.fuzzy_business.full_replace( query).lower()
                description_ = [ticker_cache.fuzzy_business.full_replace( d ).lower() for d in description]
            else:
                query_ = ticker_cache.fuzzy_business.remove_suffix( query).lower()
                description_ = [ticker_cache.fuzzy_business.remove_suffix( d ).lower() for d in description]
            scores = [rapidfuzz.fuzz.token_set_ratio(query_, d ) for d in description_]
            best = np.argmax(scores)
            return ( description_[best], scores[best] )

class geo_cache( web_cache ):
    '''
    geolocate address using google
    '''
    drop_suite = lambda s: regex.sub('(ste|suite) \w+', '', s, flags=regex.IGNORECASE).strip()
    fuzzy_address = fuzzy_regex.fuzzy_standardize().read(
        source='common_abbreviations.xlsx',
        sheet_name=['compass directions', 'states', 'cardinals', 'ordinals', 'postal suffix'])

    def __init__(self, **kwargs ):
        # "AIzaSyA67lO5EpX8z1Adv2ePvSSokzFKRFdAi6g"

        if not 'pickle_file' in kwargs:
            kwargs['pickle_file'] = 'GoogleV3_address'
        if not 'description' in kwargs:
            kwargs['description'] = 'geo data caching'
        assert 'api_key' in kwargs, 'get API key from google'

        self.client = GoogleV3(kwargs['api_key'])
        super().__init__(  **kwargs )

        self.address_dict = {}
        self.string_clean = lambda s: geo_cache.drop_suite( geo_cache.fuzzy_address.full_replace( lower_strip_non_char( s )) ).lower()
        ##
    def client_fnc(self, query):
        ##
        return self.client.geocode( self.string_clean( query ), exactly_one=True ) #set to false if I want several addresses

    def address(self, query ):
        ##
        query_ = self.string_clean( query ).lower()
        location = self.fetch( query_, cleaned_query=True )
        if not query_ in self.address_dict:
            address_struct_ = defaultdict(lambda: '')
            if location == None:
                self.address_dict[query_] = address_struct_
            else:
                for c in location.raw['address_components']:
                    address_struct_[' '.join( c['types'] ) ] = c['short_name']
                ##
                address_struct_['street address'] = '%s %s'%(address_struct_['street_number'], address_struct_['route'] )
                address_struct_['full address'] = ' '.join([address_struct_[k] for k in ['street_number', 'route', 'locality political', 'administrative_area_level_1 political', 'postal_code']])
                address_struct_['city'] = address_struct_['locality political']
                address_struct_['state'] = address_struct_['administrative_area_level_1 political']
                address_struct_['zip'] = address_struct_['postal_code']
                self.address_dict[query_] = address_struct_
        return self.address_dict[query_]
        ##

    def best_one(self, query, suffix = True, force_save = True ):
        ##
        ans = self.fetch( query, force_save )
        ##
        pass


# Press the green button in the gutter to run the script.
# geo_address = geo_cache()
# geo_address.address('2103 s me st jacksonville il 62650')['full address']

if __name__ == '__main__':

    pass