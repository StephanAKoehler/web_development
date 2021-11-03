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
# geolocator = GoogleV3(api_key='AIzaSyA88neTs4bkjdA3BsG_PNZyjTfi9UUf4a8')

from tqdm.auto import tqdm
tqdm.pandas(desc="progess")
lower_strip_non_char = lambda s: regex.sub('\s+', ' ', regex.compile('\W').sub(' ', regex.compile( "((?<=^|\w)[\.\-/,']+)|([\.\-/,']+(?=\w|$))").sub('', str(s).lower()))).strip()

#TODO geo_cache
#TODO ticker_cache
class web_cache:
    '''
    class that caches the web results, and saves them with dill
    main function is fetch, which will either retrieve from cache or web
    '''
    def __init__( self, pickle_file = 'default', description = None, api_key = None, string_clean = lower_strip_non_char,
        buffer_length = 10 ):
        self.pickle_file = pickle_file
        try:
            self.api_key, self.buffer_length, self.description, self.string_clean = dill.load(open(self.pickle_file+'.pkl', 'rb'))
            self.data = dill.load(open(self.pickle_file+'_data.pkl', 'rb'))
        except:
            self.api_key, self.buffer_length, self.data, self.description, self.string_clean = \
                api_key, buffer_length, {}, description, string_clean
            dill.dump( [self.api_key, self.buffer_length, self.description, self.string_clean], open(self.pickle_file+'.pkl', 'wb') )
            dill.dump( self.data, open(self.pickle_file + '_data.pkl', 'wb'))
        self.last_save = len( self.data )

    def save(self, force_save = True):
        if force_save or self.last_save < len( self.data ) - self.buffer_length:
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
    def __init__(self ):
        api_key_ = "c5ulu3qad3if2tr1agpg"
        self.client = finnhub.Client(api_key=api_key_)
        super().__init__(  pickle_file = 'finnhub_ticker', api_key = api_key_,
                          description = 'stock ticker using finnhub' )
        self.fuzzy_business = fuzzy_regex.fuzzy_standardize().read(
            source='common_abbreviations.xlsx',
            sheet_name=['business'])
        self.string_clean = lambda s: self.fuzzy_business.remove_suffix( s.lower() )

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
                query_ = self.fuzzy_business.full_replace( query).lower()
                description_ = [self.fuzzy_business.full_replace( d ).lower() for d in description]
            else:
                query_ = self.fuzzy_business.remove_suffix( query).lower()
                description_ = [self.fuzzy_business.remove_suffix( d ).lower() for d in description]
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

    def __init__(self, pickle_file = 'GoogleV3_address' ):
        self.api_key = "AIzaSyA88neTs4bkjdA3BsG_PNZyjTfi9UUf4a8"
        self.client = GoogleV3(self.api_key)
        super().__init__(  pickle_file = pickle_file, api_key = self.api_key, description='geo data for EPA project' )

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
    import glob
    source = '/Users/stephankoehler/Dropbox/Carbon Emissions/EPA data/parent company/ghgp_data_parent_company_10_2020/facility_parent *.pkl'
    files = sorted(filter(regex.compile('facility_parent \d{4}.pkl').search, glob.glob(source)))
    df = pd.concat( [pd.read_pickle(f)[['parent name', 'parent address', 'parent city', 'parent state', 'parent zip']] for f in files ], axis = 0 ).drop_duplicates()#.iloc[:10,:]
    if True:
        ticker = ticker_cache( )
        ticker.best_one('tyson foods')
        # ticker.fetch( 'apple corporation')
        # print( ticker.best_one('apple corporation', suffix = True ), ticker.best_one('apple corporation', suffix = False ) )
        ##
        df_ = df[['parent name']].drop_duplicates().copy()
        tmp = df_['parent name'].progress_apply( lambda name: ticker.best_one(name, suffix = True, force_save = False ) )
        df_['ticker parent name with suffix'] = [v[0] if v != None else None for v in tmp]
        df_['score ticker parent name with suffix'] = [v[1] if v != None else None for v in tmp]
        tmp = df_['parent name'].progress_apply( lambda name: ticker.best_one(name, suffix = False, force_save = False ) )
        df_['ticker parent name no suffix'] = [v[0] if v != None else None for v in tmp]
        df_['score ticker parent name no suffix'] = [v[1] if v != None else None for v in tmp]
        df = df.merge( df_, on = 'parent name' )
        ##
    if True:
        geo_address = geo_cache()
        ##
        geo_address.address( '1024 east 50th street Chicago IL')


        ##
        df_ = df[['parent address', 'parent city', 'parent state', 'parent zip']].drop_duplicates().copy()
        df_['full parent address'] = df_[['parent address', 'parent city', 'parent state', 'parent zip']].progress_apply( lambda v: geo_address.string_clean( ' '.join(v) ), axis = 1 )
        # address.fetch('1024 E 50th street Chicago IL')
        # tmp = pickle.load( open('/Users/stephankoehler/Dropbox/projects/python/address/google geolocator.pkl', 'rb'))
        df_['geo full parent address'] = df_['full parent address'].progress_apply( lambda full_address: geo_address.address( full_address )['full address'] )
        df_.head()
        ##
        df_['score street address'] = df_[['parent address', 'full parent address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address.string_clean(v[0]),
                 geo_address.string_clean( geo_address.address( v[1])[ 'street address'].lower())), axis=1)

        df_['score city'] = df_[['parent city', 'full parent address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address.string_clean(v[0]),
                 geo_address.string_clean( geo_address.address( v[1])[ 'city'].lower())), axis=1)

        df_['score state'] = df_[['parent state', 'full parent address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address.string_clean(v[0]),
                 geo_address.string_clean( geo_address.address( v[1])[ 'state'].lower())), axis=1)

        df_['score zip'] = df_[['parent zip', 'full parent address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address.string_clean(v[0]),
                 geo_address.string_clean( geo_address.address( v[1])[ 'zip'].lower())), axis=1)

        df = df.merge( df_, on = ['parent address', 'parent city', 'parent state', 'parent zip'] )
        # print( geo_address.fetch( '1024 E 50th street Chicago IL') )
    out = os.path.join( os.path.split(source)[0], 'parent web referenced.pkl' )
    print( 'saving to: ', out )
    df.to_pickle( out )
    out = os.path.join( os.path.split(source)[0], 'parent web referenced.xlsx' )
    print( 'saving to: ', out )
    df.to_excel( out, index=False )
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
