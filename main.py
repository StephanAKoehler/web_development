'''
Here I am using classes from Google_Finnhub.py to process EPA data
'''
from Google_Finnhub import ticker_cache, geo_cache
import glob
import regex
import pandas as pd
import rapidfuzz
import os
from api_keys import api_keys #api_keys.py contains dictionary
#api_keys = {'finnhub': 'your_finnhub_api', 'GoogleV3': 'your_googleV3_api' }

if __name__ == '__main__':
    source = '/Users/stephankoehler/Dropbox/Carbon Emissions/EPA data/parent company/ghgp_data_parent_company_10_2020/facility_parent *.pkl'
    files = sorted(filter(regex.compile('facility_parent \d{4}.pkl').search, glob.glob(source)))

    import glob
    source = '/Users/stephankoehler/Dropbox/Carbon Emissions/EPA data/parent company/ghgp_data_parent_company_10_2020/facility_parent *.pkl'
    files = sorted(filter(regex.compile('facility_parent \d{4}.pkl').search, glob.glob(source)))
    if False: #dealing with parents: ticker and geo-location
        df = pd.concat( [pd.read_pickle(f)[['parent name', 'parent address', 'parent city', 'parent state', 'parent zip']] for f in files ], axis = 0 ).drop_duplicates()#.iloc[:10,:]
        if True: #parent tickers
            ticker = ticker_cache(api_key=api_keys['finnhub'])
            # ticker.best_one('tyson foods')
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
        if True: #geo-locating parents
            geo_address = geo_cache( api_key = api_keys['GoogleV3'] )
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

    if True: #geolocating facilities
        geo_address_facility = geo_cache(api_key=api_keys['GoogleV3'], pickle_file = 'GoogleV3_facility_adress' )
        df = pd.concat( [pd.read_pickle(f)[['FACILITY ADDRESS', 'FACILITY CITY',  'FACILITY STATE',  'FACILITY ZIP']] for f in files ], axis = 0 ).drop_duplicates()
        ##
        df['facility address'] = df['FACILITY ADDRESS'].progress_apply( lambda v: geo_address_facility.string_clean(v) if isinstance(v,str) else '' )
        df['facility city'] = df['FACILITY CITY'].progress_apply( lambda v: geo_address_facility.string_clean(v) if isinstance(v,str) else '' )
        df['facility state'] = df['FACILITY STATE'].progress_apply( lambda v: geo_address_facility.string_clean(v) if isinstance(v,str) else '' )

        df['facility zip'] = df['FACILITY ZIP'].progress_apply( lambda v: str(v)[:5])
        df['full facility address'] = df[['facility address', 'facility city', 'facility state', 'facility zip']].progress_apply(  lambda v: geo_address_facility.string_clean( ' '.join(v) ), axis = 1 )
        df_ = df[['facility address', 'facility city', 'facility zip', 'facility state', 'full facility address']].drop_duplicates()
        df_['geo full facility address'] = df_['full facility address'].progress_apply(  lambda full_address: geo_address_facility.address(full_address)['full address'])
        ##
        df_['score street address'] = df_[['facility address', 'full facility address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address_facility.string_clean(v[0]),
                     geo_address_facility.string_clean( geo_address_facility.address( v[1])[ 'street address'].lower())), axis=1)
        ##
        df_['score city'] = df_[['facility city', 'full facility address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address_facility.string_clean(v[0]),
                 geo_address_facility.string_clean( geo_address_facility.address( v[1])[ 'city'].lower())), axis=1)
        ##
        df_['score state'] = df_[['facility state', 'full facility address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address_facility.string_clean(v[0]),
                 geo_address_facility.string_clean( geo_address_facility.address( v[1])[ 'state'].lower())), axis=1)
        ##
        df_['score zip'] = df_[['facility zip', 'full facility address']].progress_apply(lambda v: rapidfuzz.fuzz.ratio(geo_address_facility.string_clean(v[0]),
                 geo_address_facility.string_clean( geo_address_facility.address( v[1])[ 'zip'].lower())), axis=1)

        df_.head()
        df = df.merge(df_[['full facility address', 'geo full facility address', 'score street address', 'score city', 'score state', 'score zip' ]], on='full facility address')
        out = os.path.join( os.path.split(source)[0], 'facility web referenced.pkl' )
        print( 'saving to: ', out )
        df.to_pickle( out )
        out = os.path.join( os.path.split(source)[0], 'facility web referenced.xlsx' )
        print( 'saving to: ', out )
        df.to_excel( out, index=False )

        ##
