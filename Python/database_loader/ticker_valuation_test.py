import configparser
import time # for dealing with time operations/elapsed time
import sys  # For pointing rozlib library folder (path comes from config.ini file)
import investpy

# Constants
csv_historical_folder_path = r'C:\Projetos\Python\Stock Market\Resources\Datasets\csv'
# end of constants

# Reads rozlib library, and fiislib library paths from config.ini file and import libraries packages
rozlib_path = ''
def add_rozlib_library_path():
    global rozlib_path, fiislib_path
    config = configparser.ConfigParser()
    config.sections()
    config.read(r'config.ini', encoding='utf-8')
    rozlib_path = config['GENERAL']['rozlibFolderPath']
    print(rozlib_path)
    sys.path.insert(1, rozlib_path)
    
    fiislib_path = config['GENERAL']['fiislibFolderPath']
    sys.path.insert(1, fiislib_path)
add_rozlib_library_path()

#from rozlib.database.postgres import Postgres
from rozlib.util import Utilities
import fiislib

# Utilities object
utilities = Utilities()

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def get_ticker_quotations(ticker):
    try:
        print(f'Ticker: {ticker}')
        df = investpy.get_stock_historical_data(stock=ticker,
            country='Brazil',
            from_date='01/01/1990',
            to_date='28/06/2022',
            interval="Daily")
        df.drop(columns=['Currency'], inplace=True)
        df.to_csv(csv_historical_folder_path + '\\' + ticker + ".csv", sep=';')
        time.sleep(4)
        #print(df.head(10))
        #exit(0)
    except RuntimeError as E:
        print(f'Ticker {ticker} not found in investing.com')
        print(E)
    except ConnectionError as E:
        print("Server blocked: too many connections")
        print(E)
    except IndexError as E:
        print("Historical data page exists but is empty")
        print(E)

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    #retrievedData = fiis_definition.get_registered_fiis_list()
    retrievedData = investpy.get_stocks_dict(country='Brazil')
    utilities.call_function_for_single_or_multithread([x['symbol'] for x in retrievedData], get_ticker_quotations)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)