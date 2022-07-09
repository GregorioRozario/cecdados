"""
Updates a FII valuation (net value and stock market values)

Author: Rozario Engenharia

First release: December 6th, 2021
"""

import configparser # for reading settings file
import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)
from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import re   # for regular expressions purposes
from datetime import datetime   # for getting timestamp and date
import psycopg2

# Reads rozlib library, and fiislib library paths from config.ini file and import libraries packages
rozlib_path = ''
def add_rozlib_library_path():
    global rozlib_path, fiislib_path
    config = configparser.ConfigParser()
    config.sections()
    config.read(r'config.ini', encoding='utf-8')
    rozlib_path = config['GENERAL']['rozlibFolderPath']
    sys.path.insert(1, rozlib_path)
    
    fiislib_path = config['GENERAL']['fiislibFolderPath']
    sys.path.insert(1, fiislib_path)
add_rozlib_library_path()

from rozlib.database.postgres import Postgres
from rozlib.util import Utilities
import fiislib

valuation_column_names = [
    'fii_id',
    'timestamp',
    'net_worth',
    'stock_worth',
    ]

verbosity = 1

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def get_valuation_detailed_information(fii_ticker):
    """Retrieves net and stock market valuation for a given ticker

    Args:
        fii_ticker (str): FII ticker for which net and stock market valuation should be retrieved

    Returns:
        tuple: net valuation and stock market valuation in a tuple (net, stock)
    """
    # With Beautiful Soup retrieves FII's valuation data, such as net worth and stock valuation
    soup = BeautifulSoup(urlopen(Request(fiis_definition.FIIDetailedDataURLPart2.replace('FII_TICKER', fii_ticker), headers={"User-Agent": "Mozilla/5.0"})).read(), features='html.parser')

    #top-info top-info-2 top-info-md-3 top-info-lg-n d-flex justify-between
    valuation_section = soup.find("div", attrs={'class':'top-info top-info-2 top-info-md-3 top-info-lg-n d-flex justify-between'})  # Main div
    if valuation_section == None:
        return None

    net_worth = 0
    stock_valuation = 0

    j = 0
    for currentValue in valuation_section.findAll('span', attrs={'class': 'sub-value'}):
        if j == 0:
            try:
                net_worth = int(re.sub(r'\D', '', currentValue.text))
            except ValueError:
                # If ticker has no net worth, 0 is considered as net worth
                net_worth = 0
        else:
            try:
                stock_valuation = int(re.sub(r'\D', '', currentValue.text))
            except ValueError:
                # If ticker has no stock valuation, 0 is considered as stock valuation
                stock_valuation = 0
            break
        j = j + 1
    return net_worth, stock_valuation

def get_last_valuation_information_for_FII(fii_id, postgres):
    """Retrieves, from fiis_valuation table the last net and stock market valuation record for a given FII

    Args:
        fii_id (int): FII identification for which last valuation record should be retrieves
        postgres (rozlib.database.postgres.Postgres): valid Postgres' connection

    Returns:
        tuple: net valuation and stock market valuation in a tuple (net, stock)
    """
    
    postgres.cur.execute('SELECT net_worth, stock_worth\n'
        'FROM fiis_valuation\n'
        f'WHERE fii_id={fii_id}\n'
        'ORDER BY timestamp DESC LIMIT 1')
    return postgres.cur.fetchone()

def update_fiis_valuation(fii_ticker):
    """Inserts in fiis_valuation table a new net and stock market valuation records for a given FII

    Args:
        fii_ticker (str): FII ticker for which a new net and stock market valuation record should be inserted
    """
    
    try:
        current_net_worth, current_stock_valuation = get_valuation_detailed_information(fii_ticker)
        
        # Connects to the datebase
        postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
        postgres.connectToDatabase()

        # Collects FII id, from "registered FIIs" table
        fii_id = fiis_definition.get_fii_id_by_ticker(fii_ticker)

        # Gets the last valuation information for given FII
        try:
            retrievedTuple = get_last_valuation_information_for_FII(fii_id, postgres)
        except psycopg2.errors.UndefinedColumn:
            utilities.eprint(2, f'Problem retrieving ticker {fii_ticker} valuation information.\n Probably this FII is not yet registered')
            return

        # If there is no retrieved tuple with last valuation data, this means there is no valuation
        # information for given FII. In this case, a new valuation tuple must be inserted at the
        # "FIIs valuation" table
        if retrievedTuple == None:
            postgres.exec_insert_query('fiis_valuation',
                valuation_column_names,
                [fii_id, datetime.now(), current_net_worth, current_stock_valuation,])
            postgres.conn.commit()
            utilities.print_verbose(f'First value for ticker: {fii_ticker}\nNet: {current_net_worth}\nStock: {current_stock_valuation}\n', verbosity_level=1)
        else:
            # If current value changed from last value, inserts this new value in "FIIs valuation" table
            last_net_worth, last_stock_worth = retrievedTuple
            if last_net_worth != current_net_worth or last_stock_worth != current_stock_valuation:
                postgres.exec_insert_query('fiis_valuation',
                    valuation_column_names,
                    [fii_id, datetime.now(), current_net_worth, current_stock_valuation,])
                postgres.conn.commit()
                utilities.print_verbose(f'Ticker: {fii_ticker}\nNet: {current_net_worth}\nStock: {current_stock_valuation}\n', verbosity_level=1)
    except TypeError as e:
        utilities.eprint(2, f'Ticker {fii_ticker} has no valuation information')
    except HTTPError as e:
        utilities.eprint(2, f'Ticker {fii_ticker} has no valuation information')

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # second, picks the list of available FIIs, from www.fiis.com.br
    fiis_list = fiis_definition.get_fiis_dict()
    
    # Third, updates all FIIs valuations (net and stock market)
    utilities.call_function_for_single_or_multithread(fiis_list, update_fiis_valuation)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
