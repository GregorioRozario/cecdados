"""
Updates revenues values

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

revenue_column_names = [
    'date',
    'fii_id',
    'value',
    'assets_sales',
    'ffo',
    ]

verbosity = 1

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def get_revenue_detailed_information(fii_ticker):
    """Retrieves three months average revenue for a given ticker

    Args:
        fii_ticker (str): FII ticker for which average revenue should be retrieved

    Returns:
        float: average revenue valuation
    """
    
    # With Beautiful Soup retrieves FII's revenue data
    soup = BeautifulSoup(urlopen(Request(fiis_definition.FIIDetailedDataURLPart4.replace('FII_TICKER', fii_ticker), headers={"User-Agent": "Mozilla/5.0"})).read(), features='html.parser')

    # Third <table> with class w728
    # Fifth <tr>
    try:
        revenue_section = soup.find_all("table", attrs={'class':'w728'})[2].find_all('tr')[4]
    except IndexError:
        # Information unavailable for FII
        return None
    
    # Second, third, forth and fifth <tr>s, Sixth <td> for each
    first_revenue_value = int(revenue_section.find_all('tr')[1].find_all('td')[5].text.replace('.', ''))
    second_revenue_value = int(revenue_section.find_all('tr')[2].find_all('td')[5].text.replace('.', ''))
    third_revenue_value = int(revenue_section.find_all('tr')[3].find_all('td')[5].text.replace('.', ''))
    forth_revenue_value = int(revenue_section.find_all('tr')[4].find_all('td')[5].text.replace('.', ''))
    
    return (first_revenue_value / 3, second_revenue_value / 3, third_revenue_value / 3) # three months

def get_last_revenue_information_for_FII(fii_id, postgres):
    """Retrieves, from revenue table, the last revenue record for a given FII

    Args:
        fii_id (int): FII identification for which last revenue record should be retrieved
        postgres (rozlib.database.postgres.Postgres): valid Postgres' connection

    Returns:
        tuple: last revenue, assets sales and FFO record stored in database
    """
    
    query = ('SELECT value, assets_sales, ffo\n'
        'FROM revenue\n'
        f'WHERE fii_id={fii_id}\n'
        'ORDER BY date DESC LIMIT 1')
    postgres.cur.execute(query)
    return postgres.cur.fetchone()

def update_fii_revenue(fii_ticker):
    """Inserts in revenue table a new revenue record for a given FII

    Args:
        fii_ticker (str): FII ticker for which a new revenue record should be inserted
    """
    
    try:
        current_revenue = get_revenue_detailed_information(fii_ticker)
        
        if current_revenue == None:
            utilities.eprint(2, f'Problem retrieving ticker {fii_ticker} revenue information.\n Probably this FII has detailed web page bound to it')
        
        # Connects to the datebase
        postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
        postgres.connectToDatabase()

        # Collects FII id, from "registered FIIs" table
        fii_id = fiis_definition.get_fii_id_by_ticker(fii_ticker)

        # Gets the last valuation information for given FII
        try:
            last_revenue_value = get_last_revenue_information_for_FII(fii_id, postgres)
        except psycopg2.errors.UndefinedColumn:
            utilities.eprint(2, f'Problem retrieving ticker {fii_ticker} revenue information.\n Probably this FII is not yet registered')
            return

        # If there is no retrieved tuple with last revenue data, this means there is no revenue
        # information for given FII. In this case, a new revenue must be inserted at the
        # "revenue" table
        if last_revenue_value == None:
            postgres.exec_insert_query('revenue',
                revenue_column_names,
                [datetime.now(), fii_id, current_revenue[0], current_revenue[1], current_revenue[2],])
            postgres.conn.commit()
            utilities.print_verbose(f'First revenue value for ticker: {fii_ticker}\nValue: {current_revenue}\n', verbosity_level=1)
        else:
            # If current value changed from last value, inserts this new value in "revenue" table
            if (current_revenue[0] != float(last_revenue_value[0]) or
                current_revenue[1] != float(last_revenue_value[1]) or
                current_revenue[2] != float(last_revenue_value[2])):
                
                postgres.exec_insert_query('revenue',
                    revenue_column_names,
                    [datetime.now(), fii_id, current_revenue[0], current_revenue[1], current_revenue[2],])
                postgres.conn.commit()
                utilities.print_verbose(f'Ticker: {fii_ticker}\nRevenue: {current_revenue}\n', verbosity_level=1)
    except TypeError as e:
        utilities.eprint(2, f'Ticker {fii_ticker} has no revenue information')
    except HTTPError as e:
        utilities.eprint(2, f'Ticker {fii_ticker} has no revenue information')

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # second, picks the list of available FIIs, from www.fiis.com.br
    fiis_list = fiis_definition.get_fiis_dict()
    
    # Third, updates all FIIs valuations (net and stock market)
    utilities.call_function_for_single_or_multithread(fiis_list, update_fii_revenue)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
