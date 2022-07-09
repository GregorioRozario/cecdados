#!/usr/bin/python3

from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
from urllib.request import Request, urlopen
import re   # for regular expressions purposes
import configparser # for reading settings file
import datetime # for handling dates and timestamps
import sys  # For pointing rozlib library folder (path comes from config.ini file)

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

# Contants
dividends_quotes_table_columns = [
    'fii_id',
    'date_record',
    'date_payment',
    'type_id',
    'value',
    ]
# end of Constants

verbosity = 1

# If a command line argument called multithread was passed as True, parallel executions will take place
isMultiThread = False

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def get_fiis_list():
    """Gets the list of registered FIIs, from the "registered FIIs" table

    Returns:
        list: all registered FIIs ids and tickers, in a list of tuples
    """
    
    # Connects to the datebase
    pgsql = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    pgsql.connectToDatabase()
    pgsql.cur.execute('SELECT fii_id, ticker FROM registered_fiis')
    data = pgsql.cur.fetchall()
    pgsql.disconnectFromDatabase()
    return [(x[0], x[1]) for x in data]

def register_new_dividend_type(type_description, pgsql):
    """Inserts a new dividend type in the "dividend types" table

    Args:
        type_description (str): the new dividend type description
        pgsql (rozlib.database.postgres.Postgre): object with a valid connection to the SQL database

    Returns:
        int: new inserted dividend type id
    """
    new_type_id =  pgsql.exec_insert_query('dividend_types',
                                          ['description'],
                                          [type_description])
    pgsql.conn.commit()
    return new_type_id

def get_dividends_data_for_fii(fii_ticker):
    """Gets dividend quotation history for a given FII

    Args:
        fii_ticker (str): FII ticker, whose dividend history should be retrieved

    Returns:
        list: list of dictionaries with dividends':
            quotations
            date record (data com)
            date payment
            type (of dividend)
    """
    
    dividend_data_list = []

    # Gathers information from website
    fiis_dividends_info_page = urlopen(Request(fiis_definition.FIIDividendsHistory.replace('FII_TICKER', fii_ticker), headers={"User-Agent": "Mozilla/5.0"})).read()

    # parses the html, looking for those places where the main information are located
    soup = BeautifulSoup(fiis_dividends_info_page, features='html.parser')

    # The list of quotations;
    # If there is no quotation bound to the FII, there is nothing else left for this function
    try:
        values = []
        for current_dividend_quotation in soup.find('table', attrs={'id':'resultado'}).findAll('td', text=re.compile(r'^\d*[,\.]\d*$')):
            values.append(float(current_dividend_quotation.text.replace(',', '.')))
        
        i = 0
        dates_records = []
        dates_payments = []
        for current_date_information in soup.find('table', attrs={'id':'resultado'}).findAll('td', text=re.compile(r'^\d{1,2}\/\d{1,2}\/\d{2,4}$')):
            if i % 2 == 0:
                dates_records.append(current_date_information.text)
            else:
                dates_payments.append(current_date_information.text)
            i = i + 1

        types = []
        for current_dividend_type in soup.find('table', attrs={'id':'resultado'}).findAll('td', text=re.compile(r'^\D*$')):
            types.append(current_dividend_type.text)
    except AttributeError:
        return []

    for i in range(0, len(values)):
        current_data = {}
        current_data['value'] = values[i]
        current_data['type'] = types[i]
        current_data['date_record'] = datetime.datetime.strptime(dates_records[i], "%d/%m/%Y").date()
        current_data['date_payment'] = datetime.datetime.strptime(dates_payments[i], "%d/%m/%Y").date()
        dividend_data_list.append(current_data)
    
    return dividend_data_list

def update_dividends_for_fii(current_fii):
    """Updates data in "dividend quotations" table

    Args:
        current_fii (tuple): FII information, with pattern (fii id, ticker), whose dividend data should be updated
    """
    
    (fii_id, fii_ticker) = current_fii

    # Connects to the datebase
    postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    postgres.connectToDatabase()

    for current_dividend_entry in get_dividends_data_for_fii(fii_ticker):
        # Gets dividend type ID, from "dividend types" table
        postgres.cur.execute("SELECT type_id\n"
            "FROM dividend_types\n"
            "WHERE LOWER(description) = LOWER(%s)\n",
            (current_dividend_entry['type'], ))
        retrievedData = postgres.cur.fetchone()

        # If type was not found in "dividend types" table, inserts it in this table
        type_id = 0
        if retrievedData == None:
            type_id = register_new_dividend_type(current_dividend_entry['type'], postgres)
        else:
            type_id = retrievedData[0]

        data = [fii_id, current_dividend_entry['date_record'], current_dividend_entry['date_payment'], type_id, current_dividend_entry['value']]
        retVal = postgres.exec_insert_query_ignoring_repeated_pk('dividend_quotations',
                                                        dividends_quotes_table_columns,
                                                        data)
        # In case there was a return different from None, this means new data was inserted in the table
        if retVal != None:
            postgres.conn.commit()
                        
            utilities.print_verbose(f'New data inserted to ticker {fii_ticker}', verbosity_level=verbosity)
        else:
            postgres.conn.rollback()
    postgres.disconnectFromDatabase()

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # Gets FIIs' list
    fiis_list = get_fiis_list()

    # inserts up to date dividend information, one by one, for each FII
    utilities.call_function_for_single_or_multithread(fiis_list, update_dividends_for_fii)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
