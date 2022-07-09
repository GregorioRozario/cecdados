"""
Script for registering new FIIs, as long as they appear in the https://fiis.com.br/lista-de-fundos-imobiliarios/
webpage

Author: Rozario Engenharia

First release: November 24th, 2021
"""

import configparser # for reading settings file
import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)

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

verbosity = 1

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

registered_fiis_columns = (
    'ticker',
    'description',
    'stock_market_session_name',
    'type_id',
    'anbima_type_id',
    'cvm_registration_date',
    'cnpj',
    'administrator_id'
)

def register_fiis_not_in_registered_fiis_table(internet_fiis_dict, db_fiis_list):
    """Registers FIIs present in www.fiis.com.br and not present in the "registered FIIs" database

    Args:
        internet_fiis_dict (dict): dictionary with FIIs general information in the format {ticker: {link}, {description}}
        db_fiis_list (list): list of FIIs, from the database, in tuples format
    """
    
    # Lists those FIIs from internet list, not in DB list
    new_fiis_tickers = list(set(internet_fiis_dict.keys()).difference([x[1] for x in db_fiis_list]))

    # iterates through the new FIIs list, registering them (addind to the "registered FIIs" table)
    for current_fii_ticker in new_fiis_tickers:
        current_fii_detailed_info = fiis_definition.getDetailedInformationFromFII(current_fii_ticker, internet_fiis_dict[current_fii_ticker])
        
        # If there are no detailed information about the FII, it is useless to keep on this function
        if current_fii_detailed_info == {}:
            utilities.eprint(1, f"Unable to get detailed information from {current_fii_ticker}. Any error on server?")
            continue

        # Retrieves FII type id, from "type" table
        type_id = fiis_definition.get_fii_type_by_description(current_fii_detailed_info['type'])
        
        # Retrieves Anbima type id, from "anbima_type" table
        anbima_type_id = fiis_definition.get_anbima_type_by_description(current_fii_detailed_info['anbimaType'])

        # Retrieves Administrator's id from the "FIIs administrators" table
        administrator_id = fiis_definition.get_administrator_id_by_cnpj(current_fii_detailed_info['administratorCNPJ'], current_fii_detailed_info['administratorName'])

        # Connects to the database
        pgsql = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
        pgsql.connectToDatabase()
        fii_id = pgsql.exec_insert_query('registered_fiis',
            registered_fiis_columns, 
            [current_fii_ticker,
            internet_fiis_dict[current_fii_ticker]['description'],
            current_fii_detailed_info['stockMarketSessionName'],
            type_id,
            anbima_type_id,
            current_fii_detailed_info['cvmRegistrationDate'],
            current_fii_detailed_info['cnpj'],
            administrator_id
            ],
            primary_key_label='fii_id')
        pgsql.conn.commit()
        pgsql.disconnectFromDatabase()
        
        # Indicates a new FII was just inserted at the database
        utilities.print_verbose(message=f'New fii {current_fii_ticker} registered with ID {fii_id}', verbosity_level=0)

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # second, picks the list of available FIIs, from www.fiis.com.br
    fiis_dict_from_internet = fiis_definition.get_fiis_dict()

    # third: lists all FIIs from "registered FIIs" table
    fiis_list_from_db = fiis_definition.get_registered_fiis_list(ignore_fiis_without_detailed_info_weblink=False)

    # Forth: FIIs found in www.fiis.com.br and not found in
    # "registered FIIs" table are then stored
    register_fiis_not_in_registered_fiis_table(fiis_dict_from_internet, fiis_list_from_db)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
