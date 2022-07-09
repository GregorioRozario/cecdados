#!/usr/bin/python3

"""
Performs an update on table "stakeholders"

Author: Rozario Engenharia

First release: November 16th, 2021
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

from rozlib.util import Utilities
import fiislib

verbosity = 1

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

# Constants
stakeholders_table_columns = (
    'fii_id',
    'date',
    'amount',
    )
# End of contants

# Utilities object
utilities = Utilities(verbosity)

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # second, picks the list of available FIIs, from www.fiis.com.br
    fiis_list = fiis_definition.get_fiis_dict()

    # third, retrieves quotes information from FII
    quotesUpdate = fiislib.PrimitiveIndicator(utilities, fiis_definition, 'stakeholders', 5, stakeholders_table_columns)
    utilities.call_function_for_single_or_multithread(fiis_list, quotesUpdate.update_quotes_information)
    
if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
