#!/usr/bin/python3

"""
Script for updating FIIs' daily quotation on Database
Author: Rozario
First release: August 23rd, 2021
"""

#from email import message
import configparser # for reading settings file
import datetime # for handling dates and timestamps
import time # for dealing with time operations/elapsed time
from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
from urllib.request import Request, urlopen
import sys  # For pointing rozlib library folder (path comes from config.ini file)
import urllib   # For dealing with HTTP resquests errors

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
fiis_daily_quotation_columns = (
    'fii_id',
    'date',
    'last',
    'open',
    'maximum',
    'minimum',
    'volume',
    'variation'
)
# End of contants

# Utilities object
utilities = Utilities()

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def updateFiiDailyQuotation(fii_data):
    """Updates daily quotations for a given FII

    Args:
        fii_data (iterable): iterable with fii id (from registered FIIs table), ticker and link to webpage with daily quotation
    """
    
    fii_id = fii_data[0]
    fii_ticker = fii_data[1]
    fii_historical_data_link = fii_data[2]

    # Connects to the datebase
    postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    postgres.connectToDatabase()

    # Necessary because investing.com tries to deny direct access without a browser
    try:
        fiiDetailedInfoPage = urlopen(Request(fii_historical_data_link, headers={"User-Agent": "Mozilla/5.0"})).read()
    
        # parses the html, looking for those places where the main information are located
        soup = BeautifulSoup(fiiDetailedInfoPage, features='html.parser')
        
        for table in soup.findAll('table', attrs={'id':'curr_table'}):
            tbody = BeautifulSoup(str(table), features='html.parser').findAll('tbody')
            for tr in BeautifulSoup(str(tbody), features='html.parser').findAll(str('tr')):
                td = BeautifulSoup(str(tr), features='html.parser').findAll(str('td'))
                    
                try:
                    # First table row: date
                    date = td[0].text
                    # Second table row: last quotation
                    lastQuotation = td[1].text.replace(".", "").replace(",", ".")
                    # Third table row: opening quotation
                    openingQuotation = td[2].text.replace(".", "").replace(",", ".")
                    # Forth table row: maximum quotation
                    maximumQuotation = td[3].text.replace(".", "").replace(",", ".")
                    # Fifth table row: minimum quotation
                    minimumQuotation = td[4].text.replace(".", "").replace(",", ".")
                    # Sixth table row: quotations dealings volume
                    volume = td[5].text.replace(".", "").replace(",", ".").replace("-", "0")
                    volume = float(volume.replace("K", "")) * 1000 if volume.find("K") != -1 else float(volume.replace("M", "")) * 1000000 if volume.find("M") != -1 else float(volume)
                    # Seventh table row: quotation variation
                    variation = td[6].text.replace(".", "").replace(",", ".").replace("%", "")
                    
                    # only inserts quotation data for dates prior than today or if today, after 8:00pm (to keep data always up to date, avoiding rubish)
                    if (datetime.datetime.strptime(date, "%d.%m.%Y").date() < datetime.date.today() or
                    (datetime.datetime.strptime(date, "%d.%m.%Y").date() == datetime.date.today() and time.struct_time(time.localtime()).tm_hour >= 20)):
                        retVal = postgres.exec_insert_query_ignoring_repeated_pk('fiis_daily_quotation',
                            fiis_daily_quotation_columns,
                            (fii_id,
                            datetime.datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d"),
                            lastQuotation,
                            openingQuotation,
                            maximumQuotation,
                            minimumQuotation,
                            volume,
                            variation))
                        # In case there was a return different from None, this means new data was inserted in the table
                        if retVal != None:
                            postgres.conn.commit()
                            utilities.print_verbose(f'New data inserted to ticker {fii_ticker}', verbosity_level=1)
                        # On the other hand, make sure no updates are sent to the database
                        else:
                            postgres.conn.rollback()
                except IndexError:
                    # there is no data to be retrieved from website
                    utilities.eprint(2, f'\nCould not retrieve quotation data for ticker {fii_ticker}. Missing data')
    except urllib.error.HTTPError as e:
        utilities.eprint(2, f'\nCould not retrieve quotation data for ticker {fii_ticker}. HTTP Error {e}')

    # Disconnects from the database
    postgres.disconnectFromDatabase()

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    retrievedData = fiis_definition.get_registered_fiis_list()
    utilities.call_function_for_single_or_multithread(retrievedData, updateFiiDailyQuotation)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)