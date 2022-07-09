#!/usr/bin/python3

"""
Performs an update on table "real_estates" and "real_estates_indices"

Author: Rozario Engenharia

First release: December 1st, 2021
"""

from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
from urllib.request import Request, urlopen
from unidecode import unidecode
import configparser # for reading settings file
import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)
import re   # for regular expressions purposes
import psycopg2 # for SQL interface
from datetime import datetime   # for getting timestamp and date
import traceback    # for printting exception traceback
import math

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

real_estates_column_names = [
    'fii_id',
    'description',
    'state_province',
    'goal',
    'area',
    ]

real_estates_indices_column_names  = [
    'estate_id',
    'timestamp',
    'vacancy',
    'non_payment'
]

verbosity = 1

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def getFiiRealEstatesDetailedInfo(fii_ticker):
    """Gets detailed information about real estates from a FII

    Args:
        fii_ticker (str): FII ticker for which real estates' detailed information should be gathered

    Returns:
        list: list of dictionaries with detailed information about all real estates bound to the given FII:
         description
         state/province
         goal
         (surface) area
         vacancy
         non-payment
    """
    
    realEstateDetailedInfo = []

    # With Beautiful Soup retrieves 0, 1 or many estates related to the given FII
    soup = BeautifulSoup(urlopen(Request(fiis_definition.FIIDetailedDataURLPart2.replace('FII_TICKER', fii_ticker), headers={"User-Agent": "Mozilla/5.0"})).read(), features='html.parser')
    try:
        portfolio_section = soup.find("div", attrs={'id':'portfolio-section'})  # Main div
        non_land_real_estates_section = portfolio_section.find('div', attrs={'class':'card-list-box navigation-dot-white', 'data-title': 'IMÓVEL'})  # non landscapes div
        land_real_estates_section = portfolio_section.find('div', attrs={'class':'card-list-box navigation-dot-white', 'data-title': 'TERRENO'})  # landscapes div
    except AttributeError:
        utilities.eprint(2, f'{fii_ticker} has no page with detailed real estates information')
        return

    if non_land_real_estates_section == None:
        return []

    # Gets all estates' descriptions
    descriptions = []
    for currentEstateDescription in non_land_real_estates_section.findAll('div', attrs={'class':'name'}):
        descriptions.append(unidecode(u'{0}'.format(currentEstateDescription.find('span').text)))   # Gets rid of special unicode characters

    # Gets all estates' states/provinces
    states_provinces = []
    for current_state_province in non_land_real_estates_section.findAll('strong', attrs={'class':'uf mr-1'}):
        states_provinces.append(current_state_province.text)
    
    # Gets all estates' goals
    goals = []
    for current_goal in non_land_real_estates_section.findAll('div', attrs={'class':re.compile(r'objective objective-[01]')}):
        goals.append(current_goal.find('strong', attrs={'class':'value'}).text)

    # Gets all estates' surface areas
    areas = []
    for current_area in non_land_real_estates_section.findAll('strong', attrs={'class':'value mt-0 w-55 justify-end d-flex'}):
        areas.append(float(current_area.find('span').text.replace('.', '').replace(',', '.').replace('m²', '')))

    # Gets all estates' vacancies and non-payment statuses
    vacancies = []
    non_payments = []
    i = 0
    for all_vacancy_non_payment_pairs in non_land_real_estates_section.findAll('div', attrs={'class':'d-flex justify-between align-items-center'}):
        for current_vacancy_non_payment_pair in all_vacancy_non_payment_pairs.findAll('strong', attrs={'class':'value'}):
            if i % 2 == 0:
                vacancies.append(float(current_vacancy_non_payment_pair.text.replace(',', '.').replace('%', '').replace('-', '-1')))
            else:
                non_payments.append(float(current_vacancy_non_payment_pair.text.replace(',', '.').replace('%', '').replace('-', '-1')))
            i = i + 1

    # Prepares the list of dictionaries with all estates' information
    try:
        for i in range(0, len(descriptions)):
            current_estate_dict = {}
            current_estate_dict['description'] = descriptions[i]
            current_estate_dict['state_province'] = states_provinces[i]
            current_estate_dict['goal'] = goals[i]
            current_estate_dict['area'] = areas[i]
            current_estate_dict['vacancy'] = vacancies[i]
            current_estate_dict['non_payment'] = non_payments[i]
            realEstateDetailedInfo.append(current_estate_dict)
    except IndexError as e:
        utilities.eprint(2, f'Problems recovering {fii_ticker} estates\n{traceback.format_exc()}')

    return realEstateDetailedInfo

def registerNewEstateIfNecessary(fii_id, ticker, estateInfoDictionary, pgsql):
    """Inserts a new Estate in the "estates" table, if it is not registered there yet
    fii_id FII identification in the reigistered_fiis table

    Args:
        fii_id (int): FII identification in the reigistered_fiis table
        ticker (str): FII ticker for which real estates' detailed information should be gathered
        estateInfoDictionary (dict): real estate information, containing the owner FII ID, its description, state/province, its goal and total area
        pgsql (postgres.Postgres): object with a valid connection to the SQL database

    Returns:
        int: just inserted estate id, or existing estate id, from the estates table
    """
    
    # first checks if given estate already exists
    pgsql.cur.execute("SELECT estate_id\n"
        "FROM real_estates\n"
        "WHERE description = %s AND area = %s AND fii_id = %s;",
        (estateInfoDictionary['description'],
         estateInfoDictionary['area'],
         fii_id, ))
    retrievedTuple = pgsql.cur.fetchone()
    
    # if the estate is not yet registered, inserts it in the estates table
    if retrievedTuple == None:
        estate_id = pgsql.exec_insert_query('real_estates',
            real_estates_column_names, 
            (fii_id, estateInfoDictionary['description'],
             estateInfoDictionary['state_province'],
             estateInfoDictionary['goal'],
             estateInfoDictionary['area'], ),
            'estate_id')
        pgsql.conn.commit()
        utilities.print_verbose(f'New real estate inserted for FII {ticker}', verbosity_level=1)
        return estate_id
    else:
        return retrievedTuple[0]

def updateFiiRealEstatesIndices(fii_id, ticker, estates_info_dict, pgsql):
    """Updates indices of a single real estate, related to a given FII, in the real_estates_indices table

    Args:
        fii_id (int): FII identification in the reigistered_fiis table
        ticker (str): FII ticker for which real estates' detailed information should be updated
        estates_info_dict (dict): real estate information, containing the owner FII ID, its description, state/province, its goal and total area
        pgsql (postgres.Postgres): object with a valid connection to the SQL database
    """
    
    # To make sure the correct real estate is picked from DB, rather than comparing description
    # and the FII to whose it belongs, also compares the area
    pgsql.cur.execute("SELECT rei.vacancy, rei.non_payment\n"
        "FROM real_estates_indices AS rei, real_estates AS re\n"
        "WHERE rei.estate_id = re.estate_id AND LOWER(re.description) = LOWER(%s) AND re.area = %s AND re.fii_id = %s\n"
        "ORDER BY rei.timestamp DESC",
        (estates_info_dict['description'],
         estates_info_dict['area'],
         fii_id, ))
    retrievedTuples = pgsql.cur.fetchone()
    
    # To pick the real estate ID it is necessary to check it apart of the real estate's indices, in
    # case there is no index bound to it yet
    pgsql.cur.execute(  'SELECT estate_id '
                        'FROM real_estates '
                        'WHERE description = %s AND area = %s AND fii_id = %s',
                        (estates_info_dict['description'],
                        estates_info_dict['area'],
                        fii_id ))
    estate_id = pgsql.cur.fetchone()[0]
    
    if (retrievedTuples == None or
        (retrievedTuples != None and
            (estates_info_dict['vacancy'] != retrievedTuples[0] or estates_info_dict['non_payment'] != retrievedTuples[1]))):
        
        values = (estate_id, datetime.now(), estates_info_dict['vacancy'], estates_info_dict['non_payment'])
        try:
            pgsql.exec_insert_query('real_estates_indices', real_estates_indices_column_names, values)
            pgsql.conn.commit()
            utilities.print_verbose(f'Vacancy/non-payment updated to {ticker}, real estate {estates_info_dict["description"]}, ID {estate_id}', verbosity_level=1)
        except psycopg2.errors.UniqueViolation:
            utilities.eprint(2, f'Error when updating {ticker}, estate id {estate_id}, estate description {estates_info_dict["description"]}. Duplicated entry.')
            pgsql.conn.rollback()

def updateRealEstatesIndicesForFiis(current_fii):
    """Updates real estates indices of a given FII

    Args:
        current_fii (tuple): FII information, with pattern (fii_id, ticker, historical_data_link)
    """
    
    # Connects to the datebase
    postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    postgres.connectToDatabase()

    # Picks real estates detailed information for the given FII
    current_fii_real_estates_dict = getFiiRealEstatesDetailedInfo(current_fii[1])
    if current_fii_real_estates_dict != None:
        for current_estate in current_fii_real_estates_dict:
            registerNewEstateIfNecessary(current_fii[0], current_fii[1], current_estate, postgres)
            updateFiiRealEstatesIndices(current_fii[0], current_fii[1], current_estate, postgres)
    
    # Disconnects from the database
    postgres.disconnectFromDatabase()

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()

    # second, picks the list of available FIIs, from "registered FIIs" table
    fiis_list = fiis_definition.get_registered_fiis_list()

    # third, retrieves quotes information from FII
    utilities.call_function_for_single_or_multithread(fiis_list, updateRealEstatesIndicesForFiis)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
