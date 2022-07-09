"""
Common features for FIIs data handling, throughout the solution

Author: Rozario Engenharia

First release: November 16th, 2021
"""

import configparser # for reading settings file
from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
import requests # for retrieving HTML data from the internet (web scraping)
import html # for unescaping HTML characters
import sys  # for command line arguments and for pointing rozlib library folder (path comes from config.ini file)
import re   # for regular expressions purposes
from datetime import datetime # for handling dates and timestamps
#from urllib.request import Request, urlopen

verbosity = 1

# Reads rozlib library path from config.ini file and import libraries packages
rozlib_path = ''
def add_rozlib_library_path():
    global rozlib_path
    config = configparser.ConfigParser()
    config.sections()
    config.read('../config.ini', encoding='utf-8')  # For notebooks (Jupyter, Google Colab)
    try:
        rozlib_path = config['GENERAL']['rozlibFolderPath']
    except KeyError:
        config.read('config.ini', encoding='utf-8') # For non notebooks (regular python)
        rozlib_path = config['GENERAL']['rozlibFolderPath']
    sys.path.insert(1, rozlib_path)
add_rozlib_library_path()

from rozlib.database.postgres import Postgres
from rozlib.util import Utilities
utilities = Utilities(verbosity)

# Pre defined SQL Queries
greatest_stock_market_sessions_retrieve_query = (
    'SELECT MAX(b.count) maximum FROM\n'
    '(\n'
        'SELECT COUNT(*) AS count\n'
        'FROM fiis_daily_quotation AS fdq\n'
        'WHERE date >= %s AND date <= %s\n'
        'AND fdq.fii_id IN (SELECT * FROM get_valid_fiis_ids_per_date(%s)\n'
    ')\n'
    'GROUP BY fdq.fii_id) b\n'
    'GROUP BY (b.count)\n'
    'ORDER BY count DESC LIMIT 1\n'
)
simetric_beta_calculation_query = (
    'SELECT fii.ticker, COVAR_POP(fii.last, ifix.last) / VARIANCE(ifix.last) AS "beta"\n'
    'FROM\n'
    '(\n'
        'SELECT fdq.last, fdq.date, rf.ticker, fdq.fii_id\n'
        'FROM fiis_daily_quotation AS fdq, registered_fiis AS rf\n'
        'WHERE fdq.fii_id=rf.fii_id AND LOWER(rf.ticker)<>\'ifix\'\n'
        'AND fdq.fii_id IN %s'
    ') AS fii,\n'
    '(\n'
        'SELECT fdq.last, fdq.date\n'
        'FROM fiis_daily_quotation AS fdq, registered_fiis AS rf\n'
        'WHERE fdq.fii_id=rf.fii_id AND LOWER(rf.ticker)=\'ifix\'\n'
    ') AS ifix\n'
    'WHERE fii.date=ifix.date AND\n'
    'fii.date >= %s AND fii.date <= %s\n'
    'GROUP BY (fii.ticker, fii.fii_id)\n'
    'ORDER BY "beta" DESC;'
)
dividend_yield_calculation_query = (
    'SELECT dividends.ticker, 100*(dividends.avg_dividend/quotations.avg_quotation) AS "dy"\n'
    'FROM\n'
    '(\n'
        'SELECT dq.fii_id, rf.ticker, AVG(dq.value) AS avg_dividend\n'
        'FROM dividend_quotations AS dq, registered_fiis AS rf, dividend_types AS dt\n'
        'WHERE dq.fii_id=rf.fii_id AND dq.type_id=dt.type_id AND\n'
        'dq.date_record >= date_trunc(\'day\', %s::DATE - interval \'1 year\') AND dq.date_record <= %s\n'
        'GROUP BY dq.fii_id, rf.ticker\n'
    ') AS dividends,\n'
    '(\n'
        'SELECT fdq.fii_id, rf.ticker, AVG(fdq.last) AS avg_quotation\n'
        'FROM fiis_daily_quotation AS fdq, registered_fiis AS rf\n'
        'WHERE fdq.fii_id=rf.fii_id AND\n'
        'fdq.date >= date_trunc(\'day\', %s::DATE - interval \'1 year\') AND fdq.date <= %s\n'
        'GROUP BY fdq.fii_id, rf.ticker\n'
    ') AS quotations\n'
    'WHERE dividends.fii_id=quotations.fii_id;'
)
beta_calculation_query = (
    'SELECT fii.ticker, COVAR_POP(fii.last, ifix.last) / VARIANCE(ifix.last) AS "beta"\n'
    'FROM\n'
    '(\n'
        'SELECT fdq.last, fdq.date, rf.ticker, fdq.fii_id\n'
        'FROM fiis_daily_quotation AS fdq, registered_fiis AS rf\n'
        'WHERE fdq.fii_id=rf.fii_id AND LOWER(rf.ticker)<>\'ifix\'\n'
        'AND fdq.fii_id IN %s'
    ') AS fii,\n'
    '(\n'
        'SELECT fdq.last, fdq.date\n'
        'FROM fiis_daily_quotation AS fdq, registered_fiis AS rf\n'
        'WHERE fdq.fii_id=rf.fii_id AND LOWER(rf.ticker)=\'ifix\'\n'
    ') AS ifix\n'
    'fii.date >= %s AND fii.date <= %s\n'
    'GROUP BY (fii.ticker, fii.fii_id)\n'
    'ORDER BY "beta" DESC;'
)
valid_fiis_ids_per_date = ''

class FIIs:
    # Global configuration definitions
    fiisListURL = ""
    FIIDetailedDataURLPart1 = ''
    fiisRegistrationComplementaryDataScriptPath = ''
    multithread = False
    configurationFilePath = '../config.ini' if Utilities.is_notebook() else 'config.ini'
    FIIDividendsHistory = ''
    FIIDetailedDataURLPart2 = ''
    FIIDetailedDataURLPart4 = ''

    # SQL variables
    dbname = ''
    hostname = ''
    postgres_port = 0
    user_name = ''
    user_passwd = ''
    psqlPath = ''
    # End of SQL variables

    def __get_new_sql_connection(self):
        pgsql = Postgres(self.dbname, self.hostname, self.postgres_port, self.user_name, self.user_passwd)
        pgsql.connectToDatabase()
        return pgsql

    # Gets a FII identification, from the "registered FIIs" table, given its ticker
    def get_fii_id_by_ticker(self, ticker, postgres=None):
        if postgres == None:
            pgsql = self.__get_new_sql_connection()
        else:
            pgsql = postgres
        try:
            fii_id = pgsql.select_data_from_table('registered_fiis', column_names=['fii_id'], where_conditions=f'LOWER(ticker) = LOWER(\'{ticker}\')')[0][0]
            pgsql.disconnectFromDatabase()
            return fii_id
        except IndexError:
            # FII is not yet registered
            return None
        finally:
            if postgres == None:
                pgsql.disconnectFromDatabase()

    # Retrieves configuration data from the application's .ini file
    def retrieveConfigurationFromINIFile(self):
        # retrieves settings from the configurations file
        config = configparser.ConfigParser()
        config.sections()
        config.read(self.configurationFilePath, encoding='utf-8')

        # FIIs list data
        self.fiisListURL = config['FIIS_LIST_RETRIEVAL']['FIIsListURL']
        self.FIIDetailedDataURLPart1 = config['FII_DETAILED_DATA']['FIIDetailedDataURLPart1']
        self.FIIDetailedDataURLPart2 = config['FII_DETAILED_DATA']['FIIDetailedDataURLPart2']
        self.FIIDetailedDataURLPart4 = config['FII_DETAILED_DATA']['FIIDetailedDataURLPart4']
        self.FIIDividendsHistory = config['FII_DETAILED_DATA']['FIIDividendsHistory']

        # SQL connection and scripting data
        self.dbname = config['SQL_SETTINGS']['dbname']
        self.hostname = config['SQL_SETTINGS']['host']
        self.postgres_port = int(config['SQL_SETTINGS']['serverPort'])
        self.user_name = config['SQL_SETTINGS']['username']
        self.user_passwd = config['SQL_SETTINGS']['password']
        self.psqlPath = config['SQL_SETTINGS']['psqlPath']
        self.fiisRegistrationComplementaryDataScriptPath = config['SQL_SETTINGS']['fiisRegistrationComplementaryDataScriptPath']

    # Retrieves a dictionary of available FIIs from www.fiis.com.br
    # @param postgresql object with a valid connection to the database
    # @return dictionary of all registered FIIs with:
    # ticker
    # link to more detailed information
    # description
    def get_fiis_dict(self):
        fiis_dict = {}

        # parses the html, looking for those places where the main information are located
        soup = Utilities.parse_web_page(self.fiisListURL)
        for div in soup.findAll('div', attrs={'class':'item'}):
            ticker = div.find('span', attrs={'class':'ticker'})
            link = div.find('a', href=True)
            description = div.find('span', attrs={'class':'name'})
            ticker = re.sub(r'\s*', '', ticker.text) # Make sure no white spaces are there
            fiis_dict[ticker] = {}
            fiis_dict[ticker]['link'] = link['href']
            fiis_dict[ticker]['description'] = description.text

        return fiis_dict
    
    def get_registered_fiis_list(self, ignore_fiis_without_detailed_info_weblink=True, only_valid_fiis=False):
        """Gets a list of registered FIIs, from registered_fiis table

        Args:
            ignore_fiis_without_detailed_info_weblink (bool, optional): if true, discards those FIIs without historical data link. Defaults to True.
            only_valid_fiis (bool, optional): if true, discards FIIs with no historical data in the past month, without dividend data, without stakeholders and without stock and net worth

        Returns:
            list: list of registered FIIs, from database
        """
        
        column_names=['fii_id', 'ticker', 'historical_data_link']
        
        # Connects to the datebase
        pgsql = self.__get_new_sql_connection()

        if only_valid_fiis:
            fiis_list = pgsql.select_data_from_table('get_valid_fiis()',
                column_names=column_names) 
        else:
            fiis_list = pgsql.select_data_from_table('registered_fiis',
                column_names=column_names,
                where_conditions='historical_data_link <> \'\'' if ignore_fiis_without_detailed_info_weblink else None)
        pgsql.disconnectFromDatabase()
        return fiis_list

    def getDetailedInformationFromFII(self, ticker, fiiInformationDictionary):
        """Retrieves detailed information from a given FII, given a link to a page with detailed information

        Parameters
        ----------
        ticker : str
            FII ticker to retrieve detailed data
        fiiInformationDictionary : dict
            Dictionary with general data about a FII
        
        Returns
        -------
        dict
            Dictionary with pattern {stockMarketSessionName, type, anbimaType, cvmRegistrationDate, quotesAmount, stakeholdersAmount, cnpj}
        """
        
        #fiiDetailedInfoPage = html.unescape(requests.get(fiiInformationDictionary['link']).text)
        
        fiiDetailedInfo = {}
        
        # parses the html, looking for those places where the main information are located
        #soup = BeautifulSoup(fiiDetailedInfoPage, features='html.parser')
        soup = Utilities.parse_web_page(fiiInformationDictionary['link'])
        for div in soup.findAll('div', attrs={'id':'informations--basic'}):
            span = BeautifulSoup(str(div), features='html.parser').findAll('span', attrs={'class':'value'})
            stockMarketSessionName = span[0].text
            type = span[1].text
            anbimaType = span[2].text
            cvmRegistrationDate = span[3].text
            quotesAmount = span[4].text
            stackeholdersAmount = span[5].text
            cnpj = span[6].text
            
            fiiDetailedInfo['stockMarketSessionName'] = stockMarketSessionName
            fiiDetailedInfo['type'] = type
            fiiDetailedInfo['anbimaType'] = anbimaType
            
            # checks whether the cvm registration date is absent. If it is, stores a standard date (01/01/1970) and prints a warning on 
            fiiDetailedInfo['cvmRegistrationDate'] = cvmRegistrationDate
            try:
                fiiDetailedInfo['cvmRegistrationDate'] = datetime.strptime(fiiDetailedInfo['cvmRegistrationDate'], "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                # TODO: fetch this information from fundamentus.com.br
                fiiDetailedInfo['cvmRegistrationDate'] = '1900-01-01'
                utilities.eprint(2, f"Date could not be retrieved for ticker {ticker}")
            
            fiiDetailedInfo['quotesAmount'] = re.sub('[^0-9]', '', quotesAmount)
            fiiDetailedInfo['stackeholdersAmount'] = re.sub('[^0-9]', '', stackeholdersAmount)
            
            # checks whether the information "cnpj" is absent. If it is, stores 0 for cnpj and prints a warning on screen
            fiiDetailedInfo['cnpj'] = re.sub('[^0-9]', '', cnpj)
            if len(fiiDetailedInfo['cnpj']) <= 0:
                fiiDetailedInfo['cnpj'] = '0'
                # TODO: fetch this information from fundamentus.com.br
                utilities.eprint(2, f"CNPJ could not be retrieved for ticker {ticker}")
        
        # parses the html, looking for those places where the extra information are located
        for div in soup.findAll('div', attrs={'id':'informations--extra'}):
            fiiDetailedInfo['notes'] = BeautifulSoup(str(div), features='html.parser').find('div', attrs={'class':'notas'}).text.replace("\n", "")
            fiiDetailedInfo['taxes'] = BeautifulSoup(str(div), features='html.parser').find('div', attrs={'class':'taxas'}).text.replace("\n", "")

        # parses the html, looking for those places where the FII's administrator information are located
        for div in soup.findAll('div', attrs={'class':'text-wrapper'}):
            try:
                fiiDetailedInfo['administratorName'] = BeautifulSoup(str(div), features='html.parser').find('span', attrs={'class':'administrator-name'}).text
                
                # checks whether the information "FII administrator cnpj" is absent. If it is, stores 0 for administrator cnpj and prints a warning on screen
                fiiDetailedInfo['administratorCNPJ'] = re.sub('[^0-9]', '', BeautifulSoup(str(div), features='html.parser').find('span', attrs={'class':'administrator-doc'}).text)
                if len(fiiDetailedInfo['administratorCNPJ']) <= 0:
                    fiiDetailedInfo['administratorCNPJ'] = '0'
                    utilities.eprint(2, f"Administrator CNPJ could not be retrieved for ticker {ticker}")
                break
            except AttributeError:
                pass
            
        return fiiDetailedInfo
    
    # Gets the FII type id, from the "FIIs types" table. Creates a new type if not exists yet
    # @param description the FII type description
    # @return the FII type id
    def get_fii_type_by_description(self, description):
        pgsql = self.__get_new_sql_connection()
        try:
            type_id = pgsql.select_data_from_table('fiis_types', ['type_id'], where_conditions=f'LOWER(description) = LOWER(\'{description}\')')[0]
        except IndexError:
            utilities.print_verbose(f'New FII type registered: {description}', verbosity_level=verbosity)
            type_id = pgsql.exec_insert_query('fiis_types', ['description'], [description], primary_key_label='type_id')
            pgsql.conn.commit()
            return type_id
        finally:
            pgsql.disconnectFromDatabase()
        return type_id
    
    # Gets Anbima type id, from "Anbima types" table. Creates a new type if not exists yet
    # @param description the Anbima type description
    # @return the Anbima type id
    def get_anbima_type_by_description(self, description):
        pgsql = self.__get_new_sql_connection()
        try:
            type_id = pgsql.select_data_from_table('anbima_types', ['type_id'], where_conditions=f'LOWER(description) = LOWER(\'{description}\')')[0]
        except IndexError:
            utilities.print_verbose(f'New Anbima type registered: {description}', verbosity_level=verbosity)
            anbima_type = pgsql.exec_insert_query('anbima_type', ['description'], [description], primary_key_label='type_id')
            pgsql.conn.commit()
            return anbima_type
        finally:
            pgsql.disconnectFromDatabase()
        return type_id
    
    def get_administrator_id_by_cnpj(self, cnpj, name):
        """Gets the FII Administrator id, from the "FIIs Administrators types" table. Creates a new administrator if not exists yet

        Args:
            cnpj (int): FII administrator CNPJ (identification number)
            name (str): FII administrator name

        Returns:
            int: administrator id (not CNPJ)
        """
        
        pgsql = self.__get_new_sql_connection()
        try:
            administrator_id = pgsql.select_data_from_table('fiis_administrators', ['fii_administrator_id'], where_conditions=f'cnpj = {cnpj}')[0]
        except IndexError:
            administrator_id = pgsql.exec_insert_query('fiis_administrators', ['cnpj', 'name'], [cnpj, name], primary_key_label='fii_administrator_id')
            pgsql.conn.commit()
            utilities.print_verbose(f'New FII administrator registered: CNPJ: {cnpj}\tName: {name}', verbosity_level=verbosity)
            return administrator_id
        finally:
            pgsql.disconnectFromDatabase()
        return administrator_id

    def get_fii_daily_quotation_for_date_interval(self, ticker, start_date=None, end_date=None):
        """Retrieves daily quotations for a given FII, from fiis_daily_quotation table, given its ticker and
        optional start date and end date

        Args:
            ticker (str): Ticker for which daily quotation should be retrieved
            start_date (datetime.date or str, optional): Date when first daily quotation should be retrieved. If None is supplied, picks 01/01/1990 as start date. Defaults to None.
            end_date (datetime.date or str, optional): Date when last daily quotation should be retrieved. If None is supplied, picks current date as end date. Defaults to None.

        Returns:
            list: list of quotations for the given FII in the given time interval
        """
        
        fii_id = self.get_fii_id_by_ticker(ticker)
        
        # Dates verification
        if start_date == None:
            start_date = '1990-01-01'
        if end_date == None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        
        # Connects to database
        pgsql = self.__get_new_sql_connection()
        
        retrieved_data = pgsql.select_data_from_table(
            'fiis_daily_quotation',
            column_names=['date',
                          'last',
                          'open',
                          'maximum',
                          'minimum',
                          'volume',
                          'variation'],
            where_conditions=f'fii_id = {fii_id} AND date >= \'{start_date}\' AND date <= \'{end_date}\'',
            order_by_conditions='date ASC')
        
        pgsql.disconnectFromDatabase()
        return retrieved_data
    
    def calculate_beta_for_time_interval(self, start_date=None, end_date=None, is_simetric_beta=False):
        """Calculates beta index from database, according to start and end dates

        Args:
            start_date (str or datetime.date, optional): start date for beta index calculation. If None is supplied, uses 01-01-1900. Defaults to None
            end_date (str or datetime.date, optional): end date for beta index calculation. If None is supplied, uses "today". Defaults to None
            is_simetric_beta (bool, optional): if True, picks only tickers and their betas for those tickers that joined the greatest amount of stock market sessions during the time interval. Defaults to False

        Returns:
            dict: dictionary with "ticker":"beta" values
        """
        
        # Connects to the datebase
        postgres = self.__get_new_sql_connection()
        
        # Dates verification
        if start_date == None:
            start_date = '1990-01-01'
        if end_date == None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        
        # In case of simetric beta calculation, some steps come first
        if is_simetric_beta:
            # Picks the amount of stock market sessions for the FII(s) that joined these sessions the most,
            # during the time interval
            postgres.cur.execute(greatest_stock_market_sessions_retrieve_query,
                                 (start_date, end_date, end_date, ))
            maximum_stock_market_sessions_in_interval = postgres.cur.fetchone()[0]
            
            # Now picks those FII(s) (identifications) that joined this amount of stock market sessions during the time interval
            postgres.cur.execute('SELECT c.fii_id FROM\n'
		  	'(\n'
                'SELECT b.fii_id, MAX(b.count) AS maximum FROM\n'
                '(\n'
                    'SELECT COUNT(*) AS count, fdq.fii_id\n'
                    'FROM fiis_daily_quotation AS fdq\n'
                    'WHERE date >= %s AND date <= %s\n'
                    'AND fdq.fii_id IN (SELECT * FROM get_valid_fiis_ids_per_date(%s))\n'
                    'GROUP BY fdq.fii_id\n'
                ') b\n'
                'GROUP BY (count, b.fii_id)\n'
			') c\n'
	 		'WHERE c.maximum=%s\n',
            (start_date, end_date, end_date, maximum_stock_market_sessions_in_interval, ))
            target_fiis_ids = postgres.cur.fetchall()
        
        # Calculates beta itself
        postgres.cur.execute(simetric_beta_calculation_query if is_simetric_beta else beta_calculation_query,
                             (tuple(target_fiis_ids) if is_simetric_beta else None, start_date, end_date ))
                
        # Creates a dict with "ticker":"beta" information, for calculated beta indices
        retrieved_data = postgres.cur.fetchall()
        beta_dict = {key: value for (key, value) in retrieved_data}
        
        # Disconnects from the database
        postgres.disconnectFromDatabase()
        
        return beta_dict
    
    def get_dy_for_interval(self, start_date=None, end_date=None):
        """Calculates DY (Dividend Yield) for all valid FIIs in database
        from given date to one year previous to this date
        
        Args:
            start_date (str or datetime.date, optional): start date for DY calculation. If None is supplied, uses 01-01-1900. Defaults to None
            end_date (str or datetime.date, optional): end date for DY calculation. If None is supplied, uses "today". Defaults to None

        Returns:
            dict: dictionary with "ticker":"DY" values
        """
        
        # Connects to the datebase
        postgres = self.__get_new_sql_connection()
        
        # Dates verification
        #if start_date == None:
        #    start_date = '1990-01-01'
        #if end_date == None:
        #    end_date = datetime.today().strftime('%Y-%m-%d')
        
        # Calculates Dividend Yield itself
        postgres.cur.execute(dividend_yield_calculation_query,
                             (end_date, end_date, end_date, end_date, ))
        
        # Creates a dict with "ticker":"DY" information, for retrieved Dividend Yield
        retrieved_data = postgres.cur.fetchall()
        dy_dict = {key: value for (key, value) in retrieved_data}
        
        # Disconnects from the database
        postgres.disconnectFromDatabase()
        
        return dy_dict
    
    def get_dy(self, start_date=None, end_date=None):
        """Calculates DY (Dividend Yeld) for all valid FIIs in database
        DY is based upon a one year interval dividend payments
        
        Args:
            start_date (str or datetime.date, optional): start date for DY calculation. Defaults to None.
            end_date (str or datetime.date, optional): end date for DY calculation. Defaults to None.

        Returns:
            dict: dictionary with "ticker":"DY" values
        """
        
        # Connects to the datebase
        postgres = self.__get_new_sql_connection()
        
        postgres.cur.execute(
            'SELECT ticker, avg_dividend FROM get_yearly_dividends_per_fii()\n'
            'WHERE fii_id IN (SELECT * FROM get_valid_fiis_ids())\n'
            'ORDER BY avg_dividend DESC\n')
        
        # Creates a dict with "ticker":"DY" information, for retrieved Dividend Yield
        retrieved_data = postgres.cur.fetchall()
        dy_dict = {key: value for (key, value) in retrieved_data}
        
        # Disconnects from the database
        postgres.disconnectFromDatabase()
        
        return dy_dict
    
# Primitive Indicator class definition, such as stakeholders and quotes (amounts)
class PrimitiveIndicator:

    # Utilities object
    utilities = None

    # Library with common methods for all information related to FIIs
    fiislib = None

    table_columns = (
        'fii_id',
        'date',
        'amount',
    )
    table_name = ''

    # The index of 'span' markup, from www.fiis.com.br, where the target information is stored
    # 4: for quotes amount
    # 5: for stakeholders amount
    span_markup_indicator_index = 0

    def __init__(self, utilities, fiislib, table_name, span_markup_indicator_index, table_columns=None):
        self.utilities = utilities
        if table_columns != None:
            self.table_columns = table_columns
        self.table_name = table_name
        self.span_markup_indicator_index = span_markup_indicator_index
        self.fiislib = fiislib

    # Gets quotes information from www.fiis.com.br
    # @param fii_ticker the ticker from which The Indicator amount should be retrieved
    # @return amount of Indicator for given FII
    def get_quotes_amount(self, fii_ticker):
        #fiiDetailedInfoPage = html.unescape(requests.get(self.fiislib.FIIDetailedDataURLPart1.replace('FII_TICKER', fii_ticker)).text)
        
        # parses the html, looking for those places where the main information are located
        #soup = BeautifulSoup(fiiDetailedInfoPage, features='html.parser')
        soup = Utilities.parse_web_page(self.fiislib.FIIDetailedDataURLPart1.replace('FII_TICKER', fii_ticker))
        div = soup.find('div', attrs={'id':'informations--basic'})
        span = BeautifulSoup(str(div), features='html.parser').findAll('span', attrs={'class':'value'})
        #return re.sub('[^0-9]', '', span[self.span_markup_indicator_index].text)
        return span[self.span_markup_indicator_index].text

    # Updates quotes information (now only the amount) for a given FII
    # @param fii_ticker FII ticker
    def update_quotes_information(self, fii_ticker):
        pgsql = Postgres(self.fiislib.dbname, self.fiislib.hostname, self.fiislib.postgres_port, self.fiislib.user_name, self.fiislib.user_passwd)
        pgsql.connectToDatabase()

        # Gets the last record from the "quotes" table, to check if the amount of quotes changed (or not)
        pgsql.cur.execute(f'SELECT {self.table_columns[2]}, fii_id\n'
            f'FROM {self.table_name} NATURAL JOIN registered_fiis\n'
            f'WHERE LOWER(ticker)=LOWER(\'{fii_ticker}\')\n'
            f'ORDER BY {self.table_columns[1]} DESC')
        retrievedTuple = pgsql.cur.fetchone()

        # If no tuples were retrieved from the "indicator" table, this means there are no records
        # at this table for this FII. A new record should be registered then
        fii_id = 0
        previous_indicator_amount = 0
        if retrievedTuple == None:
            fii_id = self.fiislib.get_fii_id_by_ticker(fii_ticker)
            if fii_id == None:
                return
        else:
            previous_indicator_amount = retrievedTuple[0]
            fii_id = retrievedTuple[1]
        
        # Gets current amount of quotes
        try:
            indicator_amount = int(self.get_quotes_amount(fii_ticker).replace('.',''))
            # If there is a difference in the amount of quotes, or if there is no record for the given FII,
            # a new record is registered
            if (retrievedTuple == None) or (previous_indicator_amount != indicator_amount):
                pgsql.exec_insert_query(self.table_name,
                    self.table_columns,
                    [fii_id, datetime.today(), indicator_amount])
                pgsql.conn.commit()
                self.utilities.print_verbose(f'New {self.table_name} amount inserted to FII {fii_ticker}', verbosity_level=1)
        except IndexError:
            self.utilities.eprint(1, f"Quotes/stakeholders amount could not be retrieved for ticker {fii_ticker}")
        pgsql.disconnectFromDatabase()
