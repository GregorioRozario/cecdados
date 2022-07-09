#!/usr/bin/python3

"""
Registers news related to the IFIX index, classifying them as GOOD or BAD (here there is no NEUTRAL news)
"""
    
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

# Constants
verbosity = 1
tickers_seeker_regex = r'(?:<tr><td>)(\D{4}\d{2}\d?)(?:<\/td><td>R\$\s?\d*,\d*<\/td><td>\D?\s?\d*,\d*%<\/td><\/tr>)'
# end of constants

# Globals
news_categories = {}

# If a command line argument called multithread was passed as True, parallel executions will take place
isMultiThread = False

# Utilities object
utilities = Utilities(verbosity)

# Library with common methods for all information related to FIIs
fiis_definition = fiislib.FIIs()

def get_ifix_news_URLs():
    """Retrieves a list of URLs with IFIX related news

    Returns:
        list: list of URLs with IFIX related news
    """
    
    ifix_URLs = []
    
    # Gathers information from website
    ifix_news_info_page = urlopen(Request('https://www.euqueroinvestir.com/?s=ifix', headers={"User-Agent": "Mozilla/5.0"})).read()
    
    # parses html file, looking for those places where main information is located
    soup = BeautifulSoup(ifix_news_info_page, features='html.parser')
    
    for current_ifix_news_link in soup.findAll('p', attrs={'class':'article-teaser-author'}):
        current_ifix_news_url = current_ifix_news_link.find('a')['href']
        ifix_URLs.append(current_ifix_news_url)
    
    return ifix_URLs

def get_highest_lowest_quotations(ifix_news_url):
    """Gets the best and worst FIIs in this news (article)

    Args:
        ifix_news_url (str): URL for the news to be evaluated

    Returns:
        (list, list): a tuple with two lists: first with 5 best FIIs, second with 5 worst
    """
    
    highest_quotation_fiis_ids = []
    lowest_quotation_fiis_ids = []
    
    # Gathers information from website
    ifix_news_info_page_text = urlopen(Request(ifix_news_url, headers={"User-Agent": "Mozilla/5.0"})).read().decode('utf-8')
    
    # Use a regex pattern to seek for the best and worst FIIs quotations for the IFIX index
    pattern = re.compile(tickers_seeker_regex)
    
    # Store first 5 tickers as 5 best and 5 last as five worst
    i = 0
    for match in pattern.finditer(ifix_news_info_page_text):
        if i < 5:
            highest_quotation_fiis_ids.append(match.groups()[0])
        else:
            lowest_quotation_fiis_ids.append(match.groups()[0])
        i = i + 1
    
    return (highest_quotation_fiis_ids, lowest_quotation_fiis_ids)

def get_article_date(ifix_article_url):
    """Get the date this news (article) was issued

    Args:
        ifix_article_url (str): URL for the news to be evaluated

    Returns:
        str: date in the format d/m/y
    """
    
    # Gathers information from website
    ifix_article_info_page = urlopen(Request(ifix_article_url, headers={"User-Agent": "Mozilla/5.0"})).read()
    
    # parses html file, looking for those places where main information is located
    soup = BeautifulSoup(ifix_article_info_page, features='html.parser')
    
    return soup.find('span', attrs={'class':'article-date-day'}).text

def get_news_categories_codes():
    """Retrieves all news' categories codes (good, bad, neutral...) and store these categories in a dictionary
    with pattern "category_label:category_code". Store this dictionary in a global variable called news_categories
    """
    
    global news_categories
    
    # Connects to the datebase
    postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    postgres.connectToDatabase()
    
    postgres.cur.execute("SELECT category_id, category_label\n"
            "FROM news_categories\n")
    retrievedData = postgres.cur.fetchall()
    for current_category in retrievedData:
        news_categories[current_category[1]] = current_category[0]
    
    postgres.disconnectFromDatabase()

def register_new_article_if_not_exists(postgres, url, tickers, category_code):
    """Registers a new article (news) if this article is not yet registered

    Args:
        postgres (rozlib.database.postgres.Postgres): valid connection to Postgres database with "news" table
        url (str): URL for the article
        tickers (list): list of tickers associated to this article
        category_code (int): category code (good, bad, neutral...)
    """
    
    # If no ticker is assciated to this article, there is nothing left to do here (information from URL, not database)
    if tickers == None or len(tickers) == 0:
        return
    
    # Checks whether this news (article) is already registered in the "news" table with given category
    postgres.cur.execute("SELECT *\n"
            "FROM news\n"
            "WHERE url=%s AND category_id=%s",
            (url, category_code,))
    retrievedData = postgres.cur.fetchall()
    
    # If article is not yet registered, registers it (using a function bound to the database)
    if len(retrievedData) == 0:
        news_date = datetime.datetime.strptime(get_article_date(url), "%d/%m/%y").strftime("%Y-%m-%d")
        postgres.cur.execute("SELECT insert_news(%s, %s, %s, %s)",
                             (news_date, url, category_code, tickers, ))
        postgres.conn.commit()

def register_article(current_news_url):
    """Registers a single article. No validation whether article exists or not is performed here

    Args:
        current_news_url (str): article (news) URL to be registered
    """
    
    # Connects to the datebase
    postgres = Postgres(fiis_definition.dbname, fiis_definition.hostname, fiis_definition.postgres_port, fiis_definition.user_name, fiis_definition.user_passwd)
    postgres.connectToDatabase()
    
    # Gets best and worst tickers related to current article
    (current_highest, current_lowest) = get_highest_lowest_quotations(current_news_url)
    
    # Registers best tickers associated to this article
    register_new_article_if_not_exists(postgres, 
                                        current_news_url,
                                        current_highest,
                                        news_categories['good'])
    
    # Registers worst tickers associated to this article
    register_new_article_if_not_exists(postgres, 
                                        current_news_url,
                                        current_lowest,
                                        news_categories['bad'])
    
    postgres.disconnectFromDatabase()

def main():
    # first, retrieves configuration data from the .ini file
    fiis_definition.retrieveConfigurationFromINIFile()
    
    # second, retrieves news categories codes
    get_news_categories_codes()
    
    # third, retrieves all IFIX related news URLs and registers those news not yet in the database
    # inserts up to date dividend information, one by one, for each FII
    news_URLs = get_ifix_news_URLs()
    utilities.call_function_for_single_or_multithread(news_URLs, register_article)

if __name__ == '__main__':
    utilities.call_function_with_elapsed_time(main)
