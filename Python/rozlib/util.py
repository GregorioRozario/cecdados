"""
Utilities class with helpful methods
Author: Rozario Engenharia

First release: November 11th, 2021
Second release: March 21st, 2022
    Added logging utilities (to console and file)
"""

import sys  # for command line arguments and stderr prints
from concurrent.futures import ThreadPoolExecutor   # for multithreading purposes
import traceback    # for printing exceptions info on stdout
import time # for benchmarking purposes
from datetime import date   # for getting timestamp and date
import datetime # for handling dates and timestamps
import concurrent.futures   # for multi threading purposes
import argparse  # for command line arguments
import colorama # for painting terminal texts
import termcolor    # for painting terminal texts
import os   # for dealing with OS specific tasks
import requests # for web pages reading
from bs4 import BeautifulSoup   # for parsing html pages (web scraping)
from prettytable import PrettyTable # For printting tabular data
import logging  # For log purposes
import re

# Class to make it possible printing exceptions in multithreaded systems
class ThreadPoolExecutorStackTraced(ThreadPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""

        return super(ThreadPoolExecutorStackTraced, self).submit(
            self._function_wrapper, fn, *args, **kwargs)

    def _function_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception

        """
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise sys.exc_info()[0](traceback.format_exc())  # Creates an
                                                             # exception of the
                                                             # same type with the
                                                             # traceback as
                                                             # message

class Utilities:
    verbosity = 0
    max_workers = 20
    multithread = False
    __console_logger = None
    __file_logger = None
    
    def __init__(self, multithread=False, verbosity=0, max_workers=20):
        self.verbosity = verbosity
        self.max_workers = max_workers
        self.multithread = multithread
        colorama.init()
        if not Utilities.is_notebook():
            self.__initParameters()

    def eprint(self, verbosity_level=1, *args, **kwargs):
        """Prints to stderr

        Args:
            verbosity_level (int, optional): Minimum verbosity level to print this message. Defaults to 1.
        """
        
        if self.verbosity >= verbosity_level:
            termcolor.cprint(*args, color="red", file=sys.stderr, end='\r\n', flush=True, **kwargs)

    def print_verbose(self, message='', end='\n', flush=False, verbosity_level=0):
        """Prints a message to stdout, according to the verbosity level

        Args:
            message (str, optional): Message to be printed. Can be an f-string. Standard: ''. Defaults to ''.
            end (str, optional): Message's end character. Defaults to '\n'.
            flush (bool, optional): Flag indicating whether stdout buffer should be flushed. Defaults to False.
            verbosity_level (int, optional): Minimum verbosity level for the message to be printed. Defaults to 0.
        """
        if self.verbosity >= verbosity_level:
            print(message, end=end, flush=flush)
    
    def print_progress_percentage(self, progress_value):
        """Prints a percentage progress value on screen

        Args:
            progress_value (float): absolute progress value (not in %)
        """
        
        self.print_verbose(f'{(100 * progress_value):.2f}%', end="\r", flush=True, verbosity_level=self.verbosity)
    
    def call_function_with_elapsed_time(self, func, *args, **kwargs):
        """Calls a function/method and calculates how long it takes to execute it

        Parameters
        ----------
        func : function
            The function/method to be called

        Returns
        -------
        any
            Returned value from function/method func
        """
        
        initTime = time.time()
        retVal = func(*args, **kwargs)
        endTime = time.time()
        totalSeconds = endTime - initTime
        self.print_verbose('', verbosity_level=self.verbosity)
        self.print_verbose(f'Elapsed time: {datetime.timedelta(seconds=totalSeconds)}', verbosity_level=self.verbosity)
        return retVal
    
    def call_function_for_single_or_multithread(self, iterable, func, *args, **kwargs):
        """Executes a function/method with one or multiple threads
        
        Parameters
        ----------
        iterable : iterable
            A list, set or tuple the called function should iterate through
        func : function
            The function/method to be called
        
        Returns
        -------
        any
            Returned value from function/method func
        """
        
        k = 1   # For progress display
        return_values = []
        if self.multithread == False:
            for current_iteration in iterable:
                return_values.append(func(current_iteration, *args, **kwargs))
                self.print_progress_percentage(k / len(iterable))
                k = k + 1
        else:
            with ThreadPoolExecutorStackTraced(max_workers=self.max_workers) as executor:
                futures = []
                for current_iteration in iterable:
                    #self.print_verbose(f'Starting new thread {threading.current_thread().ident}', verbosity_level=self.verbosity)
                    currentFuture = executor.submit(func, current_iteration, *args, **kwargs)
                    futures.append(currentFuture)
                for future in concurrent.futures.as_completed(futures):
                    self.print_progress_percentage(k / len(futures))
                    k = k + 1
                
                    # appending results to return
                    futureResult = future.result()
                    if futureResult != None:
                        return_values.append(futureResult)
                    #else:
                    #    return_values.append(futureResult.get())
        self.print_verbose('', verbosity_level=self.verbosity)
        return return_values

    def __initParameters(self):
        """Initializes command line parameters (if any)
        """
        
        # Read named command line parameters
        parser = argparse.ArgumentParser()

        # Is multithread?
        parser.add_argument('multithread', action="store", default=False, nargs='?')
        # Verbosity
        parser.add_argument('--verbosity', '-v', type=int, action="store", default=0, nargs='?')
        args = parser.parse_args()
        self.multithread = args.multithread
        self.verbosity = int(args.verbosity)
    
    def is_notebook():
        """Checks whether Python is running upon a notebook (Jupyter, Google Colab)

        Returns:
            bool: True, if running on a notebook
        """
        
        try:
            shell = get_ipython().__class__.__name__
            if shell == 'ZMQInteractiveShell':
                return True   # Jupyter notebook or qtconsole
            elif shell == 'TerminalInteractiveShell':
                return False  # Terminal running IPython
            else:
                return False  # Other type (?)
        except NameError:
            return False      # Probably standard Python interpreter
    
    def clearConsole():
        """Clears console (command prompt for Windows or terminal for Linux)
        """
        
        os.system('cls' if os.name in ('nt', 'dos') else 'clear') # If Machine is running on Windows, use cls
    
    def get_timestamp_refenced_to_year(date_to_check):
        """Gets a timestamp reference to the beginning of the year (year to date, YTD), that is, ignoring the year itself.
        This reference is the amount of seconds since January 1st 00:00:00

        Parameters
        ----------
        date_to_check : datetime or date
            The timestamp or date to be referenced to January 1st 00:00:00
            
        Returns:
        -------
        int
            Amount of seconds since the January 1st 00:00:00 until date_to_check
        """
        
        #return date_to_check.strftime("%m%d%H%M%S")
        try:
            return int((date_to_check - datetime.datetime(date_to_check.year, 1, 1, 0, 0, 0, 0)).total_seconds())
        except TypeError:
            return int((date_to_check - datetime.date(date_to_check.year, 1, 1)).total_seconds())
    
    def get_web_page_title(web_page_url):
        return Utilities.parse_web_page(web_page_url).title.string
    
    def parse_web_page(web_page_url, replacements=None, use_full_headers=False):
        """
        Parses a web page, given its URL, using an engine to by pass web scrapping

        Parameters
        ----------
        web_page_url : str
            Web page URL (with http or https prefix)
        replacements : list, optional
            List of tuples with strings to be replaced and their replacement. Defaults to None
        use_full_headers : boolean, optional
            If True, uses a full set of headers to bypass HTTP(S) servers' protection against web scrapping. Defaults to False

        Returns:
        -------
        bs4.BeautifulSoup
            Object with parsed HTML markups
        """
        
        if use_full_headers:
            headers = {'authority': 'www.dickssportinggoods.com',
                        'pragma': 'no-cache',
                        'cache-control': 'no-cache',
                        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
                        'sec-ch-ua-mobile': '?0',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'sec-fetch-site': 'none',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-user': '?1',
                        'sec-fetch-dest': 'document',
                        'accept-language': 'en-US,en;q=0.9',}
            
        else:
            headers={'User-Agent': 'Mozilla/5.0'}
        
        session = requests.Session()
        website_page = session.get(web_page_url, headers=headers)
        website_content = website_page.text
        if replacements != None:
            for old_str, replacement in replacements:
                website_content = website_content.replace(old_str, replacement)
            website_content = re.sub('\s{2,}', ' ', re.sub(r'(:?\r)*(:?\n)*(:?\t)*', '', website_content))
        return BeautifulSoup(website_content, features='html.parser')
    
    def get_tabular_data(header_list, data_list):
        """
        Prints data in tabular pattern

        Parameters
        ----------
        header_list : list
            List of table header
        data_list : list
            List of lists of data to be printed on table

        Returns:
        -------
        prettytable.PrettyTable
            Data to be printed on console
        """
        
        t = PrettyTable(header_list)
        for current_row in data_list:
            t.add_row(current_row)
        return t
    