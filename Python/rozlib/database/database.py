"""
Database super class, with common features for all engines (Postgre, MySQL, SQL Server, etc)

Author: Rozario Engenharia

First release: November 20th, 2021
"""

import abc

class Database(metaclass=abc.ABCMeta):
    # Connection information
    dbname = ''
    hostname = ""
    postgres_port = 0
    user_name = ''
    user_passwd = ''

    # Connection instance
    conn = None
    # Cursor instance
    cur = None

    def __init__(self, dbname, hostname, postgres_port, user_name, user_passwd):
        self.dbname = dbname
        self.hostname = hostname
        self.postgres_port = postgres_port
        self.user_name = user_name
        self.user_passwd = user_passwd

    # connects to the database
    @abc.abstractmethod
    def connectToDatabase(self):
        pass
    
    # disconnects from the database
    @abc.abstractmethod
    def disconnectFromDatabase(self):
        pass

    # Executes an insertion query
    # @param table_name the table where data should be inserted
    # @param column_names list of columns names, where data should be inserted
    # @param data data list, to be inserted on addressed columns (must have the same size of column_names)
    # @param (optional) primary_key_label if primary key should be returned just after insertion in table
    # @param (optional) is_ignore_pk if True, does nothing in case of duplicate primary key(s)
    # @return the just inserted primary key (if primary_key_label parameter was supplied) or the SQL statement, if no primary key was supplied
    @abc.abstractmethod
    def exec_insert_query(self, table_name, column_names, data, primary_key_label=None, is_ignore_pk=False):
        pass

    # Executes an insertion query, ignoring any existing primary key
    # @param table_name the table where data should be inserted
    # @param column_names list of columns names, where data should be inserted
    # @param data data list, to be inserted on addressed columns (must have the same size of column_names)
    # @param (optional) primary_key_label if primary key should be returned just after insertion in table
    # @return the just inserted primary key (if primary_key_label parameter was supplied)
    @abc.abstractmethod
    def exec_insert_query_ignoring_repeated_pk(self, table_name, column_names, data, primary_key_label=None):
        pass

    # Retrieves data from a given table
    # @param table_name the table from where the data should be retrieved
    # @param column_names names of columns from where the data should be retrieved. Default: None. If None is this parameter's value, selects all columns (*)
    # @param where_conditions conditions to be assigned to the WHERE clause. Default: None
    # @param order_by_conditions conditions for ordering retrieved results. Default: None
    # @param group_by_conditions conditions for grouping retrieved results. Default: None
    # @return a list of data coming from the target table
    @abc.abstractmethod
    def select_data_from_table(self,
            table_name,
            column_names=None,
            where_conditions=None,
            order_by_conditions=None,
            group_by_conditions=None):
        pass
