"""
Interface class for handling PostgreSQL databases
Author: Rozario Engenharia

First release: November 11th, 2021
Second release: November 20th, 2021
    Using Database super class
"""

import psycopg2 # for PotgreSQL interface
from .database import Database

class Postgres(Database):

    def __init__(self, dbname, hostname, postgres_port, user_name, user_passwd):
        Database.__init__(self, dbname, hostname, postgres_port, user_name, user_passwd)
    
    # connects to the database
    def connectToDatabase(self):
        self.conn = psycopg2.connect(dbname = self.dbname, user = self.user_name, password = self.user_passwd, host = self.hostname, port = self.postgres_port)
        self.cur = self.conn.cursor()

    # disconnects from the database
    def disconnectFromDatabase(self):
        self.cur.close()
        self.conn.close()

    # Executes an insertion query
    # @param table_name the table where data should be inserted
    # @param column_names list of columns names, where data should be inserted
    # @param data data list, to be inserted on addressed columns (must have the same size of column_names)
    # @param (optional) primary_key_label if primary key should be returned just after insertion in table
    # @param (optional) is_ignore_pk if True, does nothing in case of duplicate primary key(s)
    # @return the just inserted primary key (if primary_key_label parameter was supplied) or the SQL statement, if no primary key was supplied
    def exec_insert_query(self, table_name, column_names, data, primary_key_label=None, is_ignore_pk=False):
        SQL = ( "INSERT INTO %s (%s) VALUES(%s) %s %s;"
                % (table_name,
                ', '.join(column_names),
                ', '.join(['%s' for i in data]),
                ' RETURNING ' + primary_key_label if primary_key_label != None else '',
                ' ON CONFLICT DO NOTHING ' if is_ignore_pk else ''))
        self.cur.execute(SQL, data)
        if primary_key_label != None:
            return self.cur.fetchone()[0]
        else:
            return SQL

    # Executes an insertion query, ignoring any existing primary key
    # @param table_name the table where data should be inserted
    # @param column_names list of columns names, where data should be inserted
    # @param data data list, to be inserted on addressed columns (must have the same size of column_names)
    # @param (optional) primary_key_label if primary key should be returned just after insertion in table
    # @return the just inserted primary key (if primary_key_label parameter was supplied)
    def exec_insert_query_ignoring_repeated_pk(self, table_name, column_names, data, primary_key_label=None):
        #return self.exec_insert_query(table_name, column_names, data, primary_key_label=primary_key_label, is_ignore_pk=True)
        try:
            return self.exec_insert_query(table_name, column_names, data, primary_key_label)
        except (psycopg2.errors.UniqueViolation, psycopg2.errors.InFailedSqlTransaction) as e:
            # Evita que dados inseridos sejam duplicados na tabela
            return None
    
    # Retrieves data from a given table
    # @param table_name the table from where the data should be retrieved
    # @param column_names names of columns from where the data should be retrieved. Default: None. If None is this parameter's value, selects all columns (*)
    # @param where_conditions conditions to be assigned to the WHERE clause. Default: None
    # @param order_by_conditions conditions for ordering retrieved results. Default: None
    # @param group_by_conditions conditions for grouping retrieved results. Default: None
    # @return a list of data coming from the target table
    def select_data_from_table(self,
            table_name,
            column_names=None,
            where_conditions=None,
            order_by_conditions=None,
            group_by_conditions=None):

        column_names_str = '*'
        if column_names != None:
            column_names_str = ', '.join(f'{column}' for column in column_names)
        
        query = (f'SELECT {column_names_str}\n'
            f'FROM {table_name}\n'
            f'{"WHERE " + where_conditions if where_conditions != None else ""}\n'
            f'{"GROUP BY " + group_by_conditions if group_by_conditions != None else ""}\n'
            f'{"ORDER BY " + order_by_conditions if order_by_conditions != None else ""}')
        self.cur.execute(query)
        return self.cur.fetchall()
