import psycopg2
import os
import time
import lib.string_operations as stringopc
from utils.errors import *


class GraphDB:
    def __init__(self, hostname, db_name, user, password=None):
        self.host = hostname
        self.db_default = 'postgres'
        self.db_name = db_name
        self.user = user
        self.password = password

        self.db_init = True
        self.db_running_status = False
        self.conn_db_name = None
        self.conn_status = False
        self.db_cursor = None
        self.db_conn = None

    def start_db(self):
        if not self.db_running_status:
            self.start_postgresql_wait(1)
            self.db_running_status = True
        else:
            pass

    def close_db(self):
        if self.db_running_status:
            self.close_conn()
            self.stop_postgresql_wait(1)
            self.db_running_status = False
        else:
            pass

    def get_cursor(self, db):
        if not self.db_running_status:
            msg = "Need to start database!"
            throw_error(msg, InvalidInputException)

        user = self.user
        pwd = self.password

        if self.conn_db_name != db:
            if self.conn_status:
                self.close_conn()

            self.db_conn = psycopg2.connect(database=db, user=user, password=pwd)
            self.db_conn.autocommit = True
            self.db_cursor = self.db_conn.cursor()

            self.conn_db_name = db
            self.conn_status = True
        else:
            if not self.conn_status:
                self.db_conn = psycopg2.connect(database=db, user=user, password=pwd)
                self.db_conn.autocommit = True
                self.db_cursor = self.db_conn.cursor()
                self.conn_status = True

    def close_conn(self):
        if self.db_running_status:
            if self.conn_status:
                self.db_conn.close()
                self.conn_status = False
            self.db_cursor = None
            self.db_conn = None
        else:
            pass

    def get_db_name(self, db_name):
        if not db_name:
            return self.db_name
        else:
            return db_name

    def start_postgresql_wait(self, wait_time):
        start_command = 'pg_ctl -D /usr/local/var/postgres start'
        os.popen(start_command)
        time.sleep(wait_time)

    def stop_postgresql_wait(self, wait_time):
        stop_command = 'pg_ctl -D /usr/local/var/postgres stop'
        os.popen(stop_command)
        time.sleep(wait_time)

    def add_extension_timescale(self, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        add_extension_command = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
        self.db_cursor.execute(add_extension_command)

    def drop_extension_timescale(self, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        add_extension_command = "DROP EXTENSION timescaledb CASCADE;"
        self.db_cursor.execute(add_extension_command)

    def check_db(self, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(self.db_default)

        select_db = 'SELECT datname FROM pg_database WHERE ' \
                    'datistemplate = false;'
        self.db_cursor.execute(select_db)
        fetch = self.db_cursor.fetchall()

        list_db = [fetch[i][0] for i in range(len(fetch))]

        if db_name in list_db:
            return True
        else:
            return False

    def create_db(self, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(self.db_default)

        try:
            self.db_cursor.execute("CREATE DATABASE %s;" % db_name)
            logging.info("Created a database %s" % db_name)
        except InvalidInitializationDB:
            self.close_conn()
            print("database: ", db_name)
            msg = "Has error to create a database!"
            throw_error(msg, InvalidInitializationDB)

    def drop_db(self, db_name):
        if self.check_db(db_name):
            self.get_cursor(self.db_default)
            self.db_cursor.execute("DROP DATABASE %s;" % db_name)

    def check_table(self, table_name, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        check_table_command = "select exists(select * from information_schema.tables where table_name=%s)"
        self.db_cursor.execute(check_table_command, (table_name,))

        if self.db_cursor.fetchone()[0]:
            return True
        else:
            return False

    def create_table(self, table_name, headers, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        headers_concatenation = "("
        for position in range(len(headers)):
            if len(headers[position]) != 3:
                msg = "Given headers are not correct"
                logging.error(msg)
            else:
                for item in headers[position]:
                    if item:
                        headers_concatenation = headers_concatenation + " " + item
                headers_concatenation = headers_concatenation + ","
        headers_concatenation = headers_concatenation + ")"
        headers_concatenation = stringopc.remove_char_of_string(1, headers_concatenation)
        headers_concatenation = stringopc.remove_char_of_string(len(headers_concatenation) - 2, headers_concatenation)

        create_table_command = "CREATE TABLE IF NOT EXISTS " + table_name + headers_concatenation
        self.db_cursor.execute(create_table_command)

    def create_hypertable(self, table_name, headers, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        headers_concatenation = "("
        for position in range(len(headers)):
            if len(headers[position]) != 3:
                msg = "Given headers are not correct"
                logging.error(msg)
            else:
                for item in headers[position]:
                    if item:
                        headers_concatenation = headers_concatenation + " " + item
                headers_concatenation = headers_concatenation + ","
        headers_concatenation = headers_concatenation + ")"
        headers_concatenation = stringopc.remove_char_of_string(1, headers_concatenation)
        headers_concatenation = stringopc.remove_char_of_string(len(headers_concatenation) - 2, headers_concatenation)

        create_table_command = "CREATE TABLE IF NOT EXISTS " + table_name + headers_concatenation
        create_hypertable_command = "SELECT CREATE_HYPERTABLE ('" + table_name + "', '" + headers[0][0] + "')"

        self.db_cursor.execute(create_table_command)
        self.db_cursor.execute(create_hypertable_command)

    def drop_table(self, table_name, db_name=''):
        db_name = self.get_db_name(db_name)

        if self.check_table(table_name, db_name):
            self.get_cursor(db_name)
            self.db_cursor.execute("DROP TABLE %s;" % table_name)

    def query_items_with_command(self, query_command, db_name=''):
        db_name = self.get_db_name(db_name)
        self.get_cursor(db_name)

        results = None
        try:
            self.db_cursor.execute(query_command)
            results = self.db_cursor.fetchall()
        except InvalidInputException:
            self.close_conn()
            msg = "Given query command has error!"
            throw_error(msg, InvalidInputException)
        return results

    def insert_item(self, headers, values, table_name, db_name=''):
        db_name = self.get_db_name(db_name)
        try:
            self.get_cursor(db_name)
            self.db_cursor.execute("INSERT INTO %s %s VALUES %s;" % (table_name, headers, values))
        except InvalidInsertItemDB:
            self.close_conn()
            msg = "Insert item to DB has error!"
            throw_error(msg, InvalidInsertItemDB)

    def update_item_with_query_content(self, query_content, update_content, table_name, db_name=''):
        db_name = self.get_db_name(db_name)
        if self.check_table(table_name, db_name):
            self.get_cursor(db_name)
            update_item_command = "UPDATE " + table_name + " SET " + update_content[0] + " = " + update_content[1] + \
                                  " WHERE time = " + query_content[0] + " AND " + query_content[1] + \
                                  " = " + query_content[2] + ";"
            self.db_cursor.execute(update_item_command)

    def delete_item_with_query_content(self, query_content, table_name, db_name=''):
        db_name = self.get_db_name(db_name)
        if self.check_table(table_name, db_name):
            self.get_cursor(db_name)
            delete_item_command = "DELETE FROM " + table_name + " WHERE " + \
                                  query_content[0] + " = " + query_content[1]
            self.db_cursor.execute(delete_item_command)
