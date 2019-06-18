import logging
import psycopg2
import os
import time
import lib.string_operations as stringopc


logging.basicConfig(level=logging.INFO)


class GraphDB:
    def __init__(self, hostname, db_name, user, password=None):
        self.host = hostname
        self.db_default = 'postgres'
        self.db_name = db_name
        self.user = user
        self.password = password

    def start_postgresql_wait(self, wait_time):
        start_command = 'pg_ctl -D /usr/local/var/postgres start'
        os.popen(start_command)
        time.sleep(wait_time)

    def stop_postgresql_wait(self, wait_time):
        stop_command = 'pg_ctl -D /usr/local/var/postgres stop'
        os.popen(stop_command)
        time.sleep(wait_time)

    def get_db_name(self, db_name):
        if not db_name:
            return self.db_name
        else:
            return db_name

    def check_db(self, db_name=''):
        db_name = self.get_db_name(db_name)

        conn = psycopg2.connect(database=self.db_default, user=self.user, password=self.password)
        cursor = conn.cursor()

        select_db = 'SELECT datname FROM pg_database WHERE ' \
                    'datistemplate = false;'
        cursor.execute(select_db)
        fetch = cursor.fetchall()

        list_db = [fetch[i][0] for i in range(len(fetch))]
        conn.close()

        if db_name in list_db:
            return True
        else:
            return False

    def create_db(self, db_name=''):
        db_name = self.get_db_name(db_name)

        conn = psycopg2.connect(database=self.db_default, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute("CREATE DATABASE %s;" % db_name)
            logging.info("Created a database %s" % db_name)
        except:
            logging.error("Has error to create a database %s" % db_name)
        finally:
            conn.close()

    def drop_db(self, db_name):
        if self.check_db(db_name):
            conn = psycopg2.connect(database=self.db_default, user=self.user, password=self.password)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("DROP DATABASE %s;" % db_name)
            conn.close()

    def check_table(self, table_name, db_name=''):
        db_name = self.get_db_name(db_name)

        conn = psycopg2.connect(database=db_name, user=self.user, password=self.password)
        cursor = conn.cursor()

        check_table_command = "select exists(select * from information_schema.tables where table_name=%s)"

        cursor.execute(check_table_command, (table_name,))

        if cursor.fetchone()[0]:
            conn.close()
            return True
        else:
            conn.close()
            return False

    def create_table(self, table_name, headers, db_name=''):
        db_name = self.get_db_name(db_name)

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

        create_table_command = "CREATE TABLE " + table_name + headers_concatenation

        conn = psycopg2.connect(database=db_name, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(create_table_command)
        conn.close()

    def drop_table(self, table_name, db_name=''):
        db_name = self.get_db_name(db_name)

        if self.check_table(table_name, db_name):
            conn = psycopg2.connect(database=db_name, user=self.user, password=self.password)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("DROP TABLE %s;" % table_name)
            conn.close()

    def insert_item(self, table_name, db_name=''):
        db_name = self.get_db_name(db_name)

        if self.check_table(table_name, db_name):
            conn = psycopg2.connect(database=db_name, user=self.user, password=self.password)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("DROP TABLE %s;" % table_name)
            conn.close()

if __name__ == "__main__":
    vsys_db_name = 'vsys_nodes_db'
    vsys_table_name = 'node_details'
    vsys_db = GraphDB('localhost', 'DBNAME', 'aaronyu', 'PASSWORD')
    vsys_db.start_postgresql_wait(1)

    if not vsys_db.check_db(vsys_db_name):
        vsys_db.create_db(vsys_db_name)

    if not vsys_db.check_table(vsys_table_name, vsys_db_name):
        vsys_db.create_table(vsys_table_name, [['vendor_id', 'SERIAL', 'PRIMARY KEY'], ['vendor_name', 'VARCHAR(255)', 'NOT NULL']], vsys_db_name)

    vsys_db.drop_table('vendors', vsys_db_name)
    vsys_db.stop_postgresql_wait(1)
