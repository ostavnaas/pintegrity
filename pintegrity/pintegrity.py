from __future__ import print_function

import hashlib
import os
import sqlite3
import platform
import yaml
import logging.handlers

from sys import stdout
from datetime import datetime
from pprint import pprint


class File_handle:
    def __init__(self, config, db):
        self.db = db
        self.current_files = []
        for path in config.file_path:
            self.build_file_list(path)
        self.check_for_missing_files()

    def __repr__(self):
        return "File_handle(Files: {})".format(len(self.file_path))

    def check_for_missing_files(self):
        for row in self.db.query_all_files():
            file_in_db = os.path.join(row[1], row[2])
            if file_in_db not in self.current_files:
                elogger.critical("Missing file: {}".format(file_in_db))
                self.db.set_as_missing_file(row[0])
        self.db.commit()

    def build_file_list(self, root_dir):
        if '~' in root_dir:
            root_dir = os.path.expanduser(root_dir)

        print('Checking files:')
        for root, dirs, files in os.walk(os.path.abspath(root_dir)):
            for file in files:
                print('.', end='')
                stdout.flush()
                self.current_files.append(os.path.join(root, file))

                if platform.system() == 'Windows':
                    stat = os.path.getctime(os.path.join(root, file))
                else:
                    stat = os.stat(os.path.join(root, file))
                    m_time = datetime.fromtimestamp(
                                stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

                file_in_db = self.db.query_file(root, file)

                if file_in_db is None:
                    file_hash = self.hash_file(root, file)
                    logger.debug('Adding file: %s', os.path.join(root, file))
                    self.db.insert_files('files', **{'file_path': root,
                                                     'file_name': file,
                                                     'last_modify': m_time,
                                                     'file_hash': file_hash,
                                                     'file_removed': '0',
                                                     'file_corrupted': '0'})
                else:
                    file_hash = self.hash_file(root, file)
                    if file_in_db[1] != file_hash:
                        corrupt_file = os.path.join(file_in_db[1],
                                                    file_in_db[2])
                        print(os.path.join(file_in_db[1], file_in_db[2]))
                        elogger.critical("""File corrupted: {}
                                         """.format(corrupt_file))

        self.db.commit()
        print('')

    def hash_file(self, root, file):
        full_path = os.path.join(root, file)
        m = hashlib.sha512()
        for x in self.read_file(full_path):
            m.update(x)
        return m.hexdigest()

    def read_file(self, file):
        with open(file, 'rb') as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                else:
                    yield chunk


class Database:
    def __init__(self, db_file):
        if db_file == ':memory:':
            self.connect = sqlite3.connect(db_file)
        else:
            self.connect = sqlite3.connect(os.path.abspath(db_file))
        self.create_table()

    def create_table(self):
        c = self.connect.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS files (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_path TEXT,
                        file_name TEXT,
                        last_modify TEXT,
                        file_hash TEXT,
                        file_removed TEXT,
                        file_corrupted TEXT
                     );''')
        self.connect.commit()

    def commit(self):
        self.connect.commit()

    def build_insert_query(self, table, **kwargs):
        row_names = ", ".join([x for x in sorted(kwargs)])
        question_mark = ", ".join(["?" for x in sorted(kwargs)])
        values = tuple([kwargs[x] for x in sorted(kwargs)])
        query = 'INSERT INTO {}({}) VALUES ({})'.format(table, row_names,
                                                        question_mark)
        return query, values

    def query_file(self, file_path, file_name):
        c = self.connect.cursor()
        c.execute('''SELECT id, file_path, file_name, file_hash
                     FROM files
                     WHERE file_path = "{}" AND file_name = "{}"
                  '''.format(file_path, file_name))
        return c.fetchone()

    def query_all_files(self):
        c = self.connect.cursor()
        c.execute('''SELECT id, file_path, file_name
                     FROM files
                     WHERE file_removed = "0" AND file_corrupted = "0"
                  ''')
        while True:
            result = c.fetchone()
            if not result:
                break
            else:
                yield result

    def insert_files(self, table, *args, **kwargs):
        c = self.connect.cursor()
        query, values = self.build_insert_query(table, **kwargs)
        c.execute(query, values)

    def set_as_missing_file(self, row_id):
        c = self.connect.cursor()
        query = '''UPDATE files
                   SET file_removed = 1
                   WHERE id = ?;'''
        c.execute(query, (row_id,))


class Config:
    def __init__(self, config_file):
        self.__dict__.update(self.load_config(config_file))

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return yaml.load(f)


def connect_db(db_file):
    if db_file == ':memory:':
        return sqlite3.connect(db_file)

    return sqlite3.connect(os.path.abspath(db_file))


def start_logging(cnf):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_dir, 'config.yaml')
    cnf = Config(config_file)

    logger = logging.getLogger(__name__)

    handler = logging.FileHandler('file_handle.log')
    fm = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    handler.setFormatter(fm)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    elogger = logging.getLogger('Exception')
    ehandler = logging.handlers.SMTPHandler(cnf.email['smtp_server'],
                                            cnf.email['from_addr'],
                                            cnf.email['to_addr'],
                                            'pintegrity')
    ehandler.setFormatter(fm)
    elogger.addHandler(ehandler)
    elogger.setLevel(logging.INFO)
    return elogger, logger


if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_dir, 'config.yaml')
    cnf = Config(config_file)
    elogger, logger = start_logging(cnf)

    db = Database(cnf.db_file)
    fh = File_handle(cnf, db)
