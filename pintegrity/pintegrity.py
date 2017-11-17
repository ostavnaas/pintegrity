from __future__ import print_function

import hashlib
import os
import sqlite3
import platform
import yaml
import logging.handlers

from sys import stdout
from datetime import datetime


class File_handle:
    def __init__(self, config, db):
        self.db = db
        self.full_path = []
        for path in config.file_path:
            self.build_file_list(path)
        self.check_for_missing_files()

    def __repr__(self):
        return "File_handle(Files: {})".format(len(self.file_path))

    def check_for_missing_files(self):
        all_files = self.db.query_all_files()
        for file in all_files:
            full_path = os.path.join(file[0], file[1])
            if full_path not in self.full_path:
                elogger.critical("Missing file: {}".format(full_path))

    def build_file_list(self, root_dir):
        if '~' in root_dir:
            root_dir = os.path.expanduser(root_dir)
        print('Checking files:')
        for root, dirs, files in os.walk(os.path.abspath(root_dir)):
            for file in files:
                print('.', end='')
                stdout.flush()
                self.full_path.append(os.path.join(root, file))

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
                                                     'file_hash': file_hash})
                else:
                    file_hash = self.hash_file(root, file)
                    if file_in_db[1] != file_hash:
                        print(os.path.join(file_in_db[2], file_in_db[3]))

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
                file_hash TEXT
                );''')
        self.connect.commit()

    def commit(self):
        self.connect.commit()

    def build_insert_query(self, table, **kwargs):
        row_names = ", ".join([x for x in kwargs])
        question_mark = ", ".join(["?" for x in kwargs])
        values = tuple([kwargs[x] for x in kwargs])
        query = 'INSERT INTO {}({}) VALUES ({})'.format(table, row_names,
                                                        question_mark)
        return query, values

    def query_file(self, file_path, file_name):
        c = self.connect.cursor()
        c.execute('''SELECT id, file_hash, file_path, file_name
                     FROM files
                     WHERE file_path = "{}" AND file_name = "{}"
                  '''.format(file_path, file_name))
        return c.fetchone()

    def query_all_files(self):
        c = self.connect.cursor()
        c.execute('''SELECT file_path, file_name
                     FROM files
                  ''')
        return c.fetchall()

    def insert_files(self, table, *args, **kwargs):
        c = self.connect.cursor()
        query, values = self.build_insert_query(table, **kwargs)
        c.execute(query, values)


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
