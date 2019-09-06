import os
import sqlite3
from sqlite3 import Error
"""
    @author Renata Ann Zeitler
    @author Joshua Good
    February 24th, 2018

    This database will be used for storing filepaths to prevent
    duplicates from entering Logstash. This is to be run for creating the
    db and necessary table. Additionally creates the initial folder structure
    if not already created.
"""

categories = ["audit", "base_os_commands", "bycast", "cassandra_commands", "cassandra_gc",
              "cassandra_system", "dmesg", "gdu_server", "init_sg", "install", "kern", 
              "messages", "pge_image_updater", "pge_mgmt_api", "server_manager", "sg_fw_update",
              "storagegrid_daemon", "storagegrid_node", "syslog", "system_commands", "upgrade", "other"]

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
        exit(1)

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        c.close()
        conn.close()
    except Error as e:
        print(e)

def setupCategories(root):
    for category in categories:
        os.makedirs(root + "/" + category)

"""
Creates a db in the specified location, for now the cwd
"""
if __name__ == '__main__':
    path = os.path.realpath(__file__)
    root = path.replace("setup.py", "logjam_categories")
    path = path.replace("setup.py", "duplicates.db")
    conn = create_connection(path)
    sql_table = """ CREATE TABLE IF NOT EXISTS paths (
                        path text, 
                        flag integer,
                        category text,
                        timestamp float
                    ); """
    create_table(conn, sql_table)
"""
Setup folder structure if root folder is not present
"""
if not os.path.exists(root):
    os.makedirs(root)
    setupCategories(root)