"""Material's importer from mysql to postgres db"""
# -*- coding: utf8 -*-

import MySQLdb
import psycopg2
import sqlite3
import time

FROMDB_NAME = 'fromdb'
FROMDB_USERNAME = 'root'
FROMDB_PASSWORD = 'mysql'
FROMDB_HOST = '127.0.0.1'

TODB_NAME = 'todb'
TODB_USERNAME = 'postgres'
TODB_PASSWORD = 'qwerty'
TODB_HOST = '127.0.0.1'

SQLITE_DBNAME = 'example.db'
SQLITE_TBLNAME = 'mysql_rows'

LOG_FILE = 'report'


def mysql_connect():
    """Connect to mysql db"""
    myconn = MySQLdb.connect(FROMDB_HOST, FROMDB_USERNAME, FROMDB_PASSWORD,
                             FROMDB_NAME)
    myconn.set_character_set('utf8')
    mycur = myconn.cursor()
    return myconn, mycur


def mysql_get_ids(mycur):
    """Get all mysql id's"""
    mysql_all_ids_sql = "SELECT id from t_pub_textarray"
    mysql_all_ids = []
    mycur.execute(mysql_all_ids_sql)
    data = mycur.fetchall()
    for row in data:
        mysql_id = row[0]
        mysql_all_ids.append(int(mysql_id))
    return mysql_all_ids


def mysql_get_rows(mysql_actual_ids, mycur):
    """Get all mysql rows"""
    mysql_sql = """SELECT id, pub_name_id, pub_textarray_type_id,
                    pub_text_length, pub_text, is_active, dt_create, dt_change
                    from t_pub_textarray where id in (%s)"""
    in_p = ', '.join(map(lambda x: '%s', mysql_actual_ids))
    mysql_sql = mysql_sql % in_p
    mycur.execute(mysql_sql, mysql_actual_ids)
    data = mycur.fetchall()
    mysql_rows = []
# Transformation query array elementss into mysql fields
    for row in data:
        mysql_id = row[0]
        pub_name_id = row[1]
        pub_textarray_type_id = row[2]
        pub_text_length = row[3]
        pub_text = row[4]
        is_active = row[5]
        dt_create = row[6]
        dt_change = row[7]
# Transformation mysql fields into postgres fields
        psql_id = mysql_id
        pub_header = pub_text
        pub_date = None
        pub_url = None
        pub_category_name_id = pub_name_id
        pub_importance_id = None
        pub_na_pravah_reclami = None
        pub_size = pub_text_length
        pub_znaki = None
        pub_tiragh = None
        pub_izd_id = None
        pub_izd_number = None
        pub_release_date = None
        pub_page_number = None
        pub_napoln_raiting = None
        pub_znaki_header_fulltext = None
        pub_user_id = None
        pub_anons = None
        pub_photo = None
        pub_origin_izd_id = pub_textarray_type_id
        is_active = is_active
        dt_create = dt_create
        dt_change = dt_change
        mysql_rows.append(tuple((psql_id, pub_header, pub_date, pub_url,
                                 pub_category_name_id, pub_importance_id,
                                 pub_na_pravah_reclami, pub_size, pub_znaki,
                                 pub_tiragh, pub_izd_id, pub_izd_number,
                                 pub_release_date, pub_page_number,
                                 pub_napoln_raiting, pub_znaki_header_fulltext,
                                 pub_user_id, pub_anons, pub_photo,
                                 pub_origin_izd_id, is_active, dt_create,
                                 dt_change)))
    return mysql_rows


def postgres_connect():
    """Connect to postgres db"""
    psconn = psycopg2.connect(database=TODB_NAME, user=TODB_USERNAME,
                              password=TODB_PASSWORD, host=TODB_HOST)
    pscur = psconn.cursor()
    return pscur, psconn


def postgres_put(pscur, psconn, row, report):
    """Insert into postgres db rows"""
    postgres_sql = """INSERT INTO t_pub_name (id, pub_header, pub_date, pub_url,
                      pub_category_name_id, pub_importance_id,
                      pub_na_pravah_reclami, pub_size, pub_znaki, pub_tiragh,
                      pub_izd_id, pub_izd_number, pub_release_date,
                      pub_page_number, pub_napoln_raiting,
                      pub_znaki_header_fulltext, pub_user_id, pub_anons,
                      pub_photo, pub_origin_izd_id, is_active, dt_create,
                      dt_change) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s)"""
    mysql_id = (row[0], )
    try:
        pscur.execute(postgres_sql, row)
        psconn.commit()
    except (Exception) as error:
        print error
        report.write(error)
        report.close()
    return mysql_id


def sqlite_put(mysql_id, report):
    """Insert into ebedded sqlite db used ids"""
    sqlite_sql = 'INSERT INTO mysql_rows(mysql_id) VALUES (?)'
    try:
        conn = sqlite3.connect(SQLITE_DBNAME)
        sqlitecur = conn.cursor()
        sqlitecur.execute(sqlite_sql, mysql_id)
        conn.commit()
        conn.close()
    except (Exception) as error:
       report.write(error)
       report.close()
       return mysql_id


def sqlite_get_used_ids():
    """Get used rows ids from sqlite db"""
    sqlite_sql = 'SELECT mysql_id FROM mysql_rows'
    used_ids = []
    try:
        conn = sqlite3.connect(SQLITE_DBNAME)
        sqlitecur = conn.cursor()
        sqlitecur.execute("""SELECT name FROM sqlite_master WHERE type='table'
                  AND name='mysql_rows'; """)
        sqlitecur.execute(sqlite_sql)
        conn.commit()
        select = sqlitecur.fetchall()
        conn.close()
        for used_id in select:
            used_ids.append(used_id[0])
    except (Exception) as error:
        if 'no such table: mysql_rows' in error:
            conn = sqlite3.connect(SQLITE_DBNAME)
            sqlitecur = conn.cursor()
            sqlitecur.execute("""CREATE TABLE mysql_rows (id integer primary key
                         autoincrement, mysql_id integer unique, timestamp
                         timestamp default current_timestamp)""")
            conn.close()
    return used_ids


def main():
    """Main function"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    report = open(LOG_FILE, 'a')

    mysql_used_ids = sqlite_get_used_ids()
    myconn, mycur = mysql_connect()
    mysql_all_ids = mysql_get_ids(mycur)
    mysql_actual_ids = [x for x in mysql_all_ids if x not in mysql_used_ids]

    print 'mysql_used_ids: %s' % (mysql_used_ids)
    print 'mysql_all_ids: %s' % (mysql_all_ids)
    print 'mysql_actual_ids: %s' % (mysql_actual_ids)
    report.write("%s\nmysql_used_ids: %s\nmysql_all_ids: %s\n"
                 "mysql_actual_ids: %s\n\n" % (timestamp, mysql_used_ids,
                                               mysql_all_ids,
                                               mysql_actual_ids))
    if not mysql_actual_ids:
        print 'no new records!'
    else:
        mysql_rows = mysql_get_rows(mysql_actual_ids, mycur)
        myconn.close()

        pscur, psconn = postgres_connect()
        for row in mysql_rows:
            mysql_id = postgres_put(pscur, psconn, row, report)
            sqlite_put(mysql_id, report)
        psconn.close()
        report.close()
        return None


if __name__ == "__main__":
    main()
