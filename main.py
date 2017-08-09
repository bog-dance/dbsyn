# -*- coding: utf8 -*-

import MySQLdb
import psycopg2
import sqlite3
import time

timestamp = time.strftime("%Y-%m-%d_%H:%M:%S")

fromdb_name = 'fromdb'
fromdb_username = 'root'
fromdb_password = 'mysql'
fromdb_host = '127.0.0.1'

todb_name = 'todb'
todb_username = 'postgres'
todb_password = 'qwerty'
todb_host = '127.0.0.1'

sqlite_dbname = 'example.db'


def mysql_get():
    sql = "SELECT id, pub_name_id, pub_textarray_type_id, pub_text_length, pub_text, is_active, dt_create, dt_change from t_pub_textarray"
    db = MySQLdb.connect(fromdb_host, fromdb_username, fromdb_password, fromdb_name)
    db.set_character_set('utf8')
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    print cursor.rowcount
    mysql_rows = []
    id_rows = []
    for row in data:
################## Transformation query array elementss into mysql fields
        id = row[0]
        pub_name_id = row[1]
        pub_textarray_type_id = row[2]
        pub_text_length = row[3]
        pub_text = row[4]
        is_active = row[5]
        dt_create = row[6]
        dt_change = row[7]
################## Transformation mysql fields into postgres fields
        id = id
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
        mysql_rows.append(tuple((id, pub_header, pub_date, pub_url, pub_category_name_id, pub_importance_id, pub_na_pravah_reclami, pub_size, pub_znaki, pub_tiragh, pub_izd_id, pub_izd_number, pub_release_date, pub_page_number, pub_napoln_raiting, pub_znaki_header_fulltext, pub_user_id, pub_anons, pub_photo, pub_origin_izd_id, is_active, dt_create, dt_change)))
        id_rows.append(tuple(( id, timestamp)))
    db.close()
    return mysql_rows, id_rows


def postgres_put(sql_rows):
    sql = '''INSERT INTO t_pub_name (id, pub_header, pub_date, pub_url, pub_category_name_id, pub_importance_id, pub_na_pravah_reclami, pub_size, pub_znaki, pub_tiragh, pub_izd_id, pub_izd_number, pub_release_date, pub_page_number, pub_napoln_raiting, pub_znaki_header_fulltext, pub_user_id, pub_anons, pub_photo, pub_origin_izd_id, is_active, dt_create, dt_change) VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    conn = psycopg2.connect(database = todb_name, user = todb_username, password = todb_password, host = todb_host)
    cur = conn.cursor()
    cur.executemany(sql, sql_rows)
    conn.commit()
    conn.close()
    return None

def sqlite_put():
    conn = sqlite3.connect(sqlite_dbname)
    c = conn.cursor()
    #c.execute('CREATE TABLE mysql_rows (id integer, select_date date, imported integer default 0)')
    c.executemany('INSERT INTO mysql_rows(id, select_date) VALUES (?,?)', id_rows)
    conn.commit()
    conn.close()
    return None


mysql_rows, id_rows = mysql_get()
postgres_put(mysql_rows)
sqlite_put()
