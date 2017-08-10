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

postgres_sql = '''INSERT INTO t_pub_name (id, pub_header, pub_date, pub_url, pub_category_name_id, pub_importance_id, pub_na_pravah_reclami, pub_size, pub_znaki, pub_tiragh, pub_izd_id, pub_izd_number, pub_release_date, pub_page_number, pub_napoln_raiting, pub_znaki_header_fulltext, pub_user_id, pub_anons, pub_photo, pub_origin_izd_id, is_active, dt_create, dt_change) VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

mysql_rows_sql = "SELECT id, pub_name_id, pub_textarray_type_id, pub_text_length, pub_text, is_active, dt_create, dt_change from t_pub_textarray"
mysql_ids_sql = "SELECT id from t_pub_textarray"

def mysql_connect():
    myconn = MySQLdb.connect(fromdb_host, fromdb_username, fromdb_password, fromdb_name)
    myconn.set_character_set('utf8')
    mycur = myconn.cursor()
    return myconn, mycur

def mysql_get_ids():
    mysql_ids = []
    mycur.execute(mysql_ids_sql)
    data = mycur.fetchall()
    for row in data:
        mysql_id = row[0]
        mysql_ids.append(int(mysql_id))
    return mysql_ids

def mysql_get_rows():
    mycur.execute(mysql_rows_sql)
    data = mycur.fetchall()
    mysql_rows = []
    for row in data:
################## Transformation query array elementss into mysql fields
        mysql_id = row[0]
        pub_name_id = row[1]
        pub_textarray_type_id = row[2]
        pub_text_length = row[3]
        pub_text = row[4]
        is_active = row[5]
        dt_create = row[6]
        dt_change = row[7]
################## Transformation mysql fields into postgres fields
        id = mysql_id
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
    return mysql_rows

def postgres_connect():
    psconn = psycopg2.connect(database = todb_name, user = todb_username, password = todb_password, host = todb_host)
    pscur = psconn.cursor()
    return pscur, psconn


def postgres_put(row):
    mysql_id = (row[0], )
    pscur.execute(postgres_sql, row)
    psconn.commit()
    return mysql_id


def sqlite_put(mysql_id):
    print mysql_id
    sql = 'INSERT INTO mysql_rows(mysql_id) VALUES (?)'
    conn = sqlite3.connect(sqlite_dbname)
    c = conn.cursor()
#    c.execute('CREATE TABLE mysql_rows (id integer primary key autoincrement, mysql_id integer unique, timestamp timestamp default current_timestamp, imported_status integer default 0)')
    c.execute(sql, mysql_id)
    conn.commit()
    conn.close()
    return mysql_id

def sqlite_get_used_ids():
    sql = 'SELECT mysql_id FROM mysql_rows'
    conn = sqlite3.connect(sqlite_dbname)
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    select = c.fetchall()
    conn.close()
    used_ids = []
    for used_id in select:
        used_ids.append(used_id[0])
    return used_ids


##################################

used_ids = sqlite_get_used_ids()

myconn, mycur = mysql_connect()
mysql_rows = mysql_get_rows()
mysql_ids = mysql_get_ids()
myconn.close()

pscur, psconn = postgres_connect()
for row in mysql_rows:
    mysql_id = postgres_put(row)
    sqlite_put(mysql_id)
psconn.close()
