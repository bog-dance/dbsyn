import MySQLdb
import psycopg2

fromdb_name = 'fromdb'
fromdb_username = 'root'
fromdb_password = 'mysql'
fromdb_host = '127.0.0.1'

todb_name = 'todb'
todb_username = 'postgres'
todb_password = 'qwerty'
todb_host = '127.0.0.1'

def mysql_get():
    sql = "SELECT * from t_pub_textarray"
    
    db = MySQLdb.connect(fromdb_host, fromdb_username, fromdb_password, fromdb_name)
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    for row in data:
        id = row[0]
        pub_name_id = row[1]
        pub_textarray_type_id = row[2]
        pub_text_length = row[3]
        pub_text = row[4]
        is_active = row[5]
        dt_create = row[6]
        dt_change = row[7]
        print "%s %s %s %s %s %s %s"  % (id, pub_name_id, pub_textarray_type_id, pub_text_length, is_active, dt_create, dt_change)
    db.close()

def postgres_put():
    conn = psycopg2.connect(database = todb_name, user = todb_username, password = todb_password, host = todb_host)
    print "Opened database successfully"
    
    cur = conn.cursor()
    cur.execute('''INSERT INTO t_pub_name (id, pub_header, pub_date, pub_url, pub_category_name_id, pub_importance_id, pub_na_pravah_reclami, pub_size, pub_znaki, pub_tiragh, pub_izd_id, pub_izd_number, pub_release_date, pub_page_number, pub_napoln_raiting, pub_znaki_header_fulltext, pub_user_id, pub_anons, pub_photo, pub_origin_izd_id, is_active, dt_create, dt_change) VALUES
    ('4971132', 'rsfcrhr94wfh294nc9uwrefbh293r4hfr79qwghfw',   '2016-10-07 12:30:01',  'http://fraza.ua/news/07.10.16/252760/razvedenie_sil_na_uchastke_bogdanovkapetrovskoe_verificirovano_obse.html',    '6',    '2',    '0',    '0',    '2821', '0',    '2515', '0',    '2016-10-07 12:30:01',  '0',    '0',    '2907', '361',  '0',    '0',    '0',    '1',    '2016-10-12 13:47:42',  NULL)''')
    
    conn.commit()
    conn.close()
