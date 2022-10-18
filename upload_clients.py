import psycopg2

read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                        password='etl_tech_user_password', host='de-edu-db.chronosavant.ru')
read_cursor = read_conn.cursor()


write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                        password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
write_cursor = write_conn.cursor()

read_cursor.execute("SELECT * FROM main.rides")
clients = read_cursor.fetchall()

for client in clients:
    phone_num = client[2]
    card_num = client[3]
    deleted_flag = 'N'
    end_dt = None
    
    # Проверка на поворение строки и обновление end_dt если есть повторы
    write_cursor.execute(f"SELECT * FROM dim_clients WHERE phone_num = '{phone_num}';")
    update_client = write_cursor.fetchall()
    if len(update_client) > 0:
        last_line = update_client[-1]
        if (last_line[0], last_line[2], last_line[3], last_line[4]) == (phone_num, card_num, deleted_flag, end_dt):
            continue
        else:
            write_cursor.execute(f"UPDATE dim_clients SET end_dt = CURRENT_TIMESTAMP WHERE phone_num = '{phone_num}' AND end_dt IS NULL;")
    
    print((phone_num, card_num, deleted_flag, end_dt))
    write_cursor.execute('INSERT INTO dim_clients VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s);',
                (phone_num, card_num, deleted_flag, end_dt))
    write_conn.commit()


write_cursor.close()
write_conn.close()
read_cursor.close()
read_conn.close()