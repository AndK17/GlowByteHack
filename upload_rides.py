import psycopg2

read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                        password='etl_tech_user_password', host='de-edu-db.chronosavant.ru')
read_cursor = read_conn.cursor()


write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                        password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
write_cursor = write_conn.cursor()

read_cursor.execute("SELECT * FROM main.rides")
rides = read_cursor.fetchall()
read_cursor.execute("""SELECT car_plate_num, event, movement.dt, rides.dt, client_phone, card_num, point_from, point_to
                    FROM main.movement INNER JOIN main.rides ON main.movement.ride = main.rides.ride_id
                    WHERE event = 'END' OR event = 'CANCEL'""")
movement = read_cursor.fetchall()

for i in movement:
    print(i)
# for ride in rides:
#     print(ride)
#     point_from_txt = 
#     point_to_txt = 
#     distance_val = 
#     price_amt = 
#     client_phone_num = 
#     driver_pers_num = 
#     car_plate_num = 
#     ride_arrival_dt = 
#     ride_start_dt = 
#     ride_end_dt = 

#     continue

#     # TODO Доделать проверку на обновление end_dt если есть повторы
#     write_cursor.execute(f"SELECT * FROM dim_cars WHERE plate_num = '{plate_num}';")
#     update_car = write_cursor.fetchall()
#     if len(update_car) > 0:
#         last_line = update_car[-1]
        
#         if (tuple([last_line[0]])+last_line[2:]) == (plate_num, model_name, revision_dt, deleted_flag, end_dt):
#             print('-')
#             continue
#         else:
#             write_cursor.execute(f"UPDATE dim_cars SET end_dt = CURRENT_TIMESTAMP WHERE plate_num = '{plate_num}' AND end_dt IS NULL;")
    
#     write_cursor.execute('INSERT INTO dim_cars VALUES(%s, CURRENT_TIMESTAMP, %s, %s, %s, %s);',
#                 (plate_num, model_name, revision_dt, deleted_flag, end_dt))
#     write_conn.commit()


write_cursor.close()
write_conn.close()
read_cursor.close()
read_conn.close()