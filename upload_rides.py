import psycopg2

read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                        password='etl_tech_user_password', host='de-edu-db.chronosavant.ru')
read_cursor = read_conn.cursor()


write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                        password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
write_cursor = write_conn.cursor()


read_cursor.execute("""SELECT car_plate_num, event, movement.dt, rides.dt, client_phone, point_from, point_to, distance, price
                    FROM main.movement INNER JOIN main.rides ON main.movement.ride = main.rides.ride_id
                    WHERE event = 'END' OR event = 'CANCEL'""")
rides = read_cursor.fetchall()

ride_id = 0
for ride in rides[:2]:
    print(ride)
    point_from_txt = ride[5]
    point_to_txt = ride[6]
    distance_val = ride[7]
    price_amt = ride[8]
    client_phone_num = ride[4]
    driver_pers_num = None
    car_plate_num = ride[0]
    ride_arrival_dt = ride[3]
    ride_start_dt = None
    ride_end_dt = ride[2]
    
    
    print((ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num,\
        driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt))
    
    
    write_cursor.execute('INSERT INTO fact_rides VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                (ride_id, point_from_txt, point_to_txt, distance_val, price_amt, ride_arrival_dt, ride_start_dt, ride_end_dt, client_phone_num,\
                    driver_pers_num, car_plate_num))
    write_conn.commit()
    ride_id += 1


write_cursor.close()
write_conn.close()
read_cursor.close()
read_conn.close()