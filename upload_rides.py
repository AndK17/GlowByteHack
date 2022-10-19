from shutil import ExecError
import psycopg2

read_conn = psycopg2.connect(dbname='taxi', user='etl_tech_user', 
                        password='etl_tech_user_password', host='de-edu-db.chronosavant.ru')
read_cursor = read_conn.cursor()


write_conn = psycopg2.connect(dbname='dwh', user='dwh_krasnoyarsk', 
                        password='dwh_krasnoyarsk_uBPaXNSx', host='de-edu-db.chronosavant.ru')
write_cursor = write_conn.cursor()


read_cursor.execute("""SELECT point_from, point_to, distance, price, client_phone, ry.car_plate_num as car_plate_num,\
                            ry.dt as arrival_dt, NULL as start_dt, cl.dt as end_dt
                    FROM (SELECT * FROM main.movement WHERE event = 'READY') AS ry
                        INNER JOIN (SELECT * FROM main.movement WHERE event = 'CANCEL') AS cl ON ry.ride = cl.ride
                        INNER JOIN main.rides as rides ON ry.ride = rides.ride_id;""")
rides = read_cursor.fetchall()

    
read_cursor.execute("""SELECT point_from, point_to, distance, price, client_phone, ry.car_plate_num as car_plate_num,\
                            ry.dt as arrival_dt, bg.dt as start_dt, ed.dt as end_dt
                    FROM (SELECT * FROM main.movement WHERE event = 'READY') AS ry
                        INNER JOIN (SELECT * FROM main.movement WHERE event = 'BEGIN') AS bg ON ry.ride = bg.ride
                        INNER JOIN (SELECT * FROM main.movement WHERE event = 'END') as ed ON bg.ride = ed.ride
                        INNER JOIN main.rides as rides ON ry.ride = rides.ride_id;""")
rides += read_cursor.fetchall()



write_cursor.execute("SELECT MAX(ride_id) FROM fact_rides")
res = write_cursor.fetchall()
if res != [(None,)]:
    ride_id = res[-1][0] + 1
else:
    ride_id = 0
    
for ride in rides:
    print(ride)
    point_from_txt = ride[0]
    point_to_txt = ride[1]
    distance_val = ride[2]
    price_amt = ride[3]
    client_phone_num = ride[4]
    car_plate_num = ride[5]
    ride_arrival_dt = ride[6]
    ride_start_dt = ride[7]
    ride_end_dt = ride[8]
    
    try:
        write_cursor.execute(f"SELECT driver_pers_num FROM fact_waybills WHERE car_plate_num = '{car_plate_num}' AND work_start_dt <= '{ride_arrival_dt}' AND (work_end_dt >= '{ride_end_dt}' OR work_end_dt IS NULL);")
        driver_pers_num = int(write_cursor.fetchall()[-1][0])
    except Exception as e:
        print(e)
        write_cursor.execute(f"SELECT * FROM fact_waybills WHERE car_plate_num = '{car_plate_num}' AND work_start_dt <= '{ride_arrival_dt}' AND (work_end_dt >= '{ride_end_dt}' OR work_end_dt IS NULL);")
        print(write_cursor.fetchall())
        break
        
    print((ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num,\
        driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt))
    print()
    
    write_cursor.execute('INSERT INTO fact_rides VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                (ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num,\
                    driver_pers_num, car_plate_num, ride_arrival_dt, ride_start_dt, ride_end_dt))
    write_conn.commit()
    ride_id += 1


write_cursor.close()
write_conn.close()
read_cursor.close()
read_conn.close()