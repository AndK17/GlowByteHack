import os
import csv
import datetime

directory = 'payments/'

files = os.listdir(directory)

for file in files:
    print(file)
    with open('payments/'+file, newline='') as f:
        spamreader = csv.reader(f)
        for row in spamreader:
            d = row[0].split('\t')
            print(datetime.datetime.strptime(d[0], '%d.%m.%Y %H:%M:%S'), int(d[1]), float(d[2]))



# print('payment_2022-10-12_17-00.csv' > 'payment_2022-10-12_16-00.csv')