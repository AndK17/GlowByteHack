from ftplib import FTP_TLS
import os


def connection(func):
    def _wrapper(*args, **kwargs):
        con = FTP_TLS('de-edu-db.chronosavant.ru', 'etl_tech_user', 'etl_tech_user_password')
        con.prot_p()
        func(con)
        con.close()
    return _wrapper


@connection
def dowload_all_waybills(con):
    con.cwd('/waybills')
    
    files = con.nlst()
    dfiles = os.listdir('waybills/')
    to_download = set(files)-set(dfiles)
    
    
    error_files = []
    for file in to_download:
        try:
            print(file)
            with open('waybills/'+file, 'wb') as f:
                con.retrbinary('RETR ' + file, f.write, 1024)
        except:
            error_files.append[file]
    
    print(error_files)


@connection
def dowload_new_waybills(con):
    con.cwd('/waybills')
    
    with open('last_waybill.txt', 'r') as f:
        last_wb = f.readline()
    
    new_files = [i for i in con.nlst() if i > last_wb]
    print(new_files)
    
    error_files = []
    for file in new_files:
        try:
            print(file)
            with open('waybills/'+file, 'wb') as f:
                con.retrbinary('RETR ' + file, f.write, 1024)
        except:
            error_files.append[file]
    
    if error_files == []:
        return 'OK'
    else:
        return error_files
    

@connection
def dowload_all_payments(con):
    con.cwd('/payments')

    files = con.nlst()
    dfiles = os.listdir('payments_bp/')
    new_file_count = len(set(files)-set(dfiles))
    
    
    error_files = []
    for i in range(new_file_count//200 + 1):
        to_download = set(files)-set(dfiles)
        for file in to_download:
            try:
                print(file)
                with open('payments_bp/'+file, 'wb') as f:
                    con.retrbinary('RETR ' + file, f.write, 1024)
            except Exception as e:
                print(e)
                os.remove('payments_bp/'+file)
                error_files.append[file]
    
    print(error_files)
            
            
@connection
def dowload_new_payments(con):
    con.cwd('/payments')
    
    with open('last_payment.txt', 'r') as f:
        last_pay = f.readline()
    
    new_files = [i for i in con.nlst() if i > last_pay]
    print(new_files)
    
    error_files = []
    for file in new_files:
        try:
            print(file)
            with open('payments/'+file, 'wb') as f:
                con.retrbinary('RETR ' + file, f.write, 1024)
        except:
            error_files.append[file]
    
    if error_files == []:
        return 'OK'
    else:
        return error_files
    
    
if __name__ == "__main__":
    # dowload_all_waibills()
    dowload_all_payments()
    # dowload_new_waibills()
    # dowload_new_payments()