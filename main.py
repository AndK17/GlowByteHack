from upload_drivers import update_dim_drivers
from upload_payments import update_fact_payments


# 1. payments
print('fact_payments start update')
if update_fact_payments() == []:
    print('fact_payments succesfully updated')
    
# 2. clients

# 3. drivers
print('Dim_drivers start update')
if update_dim_drivers() == 'OK':
    print('Dim_drivers succesfully updated')

# 4. cars
# 5. waybills
# 6. rides
