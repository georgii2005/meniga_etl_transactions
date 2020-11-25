

#Create datawarehouse table structure
def create_dw(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS transactions (transaction_id text primary key, 
                                                        transaction_time_key int, 
                                                        amount real, 
                                                        currency text, 
                                                        merchant_unit_key int, 
                                                        subcategory_id text) ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS transaction_time (transaction_time_key int primary key,
                                                            transaction_date date,
                                                            transaction_day int,
                                                            transaction_day_of_week int, 
                                                            transaction_month int, 
                                                            transaction_year int,
                                                            auth_hour int, 
                                                            auth_minute int, 
                                                            auth_second int ) ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS merchant_unit (merchant_unit_key int primary key, 
                                                        network_merchant_name text UNIQUE,
                                                        merchant_id text) ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS subcategory (subcategory_id text primary key, 
                                                        subcategory_name text, 
                                                        category_id text) ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS category (category_id text primary key, 
                                                    category_name text, 
                                                    sector_id text) ''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS sector (sector_id text primary key, sector_name text) ''')

