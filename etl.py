#Load transaction data to datawarehouse
def transaction_etl(source_cursor, target_cursor):

    merchant_unit_columns = ['network_merchant_name', 'merchant_id']
    merchant_unit_extract = ''' SELECT DISTINCT {0} FROM transaction_raw_data '''.format(", ".join(merchant_unit_columns))
    source_cursor.execute(merchant_unit_extract)

    merchant_unit_key = 0

    for row in source_cursor:
        merchant_unit_key += 1
        merchant_unit_record = [merchant_unit_key] + list(row)
        merchant_unit_load = ''' INSERT OR IGNORE INTO merchant_unit ({0}) 
                        VALUES (?, ?, ?)'''.format(", ".join(['merchant_unit_key'] + merchant_unit_columns))
        target_cursor.execute(merchant_unit_load, merchant_unit_record)

    transactions_extract_columns = ['id', 'amount', 'currency', 'merchant_unit_key', 'subcategory_id']

    transactions_time_extract = ''' SELECT {0},
                                        transaction_date,
                                        strftime('%d', transaction_date) as "transaction_day", 
                                        strftime('%w', transaction_date) as "transaction_day_of_week",
                                        strftime('%m', transaction_date) as "transaction_month", 
                                        strftime('%Y', transaction_date) as "transaction_year", 
                                        strftime('%H', authorization_time) as "auth_hour", 
                                        strftime('%M', authorization_time) as "auth_minute",
                                        strftime('%S', authorization_time) as "auth_second"
                                    FROM transaction_raw_data
                                    LEFT JOIN merchant_unit USING (network_merchant_name)'''.format(", ".join(transactions_extract_columns))

    source_cursor.execute(transactions_time_extract)

    transaction_time_columns = ['transaction_date', 'transaction_day', 'transaction_day_of_week', 'transaction_month', 'transaction_year', 'auth_hour', 'auth_minute', 'auth_second']
    transactions_columns = ['transaction_id', 'transaction_time_key'] + transactions_extract_columns[1:]
    transaction_time_key = 0

    for row in source_cursor:
        transaction_time_key += 1
        transaction_time_record = [transaction_time_key] + list(row[len(transactions_extract_columns):])
        transaction_time_load = ''' INSERT OR IGNORE INTO transaction_time({0}) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(", ".join(['transaction_time_key']+transaction_time_columns))
        target_cursor.execute(transaction_time_load, transaction_time_record)

        transactions_record = list(row[:(len(transactions_extract_columns))])
        transactions_record.insert(1, transaction_time_key)
        transactions_load = ''' INSERT OR IGNORE INTO transactions({0}) 
                            VALUES (?, ?, ?, ?, ?, ?) '''.format(", ".join(transactions_columns))
        target_cursor.execute(transactions_load, transactions_record)

# Load data from DB to datawarehouse
def etl(source_cursor, target_cursor, extract_query, load_query):
    source_cursor.execute(extract_query)
    data = source_cursor.fetchall()
    target_cursor.executemany(load_query, data)