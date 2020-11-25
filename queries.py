import sqlite3

# Create new DB and connect to it
conn = sqlite3.connect("Transcation_DB.db")
cur = conn.cursor()

query_1 = ''' SELECT count(transaction_id) FROM transactions
                LEFT JOIN merchant_unit USING (merchant_unit_key)
                LEFT JOIN transaction_time USING (transaction_time_key)
                WHERE merchant_id = '11660f45-3265-4ff1-891e-cb6cac09dae2'
                AND transaction_date = '2019-07-24' '''

cur.execute(query_1)
print("Transactions at ATG on 24-07-2019: " + str(cur.fetchone()))

query_2 = ''' SELECT category_name FROM transactions
                LEFT JOIN transaction_time USING (transaction_time_key)
                LEFT JOIN subcategory USING (subcategory_id)
                LEFT JOIN category USING (category_id)
                WHERE transaction_date = '2019-07-24'
                GROUP BY category_name
                ORDER BY count(transaction_id) DESC
                LIMIT 1'''

cur.execute(query_2)
print("Category with most transactions on 24-07-2019: " + str(cur.fetchone()))

query_3 = ''' SELECT auth_hour FROM transaction_time
                LEFT JOIN transactions USING (transaction_time_key)
                WHERE auth_hour is not null
                GROUP BY auth_hour
                ORDER BY count(transaction_id) DESC
                LIMIT 1'''

cur.execute(query_3)
print("Busiest hour of the day in total: " + str(cur.fetchone()))

query_4 = ''' SELECT transaction_day_of_week FROM transaction_time
                LEFT JOIN transactions USING (transaction_time_key)
                GROUP BY transaction_day_of_week
                ORDER BY count(transaction_id) DESC
                LIMIT 1'''

cur.execute(query_4)
print("Busiest day of the week in total: " + str(cur.fetchone()))


query_5 = ''' SELECT network_merchant_name, transaction_day_of_week FROM transaction_time
                LEFT JOIN transactions USING (transaction_time_key)
                LEFT JOIN merchant_unit USING (merchant_unit_key)
                GROUP BY network_merchant_name, transaction_day_of_week
                ORDER BY count(transaction_id) DESC
                LIMIT 1'''

query_5_1 = ''' SELECT merchant_id, transaction_day_of_week FROM transaction_time
                LEFT JOIN transactions USING (transaction_time_key)
                LEFT JOIN merchant_unit USING (merchant_unit_key)
                WHERE merchant_id != ''
                GROUP BY merchant_id, transaction_day_of_week
                ORDER BY count(transaction_id) DESC
                LIMIT 1'''

cur.execute(query_5)
print("Busiest day of the week for a single merchant: " + str(cur.fetchone()))

conn.close()