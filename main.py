import sqlite3

from load import load_csv_sqlite
from etl import transaction_etl, etl
from datawarehouse import create_dw


def main():
   # Create new DB and connect to it
   conn = sqlite3.connect("Transcation_DB.db")
   select_cursor = conn.cursor();
   insert_cursor = conn.cursor();

   # Load transactions from csv file to SQLite DB
   transaction_table_name = 'transaction_raw_data'
   transaction_column_types = ['text', 'date', 'text', 'int', 'text', 'text', 'text', 'text']
   transaction_path = 'anon_transactions_sample.csv'
   load_csv_sqlite(transaction_path, transaction_table_name, transaction_column_types, insert_cursor)

   # Load category csv file to SQLite DB
   category_table_name = 'category_raw_data'
   category_column_types = ['text', 'text', 'text', 'text', 'text', 'text']
   category_path = 'categories.csv'
   load_csv_sqlite(category_path, category_table_name, category_column_types, insert_cursor)

   #Create datawarehouse table structure
   create_dw(insert_cursor)

   #Load transaction data to datawarehouse
   transaction_etl(select_cursor, insert_cursor)

   # Load category data to datawarehouse
   sector_extract = ''' SELECT DISTINCT sector_id, sector_name FROM category_raw_data '''
   sector_load = ''' INSERT OR IGNORE INTO sector (sector_id, sector_name) VALUES (?, ?) '''
   etl(select_cursor, insert_cursor, sector_extract, sector_load)

   category_extract = ''' SELECT DISTINCT category_id, category_name, sector_id FROM category_raw_data'''
   category_load = ''' INSERT OR IGNORE INTO category (category_id, category_name, sector_id) VALUES (?, ?, ?) '''
   etl(select_cursor, insert_cursor, category_extract, category_load)

   subcategory_extract = ''' SELECT DISTINCT subcategory_id, subcategory_name, category_id FROM category_raw_data '''
   subcategory_load = ''' INSERT OR IGNORE INTO subcategory (subcategory_id, subcategory_name, category_id) VALUES (?, ?, ?) '''
   etl(select_cursor, insert_cursor, subcategory_extract, subcategory_load)

   conn.commit()
   conn.close()


if __name__ == "__main__":
   main()