import csv

# Load data from csv file to specified table
def load_csv_sqlite (path, table_name, column_types, cursor):
    with open(path,'r') as data:
        reader = csv.DictReader(data)
        column_names = reader.fieldnames
        table_schema = ','.join([' "%s" %s' % (_name, _type) for (_name, _type) in zip(column_names, column_types)])
        sql_create = ''' CREATE TABLE IF NOT EXISTS "{0}" ({1}) '''.format(table_name, table_schema)
        cursor.execute(sql_create)

        for row in reader:
            row_values = [row[col] for col in column_names]
            sql_insert = ''' INSERT INTO "{0}" ({1}) VALUES ({2})'''.format(table_name, ", ".join(column_names), ", ".join(['?'] * len(row_values)))
            cursor.execute(sql_insert, row_values)