import csv
import psycopg2
from decimal import Decimal
import sys 
import datetime
import os

def get_column_data_types(headers, csvreader):
    data_types = []
    for i, header in enumerate(headers):
        data_types.append(None)
    for row in csvreader:
        for i, value in enumerate(row):
            if data_types[i] is None:
                try:
                    int(value)
                    if float(value).is_integer():
                        data_types[i] = "INTEGER"
                    else:
                        data_types[i] = "DECIMAL"
                except ValueError:
                    try:
                        float(value.replace("$", "").replace(",", ""))
                        data_types[i] = "DECIMAL"
                    except ValueError:
                        data_types[i] = "TEXT"
    return data_types

def print_column_data_types(headers, data_types):
    print("Column Name | Data Type")
    print("------------+----------")
    for i, header in enumerate(headers):
        print("{:<12} | {}".format(header, data_types[i]))

def get_primary_key_columns(headers):
    pk_columns = []
    num_pk_columns = int(input("Please enter the number of columns in the primary key: "))
    for i in range(num_pk_columns):
        pk_column = input("Please enter the number of primary key column {} (1-{}): ".format(i+1, len(headers)))
        pk_columns.append(headers[int(pk_column) - 1])
    return pk_columns



def create_table(filename, no_archive_file=False):
    current_file_folder_name = os.path.splitext(filename)[0]

    # Set the folder path for the CSV file in the processed folder
    processed_folder = os.path.join('processed', current_file_folder_name)
    if not os.path.exists(processed_folder):
        print(f"No processed folder found for '{filename}'")
        return

    try:
        # Get the most recent timestamped CSV file in the processed folder
        processed_filenames = [f for f in os.listdir(processed_folder) if f.endswith('.csv')]
        if not processed_filenames:
            raise ValueError(f"No processed CSV files found in folder '{processed_folder}'")
        latest_processed_filename = max(processed_filenames, key=lambda f: os.path.getmtime(os.path.join(processed_folder, f)))
        processed_filepath = os.path.join(processed_folder, latest_processed_filename)
        
        conn = psycopg2.connect(database="db1", user="alok", password="reddy", host="localhost", port="5432")
        cur = conn.cursor()

        if no_archive_file and processed_filepath is not None:
            with open(processed_filepath, "r") as csvfile:
                csvreader = csv.reader(csvfile)
                headers = next(csvreader)
                table_name = os.path.splitext(latest_processed_filename)[0]
                table_name = table_name.split('_')[0]
                # Determine the data types of each column
                data_types = get_column_data_types(headers, csvreader)

                # Print the data types
                print_column_data_types(headers, data_types)

                # Determine the primary key columns
                pk_columns = get_primary_key_columns(headers)

                pk_constraint = ", ".join(pk_columns)
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([header.replace('(', '').replace(')', '') + ' ' + data_types[i] for i, header in enumerate(headers)])}, PRIMARY KEY ({pk_constraint}));"
                print(f"Create Table Query: {create_table_query}")
                confirm = input("Do you want to create the table? Press 1 to confirm, any other key to abort: ")
                if confirm == "1":
                    try:
                        cur.execute(create_table_query)
                        print("Table created successfully!")
                        print(cur.statusmessage)
                        insert_data_into_table(cur, conn, processed_filepath, headers, data_types, table_name)
                        return True
                    except Exception as e:
                        print("Error occurred while creating table:", e)
                        print(cur.statusmessage)
                        return False
                else:
                    print("Aborted creation")
                    return False
        else:
            print("Archive file is not empty. Table updation is done")
            with open(processed_filepath, "r") as csvfile:
                csvreader = csv.reader(csvfile)
                headers = next(csvreader)
                table_name = os.path.splitext(latest_processed_filename)[0]
                table_name = table_name.split('_')[0]
                # Determine the data types of each column
                # data_types = get_column_data_types(headers, csvreader)

                # Print the data types
                # print_column_data_types(headers, data_types)

                # Determine the primary key columns
                pk_columns = get_primary_key_columns1(cur, conn, table_name)
                print(pk_columns)
                update_records(cur, conn, table_name, pk_columns, processed_filepath, data_types=None)
                return False
    except Exception as e:
        print(f"Error occurred: {e}")
        return False

def get_primary_key_columns1(cur, conn, table_name):
    """
    Get the primary key column names for a given table.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor object
        conn (psycopg2.extensions.connection): Database connection object
        table_name (str): Name of the table

    Returns:
        pk_columns (list of str): List of primary key column names
    """
    pk_columns = []
    try:
        # Execute the query to get primary key column names
        cur.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '{table_name}' AND constraint_name LIKE '%_pkey'")

        # Fetch the results
        results = cur.fetchall()

        # Extract the column names from the results
        pk_columns = [result[0] for result in results]

    except Exception as e:
        print(f"Error: {e}")

    # Return the list of primary key column names
    return pk_columns


def insert_data_into_table(cur, conn, filename, headers, data_types, table_name):
    with open(filename, "r") as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header row
        rows_to_insert = []
        for row in csvreader:
            values = []
            for i, value in enumerate(row):
                if data_types[i] == "INTEGER":
                    values.append(int(value))
                elif data_types[i] == "DECIMAL":
                    values.append(Decimal(value.replace("$", "").replace(",", "")))
                else:
                    values.append(value.replace("(", "").replace(")", ""))
            rows_to_insert.append(tuple(values))

        insert_query = f"INSERT INTO {table_name} ({', '.join([header.replace('(', '').replace(')', '') for header in headers])}) VALUES ({', '.join(['%s' for _ in range(len(headers))])});"
        print(f"Insert Query: {insert_query}")
        print(f"Values: {rows_to_insert}")
        confirm = input("Do you want to insert all rows? Press 1 to confirm, any other key to skip: ")
        if confirm == "1":
            try:
                cur.executemany(insert_query, rows_to_insert)
                print("CSV data inserted successfully!")
                print(cur.statusmessage)

                # Get current timestamp
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                
                # Create archived folder if it doesn't exist
                if not os.path.exists('archived'):
                    os.makedirs('archived')

                # Create a folder with the name of the CSV file in the archived folder if it doesn't exist
                foldername = os.path.splitext(os.path.basename(filename))[0]
                # print(filename)
                foldername = foldername.split('_')[0]
                if not os.path.exists(f'archived/{foldername}'):
                    os.makedirs(f'archived/{foldername}')

                # Write the SQL table to a CSV file in the archived folder
                with open(f'archived/{foldername}/{table_name}_{timestamp}.csv', 'w') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(headers)
                    cur.execute(f"SELECT * FROM {table_name}")
                    rows = cur.fetchall()
                    for row in rows:
                        csvwriter.writerow(row)

                print(f"CSV file saved in archived/{foldername}/{table_name}_{timestamp}.csv")

                # Print an error message if the CSV file is not archivedd properly
                if not os.path.exists(f'archived/{foldername}/{table_name}_{timestamp}.csv'):
                    print("Error: CSV file not archivedd properly")

            except Exception as e:
                print("Error occurred while inserting csv data table:", e)
                print(cur.statusmessage)
        else:
            print("Aborted Insertion")
            return False

        # Commit the changes and close the cursor and connection
        conn.commit()
        cur.close()
        conn.close()

def update_records(cur, conn, table_name, pk_columns, csv_file_path, data_types=None):
    # Open CSV file
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Initialize counters for rows updated and inserted
        rows_updated = 0
        rows_inserted = 0

        # Loop through each row in the CSV file
        for row in csv_reader:
            # Build SQL query to update record based on primary key columns
            query = "UPDATE {} SET ".format(table_name)
            update_columns = []
            where_conditions = []
            for column_name, value in row.items():
                if column_name.lower() in [pk_column.lower() for pk_column in pk_columns]:
                    where_conditions.append("{}='{}'".format(column_name.lower(), value))
                else:
                    if data_types:
                        if data_types[column_name] == "INTEGER":
                            update_columns.append("{}={}".format(column_name.lower(), int(value)))
                        elif data_types[column_name] == "DECIMAL":
                            update_columns.append("{}={}".format(column_name.lower(), Decimal(value.replace('$', '').replace(',', ''))))
                        else:
                            update_columns.append("{}='{}'".format(column_name.lower(), value))
                    else:
                        update_columns.append("{}='{}'".format(column_name.lower(), value))
            query += ', '.join(update_columns)
            query += " WHERE {}".format(' AND '.join(where_conditions))

            # Execute SQL query to update record
            cur.execute(query)

            # If no rows were updated, then insert a new row instead
            if cur.rowcount == 0:
                query = "INSERT INTO {} ({}) VALUES ({})".format(
                    table_name,
                    ', '.join(row.keys()),
                    ', '.join(["'{}'".format(value.replace("'", "''")) for value in row.values()])
                )
                cur.execute(query)
                rows_inserted += 1
            else:
                rows_updated += 1

        print(f"{rows_updated} rows updated and {rows_inserted} rows inserted.")

    # Commit changes and close connection
    conn.commit()
    conn.close()

    # Get current timestamp
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Create archived folder if it doesn't exist
    if not os.path.exists('archived'):
        os.makedirs('archived')


    # Create a folder with the name of the CSV file in the archived folder if it doesn't exist
    foldername = os.path.splitext(os.path.basename(csv_file_path))[0]
    if not os.path.exists(f'archived/{foldername}'):
        os.makedirs(f'archived/{foldername}')

    # Write the SQL table to a CSV file in the archived folder
    with open(f'archived/{foldername}/{table_name}_{timestamp}.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        for row in rows:
            csvwriter.writerow(row)

    print(f"CSV file saved in archived/{foldername}/{table_name}_{timestamp}.csv from update_records.")

def update_recordsworking(cur, conn, table_name, pk_columns, csv_file_path, data_types=None):
    # Open CSV file
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Loop through each row in the CSV file
        for row in csv_reader:
            # Build SQL query to update record based on primary key columns
            query = f"UPDATE {table_name} SET "
            update_columns = []
            where_conditions = []
            for column_name, value in row.items():
                if column_name.lower() in [pk_column.lower() for pk_column in pk_columns]:
                    where_conditions.append(f"{column_name.lower()}='{value}'")
                else:
                    if data_types:
                        if data_types[column_name] == "INTEGER":
                            update_columns.append(f"{column_name.lower()}={int(value)}")
                        elif data_types[column_name] == "DECIMAL":
                            update_columns.append(f"{column_name.lower()}={Decimal(value.replace('$', '').replace(',', ''))}")
                        else:
                            update_columns.append(f"{column_name.lower()}='{value}'")
                    else:
                        update_columns.append(f"{column_name.lower()}='{value}'")
            query += ', '.join(update_columns)
            query += f" WHERE {' AND '.join(where_conditions)}"

            print(where_conditions)
            print(update_columns)
            print("************")
            print(query)
            # Execute SQL query to update record
            cur.execute(query)
            print(f"Cursor Status from update_records: {cur.statusmessage}")

    # Commit changes and close connection
    conn.commit()
    conn.close()

    # Get current timestamp
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Create archived folder if it doesn't exist
    if not os.path.exists('archived'):
        os.makedirs('archived')

    # Create a folder with the name of the CSV file in the archived folder if it doesn't exist
    foldername = os.path.splitext(os.path.basename(csv_file_path))[0]
    if not os.path.exists(f'archived/{foldername}'):
        os.makedirs(f'archived/{foldername}')

    # Write the SQL table to a CSV file in the archived folder
    with open(f'archived/{foldername}/{table_name}_{timestamp}.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        for row in rows:
            csvwriter.writerow(row)

    print(f"CSV file saved in archived/{foldername}/{table_name}_{timestamp}.csv from update_records.")




def update_records2(cur, conn, table_name, pk_columns, csv_file_path, data_types=None):
    # Open CSV file
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Loop through each row in the CSV file
        for row in csv_reader:
            # Build SQL query to update record based on primary key columns
            query = f"UPDATE {table_name} SET "
            update_columns = []
            where_conditions = []
            for column_name, value in row.items():
                if column_name in pk_columns:
                    where_conditions.append(f"{column_name}='{value}'")
                else:
                    if data_types:
                        if data_types[column_name] == "INTEGER":
                            update_columns.append(f"{column_name}={int(value)}")
                        elif data_types[column_name] == "DECIMAL":
                            update_columns.append(f"{column_name}={Decimal(value.replace('$', '').replace(',', ''))}")
                        else:
                            update_columns.append(f"{column_name}='{value}'")
                    else:
                        update_columns.append(f"{column_name}='{value}'")
            query += ', '.join(update_columns)
            query += f" WHERE {' AND '.join(where_conditions)}"
            
            # query = f"UPDATE {table_name} SET "
            # update_columns = []
            # where_conditions = []
            # print("::::::::")
            # print(pk_columns)
            # for column_name, value in row.items():
            #     if column_name in pk_columns:
            #         where_conditions.append(f"{column_name}='{value}'")
            #     else:
            #         if data_types:
            #             if data_types[column_name] == "INTEGER":
            #                 update_columns.append(f"{column_name}={int(value)}")
            #             elif data_types[column_name] == "DECIMAL":
            #                 update_columns.append(f"{column_name}={Decimal(value.replace('$', '').replace(',', ''))}")
            #             else:
            #                 update_columns.append(f"{column_name}='{value}'")
            #         else:
            #             update_columns.append(f"{column_name}='{value}'")

            # if where_conditions:
            #     query += ', '.join(update_columns)
            #     query += f" WHERE {' AND '.join(where_conditions)}"
            # else:
            #     query += ', '.join(update_columns)
            print("************")
            print(where_conditions)
            print("************")
            print(update_columns)
            print("************")
            print(query)
            print("************")
            # Execute SQL query to update record
            cur.execute(query)
            print(f"Cursor Status from update_records: {cur.statusmessage}")

    # Commit changes and close connection
    conn.commit()
    conn.close()

    # Get current timestamp
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Create archived folder if it doesn't exist
    if not os.path.exists('archived'):
        os.makedirs('archived')

    # Create a folder with the name of the CSV file in the archived folder if it doesn't exist
    foldername = os.path.splitext(os.path.basename(csv_file_path))[0]
    if not os.path.exists(f'archived/{foldername}'):
        os.makedirs(f'archived/{foldername}')

    # Write the SQL table to a CSV file in the archived folder
    with open(f'archived/{foldername}/{table_name}_{timestamp}.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        for row in rows:
            csvwriter.writerow(row)

    print(f"CSV file saved in archived/{foldername}/{table_name}_{timestamp}.csv from update_records.")