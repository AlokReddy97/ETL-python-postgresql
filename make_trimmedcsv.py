import os
import datetime
import csv
import sys

def process_csv_file(filename, n=4):
    # Check if file exists
    if not os.path.isfile(filename):
        print(f"Error: File '{filename}' does not exist")
        return
    
    # Open the file
    with open(filename, 'r') as file:
        # Initialize variables to store the maximum column count and the rows with that count
        max_columns = 0
        max_rows = []

        # Iterate over each line in the file
        for line in file:
            # Count the number of commas in the line
            comma_count = line.count(',')
            # If the number of commas is greater than the current maximum column count,
            # update the maximum column count and reset the rows with that count
            if comma_count > max_columns:
                max_columns = comma_count
                max_rows = [line]
            # If the number of commas is equal to the current maximum column count,
            # add the row to the list of rows with that count
            elif comma_count == max_columns:
                max_rows.append(line)

    # Get current timestamp
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Create processed folder if it doesn't exist
    if not os.path.exists('processed'):
        os.makedirs('processed')

    # Create a folder with the name of the CSV file in the processed folder if it doesn't exist
    foldername = os.path.splitext(filename)[0]
    if not os.path.exists(f'processed/{foldername}'):
        os.makedirs(f'processed/{foldername}')

    processed_filename = f'processed/{foldername}/{os.path.splitext(filename)[0]}_{timestamp}.csv'
    with open(processed_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quotechar="'")
        with open(filename, 'r') as file1:
            for line in file1:
                comma_count = line.count(',')
                if n <= comma_count <= max_columns:
                    # Strip extra spaces and double quotes from each value and write to the new CSV file
                    row = [value.strip().strip('"') for value in line.strip().split(',')]
                    writer.writerow(row)

    with open(processed_filename, 'r') as file:
        csv_reader = csv.reader(file)
        rows = [row for row in csv_reader]

    with open(processed_filename, 'w', newline='') as file:
        csv_writer = csv.writer(file)
        if rows: # check if rows list is not empty
            # Modify the first row by removing extra spaces and $ symbol
            first_row = [col.replace(' ', '').strip('"').replace('$', '').replace('(', '').replace(')', '') for col in rows[0]]
            csv_writer.writerow(first_row)
            # Write the remaining rows back to the file
            for row in rows[1:]:
                csv_writer.writerow(row)
        else:
            print(f"Error: File '{processed_filename}' is empty.")
            sys.exit(1)

    # Print confirmation message
    print(f"CSV file '{filename}' processed successfully and saved as '{processed_filename}'.")

if len(sys.argv) < 3:
    print("Error: Please provide both the CSV filename and a value for n.")
    exit()

filename = sys.argv[1]
n = int(sys.argv[2])

# Process the specified CSV file
process_csv_file(filename, n)



# from compare_csv_diff import compare_csv_files
from compare_csvdiff import compare_csv_files

headers_match = compare_csv_files(filename)

no_archive_file, header_matching = compare_csv_files(filename)

print(no_archive_file)
print(header_matching)

from sqlcsv import create_table

# create_table(filename, no_archive_file=False)

# if no_archive_file:
create_table(filename,no_archive_file)
    

# if headers_match:
#     # perform some operation
# else:
#     # perform some other operation


# from csv_compare import csvtable_comparisions

# current_file_folder_name = os.path.splitext(filename)[0]
# processed_folder = os.path.join('processed', current_file_folder_name)

# try:
#     # Get the most recent timestamped CSV file in the processed folder
#     processed_filenames = [f for f in os.listdir(processed_folder) if f.endswith('.csv')]
#     print(processed_filenames)
#     if not processed_filenames:
#         raise ValueError(f"No processed CSV files found in folder '{processed_folder}'")
#     latest_processed_filename = max(processed_filenames)
#     processed_filepath = os.path.join(processed_folder, latest_processed_filename)
#     print(processed_filepath)
# except (FileNotFoundError, ValueError) as e:
#     print(str(e))
    
# csvtable_comparisions(processed_filepath, 'a')
