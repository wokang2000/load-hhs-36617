import psycopg
import pandas as pd
import sys
import credentials
from datetime import datetime
from psycopg import errors
import queries
import helper_functions as hf


def check_and_update_static_data(conn, data, columns):
    """
    Checks for discrepancies between HospitalsQualityDetails and
    HospitalSpecificDetails and updates them.

    Parameters:
    conn (psycopg.Connection): Database connection object.
    data (pd.DataFrame): Processed CMS hospital data to be compared.
    columns (list): List of column to be compared and updated.

    Notes:
    - The function performs the following tasks:
      - Reads existing data from the `HospitalSpecificDetails` table based on
        the `hospital_pk` values in the batch.
      - Identifies discrepancies between the batch data and
        `HospitalSpecificDetails` by performing a join and mismatched columns.
      - Constructs a DataFrame of discrepancies (discrepancies_df) with
        records that do not match the existing data in HospitalSpecificDetails
      - Executes an update query to modify mismatched rows in
        `HospitalSpecificDetails` based on `hospital_pk`.
    """

    data = data.copy()
    # read HospitalSpecificDetails for hospital_pk in qualtiy data batch
    with conn.transaction():
        cur = conn.cursor()

        # Execute the query with the hospital_pks tuple as a parameter
        hospital_pks = list(data['hospital_pk'])
        placeholders = ','.join(['%s'] * len(hospital_pks))
        static_data_check_query = f"""
            SELECT hospital_pk, hospital_name, address, city, zip, state
            FROM HospitalSpecificDetails
            WHERE hospital_pk IN ({placeholders})
        """
        cur.execute(static_data_check_query, (tuple(hospital_pks)))

        static_data = cur.fetchall()
        h_df = pd.DataFrame(static_data, columns=columns)
        print(h_df)

        # join h_df with the batch data, and see which rows are not in h_df
        data = data[columns]
        data = data[data['hospital_pk'].isin(list(h_df['hospital_pk']))]
        h_df = h_df.merge(data, on=columns, how="right", indicator=True)
        # if the static values does not match, indicator will be 'right_only'

        discrepencies_df = h_df[h_df['_merge'] == 'right_only'].\
            drop(columns=['_merge'])
        # fix the order of columns to be inline with update query
        col_order = [x for x in columns if x not in ['hospital_pk']] \
            + ['hospital_pk']

        # Before inserting values into HospitalQualityDetails, update
        # the values for these discrepencies
        update_values = [
            (tuple(row[col] for col in col_order))
            for idx, row in discrepencies_df.iterrows()
        ]
        cur.executemany(queries.STATIC_DETAILS_UPDATE_QUERY, update_values)
        print("Updation Successful for HospitalSpecificData")


def batch_insert_cms_data(conn, data, batch_size=100):
    """
    Inserts CMS hospital quality data into two database tables in batches,
    with handling for foreign key violations.

    Parameters:
    conn (psycopg.Connection): Database connection object.
    data (pd.DataFrame): Processed CMS hospital data to be inserted.
    batch_size (int): Number of records to process per batch. Default is 100.

    Notes:
    - The function performs the following transformations:
      - Defines SQL insertion queries for insertion.
      - If any hospital details in the quality data differ from those stored in
        HospitalSpecificDetails, the function updates them
      - Inserts rows in 'HospitalQualityDetails' table in batches.
      - Handles ForeignKeyViolation error by first inserting into
        'HospitalSpecificDetails' if required, and then retries insertion into
        'HospitalQualityDetails'.
      - Uses 'ON CONFLICT DO NOTHING' to prevent duplicate entries on conflict.
    """

    cur = conn.cursor()

    quality_data_cols = [
        'hospital_pk',
        'last_updated',
        'hospital_overall_rating',
        'hospital_ownership',
        'emergency_services'
    ]

    static_data_cols = [
        'hospital_pk',
        'hospital_name',
        'address',
        'city',
        'zip',
        'state'
    ]

    # insert rows in HospitalQualityDetails in batches
    for row_index in range(0, len(data), batch_size):
        batch_df = data[row_index:row_index + batch_size]
        print("Running process for batch", (row_index // batch_size) + 1)
        print("Number of rows in batch ", (row_index // batch_size) + 1,
              batch_size)

        # Check if hospital-specific column in quality data matches
        # HospitalSpecificDetails, update if not
        check_and_update_static_data(conn, batch_df, static_data_cols)

        # Prepare values for insertion in HospitalQualityDetails
        quality_values = [
            (tuple(row[col] for col in quality_data_cols))
            for idx, row in batch_df.iterrows()
        ]

        try:
            with conn.transaction():
                cur.executemany(queries.HOSPITAL_QUALTIY_DETAILS_INSERT_QUERY,
                                quality_values)
                print("Insertion successful for HospitalQualityDetails")
        except errors.ForeignKeyViolation:
            # Handle foreign key violation by inserting
            # into HospitalSpecificDetails first
            print("Foreign key violation encountered")
            print("Inserting into HospitalSpecificDetails.")

            # Prepare values for insertion into HospitalSpecificDetails
            static_values = [
                (tuple(row[col] for col in static_data_cols))
                for idx, row in batch_df.iterrows()
            ]

            # Insert into HospitalSpecificDetails to resolve FK dependency
            with conn.transaction():
                cur.executemany(queries.STATIC_DETAILS_INSERT_QUERY,
                                static_values)
                print("Insertion successful for HospitalSpecificDetails")

            # Reinserting into HospitalQualityDetails after resolving FK error
            with conn.transaction():
                cur.executemany(queries.HOSPITAL_QUALTIY_DETAILS_INSERT_QUERY,
                                quality_values)
                print("Insertion successful for HospitalQualityDetails")

        except Exception as e:
            print(f"Error in batch {(row_index // batch_size) + 1}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: load-quality.py <last_updated> <file_path> ")
        sys.exit(1)

    # Get file path and last_updated date from command-line arguments
    file_path = sys.argv[2]
    last_updated = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()

    data = pd.read_csv(file_path)
    print("Data has ", len(data), " rows in total")
    # insert last_updated column in the data. We get it from sys.args
    data['last_updated'] = last_updated

    processed_data = hf.process_cms_data(data)

    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )

    batch_size = 100

    batch_insert_cms_data(conn, processed_data, batch_size)
