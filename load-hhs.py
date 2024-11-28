import sys
import pandas as pd
import psycopg
from psycopg import errors
import credentials
import queries
import helper_functions
import logging

BATCH_SIZE = 1000

# logging configuration
logging.basicConfig(
    filename='hhs_data_loading.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def load_data(file_path):
    """Load and preprocess CSV data."""
    try:
        data = pd.read_csv(file_path)
        data = helper_functions.process_hhs_data(data)
        logging.info(f"Data loaded and preprocessed from {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        raise


def batch_insert_data(cursor, query, data, batch_size, table_name):
    """Insert data in batches."""
    for row in range(0, len(data), batch_size):
        batch = data[row:row + batch_size]
        try:
            cursor.executemany(query, batch)
            logging.info("Successfully inserted batch of size"
                         f"{len(batch)} into {table_name}")
        except psycopg.IntegrityError as e:
            logging.error(f"Batch insertion into {table_name} failed: {e}")
            cursor.connection.rollback()
        except Exception as e:
            logging.error("Unexpected error during batch insertion into"
                          f"{table_name}: {e}")
            cursor.connection.rollback()


def main():
    if len(sys.argv) < 2:
        logging.error("Please provide the CSV file path as an argument.")
        print("Please provide the CSV file path as an argument.")
        sys.exit(1)

    csv_file = sys.argv[1]
    try:
        data = load_data(csv_file)
        data = data.astype(str)
        data = data.applymap(lambda x: None if x == 'nan' else x)
    except Exception as e:
        logging.error(f"Error processing the data: {e}")
        sys.exit(1)

    try:
        with psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=credentials.DB_USER,
            user=credentials.DB_USER,
            password=credentials.DB_PASSWORD,
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:

                for row_index in range(0, len(data), BATCH_SIZE):
                    batch_df = data[row_index:row_index + BATCH_SIZE]
                    logging.info(f"Running process for batch "
                                 f"{(row_index // BATCH_SIZE) + 1}")
                    hospital_logistics_columns = [
                        'hospital_pk',
                        'collection_week',
                        'all_adult_hospital_beds_7_day_avg',
                        'all_pediatric_inpatient_beds_7_day_avg',
                        'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
                        'all_pediatric_inpatient_bed_occupied_7_day_avg',
                        'total_icu_beds_7_day_avg',
                        'icu_beds_used_7_day_avg',
                        'inpatient_beds_used_covid_7_day_avg',
                        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
                    ]

                    hospital_logistics_values = [
                        tuple(row[col] for col in hospital_logistics_columns)
                        for _, row in batch_df.iterrows()
                    ]

                    try:
                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_LOGISTICS_INSERT_QUERY,
                                hospital_logistics_values)
                            logging.info("Successfully inserted batch with "
                                         f"{len(batch_df)} "
                                         "rows into HospitalLogistics table")
                    except errors.ForeignKeyViolation:
                        logging.warning("Foreign key violation encountered.")
                        logging.info("Inserting into HospitalSpecificDetails.")
                        hospital_specific_details_values = [
                            (row['hospital_pk'], row['state'],
                             row['hospital_name'], row['address'],
                             row['city'], row['zip'], row['fips_code'],
                             row['longitude'], row['latitude'])
                            for _, row in batch_df.iterrows()
                        ]

                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_SPECIFIC_DETAILS_INSERT_QUERY,
                                hospital_specific_details_values)
                            print("Successfully inserted batch with "
                                  f"{len(batch_df)} rows into "
                                  "HospitalSpecificDetails table")
                            logging.info("Successfully inserted batch with "
                                         f"{len(batch_df)} rows into "
                                         "HospitalSpecificDetails table")
                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_LOGISTICS_INSERT_QUERY,
                                hospital_logistics_values)
                            logging.info("Successfully inserted batch with "
                                         f"{len(batch_df)} rows into "
                                         "HospitalLogistics table")

    except psycopg.OperationalError as e:
        logging.error(f"Database connection error: {e}")
    finally:
        cur.close()
        logging.info("Cursor closed.")
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
