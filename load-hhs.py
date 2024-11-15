import sys
import pandas as pd
import psycopg
from psycopg import errors
import credentials
import queries
import helper_functions


BATCH_SIZE = 1000


def load_data(file_path):
    """Load and preprocess CSV data."""
    data = pd.read_csv(file_path)
    data = helper_functions.process_hhs_data(data)
    return data


def batch_insert_data(cursor, query, data, batch_size, table_name):
    """Insert data in batches."""
    for row in range(0, len(data), batch_size):
        batch = data[row:row + batch_size]
        try:
            cursor.executemany(query, batch)
        except psycopg.IntegrityError as e:
            print(f"Batch insertion into {table_name} failed: {e}")
            cursor.connection.rollback()


def main():
    if len(sys.argv) < 2:
        print("Please provide the CSV file path as an argument.")

    csv_file = sys.argv[1]
    data = load_data(csv_file)

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
                    print("Running process for batch",
                          (row_index // BATCH_SIZE) + 1)
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

                    hospital_logistics_values =\
                        [(row[col] for col in hospital_logistics_columns)
                            for _, row in batch_df.iterrows()]

                    try:
                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_LOGISTICS_INSERT_QUERY,
                                hospital_logistics_values)
                            print("Successfully inserted batch\
                                  into HospitalLogistics table")
                    except errors.ForeignKeyViolation:
                        print("Foreign key violation encountered.")
                        print("Inserting into HospitalSpecificDetails.")
                        hospital_specific_details_values = [
                                (row['hospital_pk'], row['state'],
                                 row['hospital_name'],
                                 row['address'], row['city'],
                                 row['zip'], row['fips_code'],
                                 row['longitude'], row['latitude'])
                                for _, row in batch_df.iterrows()
                            ]

                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_SPECIFIC_DETAILS_INSERT_QUERY,
                                hospital_specific_details_values)
                            print("Successfully inserted batch into \
                                  HospitalSpecificDetails table")

                        with conn.transaction():
                            cur.executemany(
                                queries.HOSPITAL_LOGISTICS_INSERT_QUERY,
                                hospital_logistics_values)
                            print("Successfully inserted batch into \
                                  HospitalLogistics table")

    except psycopg.OperationalError as e:
        print("Database connection error:", e)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
