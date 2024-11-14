import sys
import pandas as pd
import psycopg
import credentials
import queries
import helper_functions


BATCH_SIZE = 100


def load_data(file_path):
    """Load and preprocess CSV data."""
    data = pd.read_csv(file_path)
    return helper_functions.preprocess(data)


def create_tables(cursor):
    """Create required tables if they do not already exist."""
    cursor.execute(queries.HOSPITAL_SPECIFIC_DETAILS_CREATE_QUERY)
    cursor.execute(queries.HOSPITAL_LOGISTICS_CREATE_QUERY)


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
        sys.exit(1)

    csv_file = sys.argv[1]
    data = load_data(csv_file)
    
    # Prepare data tuples for batch insertion
    hospital_specific_details = data[['hospital_pk', 'state', 'hospital_name', 'address', 'city', 'zip', 'fips_code', 'longitude', 'latitude']].values.tolist()
    hospital_logistics = data[['hospital_pk', 'collection_week', 'all_adult_hospital_beds_7_day_avg', 'all_pediatric_inpatient_beds_7_day_avg', 
                               'all_adult_hospital_inpatient_bed_occupied_7_day_avg', 'all_pediatric_inpatient_bed_occupied_7_day_avg', 
                               'total_icu_beds_7_day_avg', 'icu_beds_used_7_day_avg', 'inpatient_beds_used_covid_7_day_avg', 
                               'staffed_icu_adult_patients_confirmed_covid_7_day_avg']].values.tolist()
    
    try:
        with psycopg.connect(
            host="pinniped.postgres.database.azure.com",
            dbname=credentials.DB_USER,
            user=credentials.DB_USER,
            password=credentials.DB_PASSWORD,
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                # Create tables
                create_tables(cur)
                
                # Insert hospital specific details in batches
                batch_insert_data(cur, queries.HOSPITAL_SPECIFIC_DETAILS_INSERT_QUERY, hospital_specific_details, BATCH_SIZE, "Hospital Specific Details")

                # Insert hospital logistics data in batches
                batch_insert_data(cur, queries.HOSPITAL_LOGISTICS_INSERT_QUERY, hospital_logistics, BATCH_SIZE, "Hospital Logistics")

    except psycopg.OperationalError as e:
        print("Database connection error:", e)


if __name__ == "__main__":
    main()
