import psycopg
import credentials
import sys
import pandas as pd


csv_file = sys.argv[1]
data = pd.read_csv(csv_file)

# Data Cleaning .csv file for HospitalLogistics
data = data[len(data['hospital_pk']) > 6]
data['collection_week'] = pd.to_datetime(data['collection_week'])
data['all_adult_hospital_beds_7_day_avg'] = data['all_adult_hospital_beds_7_day_avg'].apply(lambda x: None if x == 'NA' else x)
data['all_pediatric_inpatient_beds_7_day_avg'] = data['all_pediatric_inpatient_beds_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['all_adult_hospital_inpatient_bed_occupied_7_day_avg'] = data['all_adult_hospital_inpatient_bed_occupied_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['all_pediatric_inpatient_bed_occupied_7_day_avg'] = data['all_pediatric_inpatient_bed_occupied_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['total_icu_beds_7_day_avg'] = data['total_icu_beds_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['icu_beds_used_7_day_avg'] = data['icu_beds_used_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['inpatient_beds_used_covid_7_day_avg'] = data['inpatient_beds_used_covid_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['staffed_icu_adult_patients_confirmed_covid_7_day_avg'] = data['staffed_icu_adult_patients_confirmed_covid_7_day_avg'].apply(lambda x: None if x == -999999 else x)



conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)

cur = conn.cursor()

insert_query = """
    INSERT INTO HospitalLogistics (
        hospital_pk, 
        collection_week, 
        all_adult_hospital_beds_7_day_avg, 
        all_pediatric_inpatient_beds_7_day_avg, 
        all_adult_hospital_inpatient_bed_occupied_7_day_avg, 
        all_pediatric_inpatient_bed_occupied_7_day_avg, 
        total_icu_beds_7_day_avg, 
        icu_beds_used_7_day_avg, 
        inpatient_beds_used_covid_7_day_avg, 
        staffed_icu_adult_patients_confirmed_covid_7_day_avg
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

batch_size = 100 # Set to 100 by default

data_tuples = [tuple(row) for row in data.to_records(index=False)]
try:
    with conn.transaction():
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS HospitalSpecificDetails (
                    hospital_pk TEXT PRIMARY KEY,
                    state CHAR(2),
                    hospital_name TEXT,
                    address TEXT,
                    city TEXT,
                    zip CHAR(5),
                    fips_code INTEGER,
                    geocoded_hospital_address POINT
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT HospitalLogistics (
                    hospital_pk TEXT PRIMARY KEY REFERENCES HospitalSpecificDetails (hospital_pk),
                    collection_week TIMESTAMP CHECK (collection_week <= CURRENT_DATE::TIMESTAMP),
                    all_adult_hospital_beds_7_day_avg NUMERIC CHECK (all_adult_hospital_beds_7_day_avg >= 0),
                    all_pediatric_inpatient_beds_7_day_avg NUMERIC CHECK (all_pediatric_inpatient_beds_7_day_avg >= 0),
                    all_adult_hospital_inpatient_bed_occupied_7_day_avg NUMERIC CHECK (all_adult_hospital_inpatient_bed_occupied_7_day_avg >= 0),
                    all_pediatric_inpatient_bed_occupied_7_day_avg NUMERIC CHECK (all_pediatric_inpatient_bed_occupied_7_day_avg >= 0),
                    total_icu_beds_7_day_avg NUMERIC CHECK (total_icu_beds_7_day_avg >= 0),
                    icu_beds_used_7_day_avg NUMERIC CHECK (icu_beds_used_7_day_avg >= 0),
                    inpatient_beds_used_covid_7_day_avg NUMERIC CHECK (inpatient_beds_used_covid_7_day_avg >= 0),
                    staffed_icu_adult_patients_confirmed_covid_7_day_avg NUMERIC  CHECK (staffed_icu_adult_patients_confirmed_covid_7_day_avg >= 0)
                );
                """
            )

            for row in range(0, len(data_tuples), batch_size):
                batch = data_tuples[row:row + batch_size]
                cur.executemany(insert_query, batch)
                conn.commit()

        except psycopg.IntegrityError as e:
            print("Foreign key constraint violation occurred:", e)

except psycopg.errors.ConnectionTimeout as e:
    print("Process took too long.  Abort:", e)
