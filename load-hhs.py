import psycopg
import credentials
import helper_functions
import sys
import pandas as pd


csv_file = sys.argv[1]
data = pd.read_csv(csv_file)

# Data Cleaning .csv file for HospitalLogistics
data = data[data['hospital_pk'].str.len() == 6]
data['collection_week'] = pd.to_datetime(data['collection_week'], errors='coerce').dt.date
data['all_adult_hospital_beds_7_day_avg'] = data['all_adult_hospital_beds_7_day_avg'].apply(lambda x: None if x == 'NA' else x)
data['all_pediatric_inpatient_beds_7_day_avg'] = data['all_pediatric_inpatient_beds_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['all_adult_hospital_inpatient_bed_occupied_7_day_avg'] = data['all_adult_hospital_inpatient_bed_occupied_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['all_pediatric_inpatient_bed_occupied_7_day_avg'] = data['all_pediatric_inpatient_bed_occupied_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['total_icu_beds_7_day_avg'] = data['total_icu_beds_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['icu_beds_used_7_day_avg'] = data['icu_beds_used_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['inpatient_beds_used_covid_7_day_avg'] = data['inpatient_beds_used_covid_7_day_avg'].apply(lambda x: None if x == -999999 else x)
data['staffed_icu_adult_patients_confirmed_covid_7_day_avg'] = data['staffed_icu_adult_patients_confirmed_covid_7_day_avg'].apply(lambda x: None if x == -999999 else x)

# Data Cleaning .csv file for HospitalSpecificDetails
data['state'] = data['state'].apply(lambda x: x if re.match(r'^[a-zA-Z]{2}$', str(x)) else None)
data['hospital_name'] = data['hospital_name'].apply(lambda x: None if x == "NA" else x)
data['address'] = data['address'].apply(lambda x: None if x == "NA" else x)
data['city'] = data['city'].apply(lambda x: None if x == "NA" else x)
data['zip'] = data['zip'].apply(lambda x: None if x == "NA" else x)
data['fips_code'] = data['fips_code'].apply(lambda x: None if x == "NA" else x)
data[['longitude', 'latitude']] = data['geocoded_hospital_address'].apply(
    lambda x: pd.Series(helper_functions.extract_coordinates(x))
)

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", 
    dbname=credentials.DB_USER,
    user=credentials.DB_USER, 
    password=credentials.DB_PASSWORD
)

cur = conn.cursor()

insert_query_table_1 = """
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

insert_query_table_2 = """
    INSERT INTO HospitalSpecificDetails (
        hospital_pk,
        state,
        hospital_name,
        address,
        city,
        zip,
        fips_code,
        longitude,
        latitude
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

batch_size = 100

data_tuples_table_2 = [
    (
        row['hospital_pk'],
        row['state'],
        row['hospital_name'],
        row['address'],
        row['city'],
        row['zip'],
        row['fips_code'],
        row['longitude'],
        row['latitude']
    )
    for _, row in data[['hospital_pk', 'state', 'hospital_name', 'address', 'city', 'zip', 'fips_code', 'longitude', 'latitude']].iterrows()
]

data_tuples_table_1 = [
    (
        row['hospital_pk'],
        row['collection_week'],  # Ensure collection_week is in datetime format
        row['all_adult_hospital_beds_7_day_avg'],
        row['all_pediatric_inpatient_beds_7_day_avg'],
        row['all_adult_hospital_inpatient_bed_occupied_7_day_avg'],
        row['all_pediatric_inpatient_bed_occupied_7_day_avg'],
        row['total_icu_beds_7_day_avg'],
        row['icu_beds_used_7_day_avg'],
        row['inpatient_beds_used_covid_7_day_avg'],
        row['staffed_icu_adult_patients_confirmed_covid_7_day_avg']
    )
    for _, row in data[['hospital_pk', 'collection_week', 'all_adult_hospital_beds_7_day_avg', 
                        'all_pediatric_inpatient_beds_7_day_avg', 'all_adult_hospital_inpatient_bed_occupied_7_day_avg', 
                        'all_pediatric_inpatient_bed_occupied_7_day_avg', 'total_icu_beds_7_day_avg', 
                        'icu_beds_used_7_day_avg', 'inpatient_beds_used_covid_7_day_avg', 
                        'staffed_icu_adult_patients_confirmed_covid_7_day_avg']].iterrows()
]

conn.autocommit = True

try:
    cur.execute(
            """
            DROP TABLE IF EXISTS HospitalSpecificDetails CASCADE;
            CREATE TABLE IF NOT EXISTS HospitalSpecificDetails (
                hospital_pk TEXT PRIMARY KEY,
                state CHAR(2),
                hospital_name TEXT,
                address TEXT,
                city TEXT,
                zip CHAR(5),
                fips_code NUMERIC,
                longitude NUMERIC,
                latitude NUMERIC
            );
            """
        )
        
    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS HospitalLogistics (
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
        
        # Insert data into HospitalSpecificDetails (Table 2)
    for row in range(0, len(data_tuples_table_2), batch_size):
        batch = data_tuples_table_2[row:row + batch_size]
        cur.executemany(insert_query_table_2, batch)
        conn.commit()

        # Insert data into HospitalLogistics (Table 1) and handle foreign key violations
    for row in range(0, len(data_tuples_table_1), batch_size):
        batch = data_tuples_table_1[row:row + batch_size]
        try:
            cur.executemany(insert_query_table_1, batch)
            conn.commit()
        except psycopg.IntegrityError as e:
            # Log the error and continue with the next batch
            print("Foreign key constraint violation occurred during HospitalLogistics insertion:", e)
            conn.rollback()  # Rollback only the failing batch, continue with the next batch

except psycopg.errors.ConnectionTimeout as e:
    print("Process took too long. Abort:", e)
