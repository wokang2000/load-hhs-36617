"""
This module contains SQL queries for creating and inserting data into the HospitalLogistics
and HospitalSpecificDetails tables in the hospital database.
"""

# Hospital Logistics Queries

HOSPITAL_LOGISTICS_CREATE_QUERY = """
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
    staffed_icu_adult_patients_confirmed_covid_7_day_avg NUMERIC CHECK (staffed_icu_adult_patients_confirmed_covid_7_day_avg >= 0)
);
"""

HOSPITAL_LOGISTICS_INSERT_QUERY = """
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

# Hospital Specific Details Queries

HOSPITAL_SPECIFIC_DETAILS_CREATE_QUERY = """
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

HOSPITAL_SPECIFIC_DETAILS_INSERT_QUERY = """
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
