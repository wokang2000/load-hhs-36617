"""
This module contains SQL queries for creating and inserting data into the
HospitalLogistics and HospitalSpecificDetails tables in the hospital database.
"""

# Hospital Logistics Queries

HOSPITAL_LOGISTICS_CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS HospitalLogistics (
    hospital_pk TEXT REFERENCES HospitalSpecificDetails(hospital_pk),
    collection_week DATE CHECK (collection_week <= CURRENT_DATE::DATE),
    all_adult_hospital_beds_7_day_avg NUMERIC
        CHECK (all_adult_hospital_beds_7_day_avg >= 0),
    all_pediatric_inpatient_beds_7_day_avg NUMERIC
        CHECK (all_pediatric_inpatient_beds_7_day_avg >= 0),
    all_adult_hospital_inpatient_bed_occupied_7_day_avg NUMERIC
        CHECK (all_adult_hospital_inpatient_bed_occupied_7_day_avg >= 0),
    all_pediatric_inpatient_bed_occupied_7_day_avg NUMERIC
        CHECK (all_pediatric_inpatient_bed_occupied_7_day_avg >= 0),
    total_icu_beds_7_day_avg NUMERIC CHECK (total_icu_beds_7_day_avg >= 0),
    icu_beds_used_7_day_avg NUMERIC CHECK (icu_beds_used_7_day_avg >= 0),
    inpatient_beds_used_covid_7_day_avg NUMERIC
        CHECK (inpatient_beds_used_covid_7_day_avg >= 0),
    staffed_icu_adult_patients_confirmed_covid_7_day_avg NUMERIC
        CHECK (staffed_icu_adult_patients_confirmed_covid_7_day_avg >= 0),
    PRIMARY KEY (hospital_pk, collection_week),
    CONSTRAINT check_total_beds_greater_than_used_beds CHECK (
       total_icu_beds_7_day_avg >= icu_beds_used_7_day_avg
    )
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
) VALUES (%s, CAST(%s AS DATE), %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (hospital_pk, collection_week) DO NOTHING;
"""

# Hospital Specific Details Queries

HOSPITAL_SPECIFIC_DETAILS_CREATE_QUERY = """
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
ON CONFLICT (hospital_pk) DO NOTHING;
"""

HOSPITAL_QUALITY_DETAILS_CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS HospitalQualityDetails (
  hospital_pk TEXT REFERENCES HospitalSpecificDetails(hospital_pk),
  last_updated DATE CHECK (last_updated <= CURRENT_DATE),
  hospital_overall_rating NUMERIC,
  hospital_ownership TEXT,
  emergency_services BOOLEAN,
  PRIMARY KEY (hospital_pk, last_updated)
);
"""

HOSPITAL_QUALTIY_DETAILS_INSERT_QUERY = """
    INSERT INTO HospitalQualityDetails (
        hospital_pk, last_updated, hospital_overall_rating,
        hospital_ownership, emergency_services
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (hospital_pk, last_updated) DO NOTHING;
"""

STATIC_DETAILS_INSERT_QUERY = """
    INSERT INTO HospitalSpecificDetails (
        hospital_pk, hospital_name, address, city, zip, state
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (hospital_pk) DO NOTHING;
"""

STATIC_DETAILS_UPDATE_QUERY = """
    UPDATE HospitalSpecificDetails
    SET
        hospital_name = %s,
        address = %s,
        city = %s,
        zip = %s,
        state = %s
    WHERE hospital_pk = %s;
"""
