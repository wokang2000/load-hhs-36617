import pandas as pd
import re


def extract_coordinates(point_str):
    """
    Extract longitude and latitude from a geospatial POINT string.

    Parameters:
    - point_str (str): A string in the format 'POINT (longitude latitude)', or 'NA'/None for missing values.

    Returns:
    - tuple: A tuple (longitude, latitude) as floats, or (None, None) if the input is 'NA' or missing.
    """
    if point_str == "NA" or pd.isna(point_str):
        return None, None

    coords = point_str.replace("POINT (", "").replace(")", "").split()
    try:
        longitude = float(coords[0])
        latitude = float(coords[1])
        return longitude, latitude
    except (IndexError, ValueError) as e:
        print(f"Error parsing coordinates: {e}")
        return None, None


def process_hhs_data(data):
    """
    Preprocess hospital data by cleaning and transforming specified columns.

    Parameters:
    - data (pd.DataFrame): The raw data to be preprocessed. Expected columns include:
      'hospital_pk', 'collection_week', 'all_adult_hospital_beds_7_day_avg', 
      'all_pediatric_inpatient_beds_7_day_avg', 'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
      'all_pediatric_inpatient_bed_occupied_7_day_avg', 'total_icu_beds_7_day_avg', 
      'icu_beds_used_7_day_avg', 'inpatient_beds_used_covid_7_day_avg', 
      'staffed_icu_adult_patients_confirmed_covid_7_day_avg', 'state', 'hospital_name', 
      'address', 'city', 'zip', 'fips_code', 'geocoded_hospital_address'.

    Returns:
    - pd.DataFrame: The cleaned and transformed data.
    
    Notes:
    - The function performs the following transformations:
      - Filters for valid hospital primary keys (6 characters).
      - Converts 'collection_week' to date format, handling errors as NaT.
      - Replaces invalid numerical values (-999999 or 'NA') with None.
      - Validates and standardizes two-character state codes.
      - Replaces 'NA' entries in categorical columns with None.
      - Extracts longitude and latitude from 'geocoded_hospital_address'.
    """
    # Filter rows where 'hospital_pk' is 6 characters long
    data = data[data['hospital_pk'].str.len() == 6]
    
    # Convert 'collection_week' to datetime and retain only the date part
    data['collection_week'] = pd.to_datetime(data['collection_week'], errors='coerce').dt.date
    
    # Replace invalid values in bed and occupancy columns with None
    invalid_value_columns = [
        'all_adult_hospital_beds_7_day_avg',
        'all_pediatric_inpatient_beds_7_day_avg',
        'all_adult_hospital_inpatient_bed_occupied_7_day_avg',
        'all_pediatric_inpatient_bed_occupied_7_day_avg',
        'total_icu_beds_7_day_avg',
        'icu_beds_used_7_day_avg',
        'inpatient_beds_used_covid_7_day_avg',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
    ]
    for column in invalid_value_columns:
        data[column] = data[column].apply(lambda x: None if x in ['NA', -999999] else x)

    # Ensure 'state' values are two-letter alphabetical codes
    data['state'] = data['state'].apply(lambda x: x if re.match(r'^[a-zA-Z]{2}$', str(x)) else None)
    
    # Replace 'NA' values in categorical columns with None
    categorical_columns = ['hospital_name', 'address', 'city', 'zip', 'fips_code']
    for column in categorical_columns:
        data[column] = data[column].apply(lambda x: None if x == "NA" else x)
    
    # Extract longitude and latitude from 'geocoded_hospital_address'
    data[['longitude', 'latitude']] = data['geocoded_hospital_address'].apply(
        lambda x: pd.Series(extract_coordinates(x))
    )

    return data
