def extract_coordinates(x):
    if x == "NA" or pd.isna(x):
        return None, None
    else:
        # Remove the 'POINT (' and ')', and split the coordinates
        coords = x.replace("POINT (", "").replace(")", "").split()
        longitude = float(coords[0])  # First value is longitude
        latitude = float(coords[1])   # Second value is latitude
        return longitude, latitude
