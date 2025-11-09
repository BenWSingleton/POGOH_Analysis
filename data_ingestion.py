import os
from geopy import distance
import pandas as pd

path = os.getcwd() + "\\data\\POGOH\\raw data\\ridership data\\"
files = os.listdir(path)

neighborhoods = {"Pierce St & Summerlea St": "Shadyside",
                 "Eliza Furnace Trail & Swineburne St": "Hazelwood",
                 "Centre Ave & Addison St": "Middle Hill",
                 "Burns White Center at 3 Crossings": "Strip District",
                 "Allegheny Station": "Chateau",
                 "10th St & Penn Ave": "Downtown",
                 "Liberty Ave & Gross St": "Bloomfield",
                 "Glasshouse": "South Shore",
                 "South Side Trail & S 4th St": "South Side Flats",
                 "Zulema St & Coltart Ave": "Central Oakland",
                 "Ellsworth Ave & N Neville St": "Shadyside",
                 "O'Hara St and University Place": "North Oakland",
                 "S Bouquet Ave & Sennott St": "Central Oakland",
                 "Tioga St & N Homewood Ave": "Homewood South",
                 "Ivy St & Walnut St": "Shadyside",
                 "Penn Ave & Putnam St (Bakery Square)": "Shadyside",
                 "Taylor St & Liberty Ave": "Bloomfield",
                 "21st St & Penn Ave": "Strip District",
                 "42nd St & Butler St": "Central Lawrenceville",
                 "Boulevard of the Allies & Parkview Ave": "Central Oakland",
                 "Liberty Ave & Stanwix St": "Downtown",
                 "Forbes Ave & Market Square": "Downtown",
                 "North Shore Trail & Fort Duquesne Bridge": "North Shore",
                 "Penn Ave & 33rd St": "Lower Lawrenceville",
                 "Penn Ave & S Whitfield St": "East Liberty",
                 "S 27th St & Sidney St. (Southside Works)": "South Side Flats",
                 "W Station Square Dr & Bessemer Court": "South Shore",
                 "Fifth Ave & S Bouquet St": "West Oakland",
                 "Forbes Ave & Schenley Dr": "North Oakland",
                 "Shady Ave & Ellsworth Ave": "Shadyside",
                 "First Ave & B St": "Downtown",
                 "Ross St & Fourth Ave": "Downtown",
                 "Schenley Dr & Schenley Dr Ext": "North Oakland",
                 "N Dithridge St & Centre Ave": "North Oakland",
                 "Butler St & 36th St": "Lower Lawrenceville",
                 "Penn Ave & S Pacific Ave": "Bloomfield",
                 "S Negley Ave & Centre Ave": "Friendship",
                 "Allequippa St & Darragh St": "Terrace Village",
                 "S 12th St & E Carson St": "South Side Flats",
                 "7th St & Penn Ave": "Downtown",
                 "S 22nd St & Sidney St": "South Side Flats",
                 "Second Ave & Tecumseh St": "Hazelwood",
                 "Eliza St & Lytle St": "Hazelwood",
                 "Hamilton Ave & Fifth Ave": "Larimer",
                 "N Braddock Ave & Hamilton Ave": "Homewood South",
                 "N Braddock Ave & Frankstown Ave": "Homewood North",
                 "17th St & Penn Ave": "Strip District",
                 "S Millvale Ave & Centre Ave": "Bloomfield",
                 "Technology Dr & Bates St": "South Oakland",
                 "Atwood St & Bates St": "Central Oakland",
                 "Coltart Ave & Forbes Ave": "Central Oakland",
                 "Bedford Ave & Memory Ln": "Hill District",
                 "Centre Ave & Heldman St": "Crawford-Roberts",
                 "W Ohio St & Brighton Rd": "Allegheny Center",
                 "Brighton Rd & Pennsylvania Ave": "Central Northside",
                 "Rosetta St & N Aiken Ave": "Garfield",
                 "52nd St & Butler St": "Upper Lawrenceville",
                 "W North Ave & Federal St": "Allegheny Center",
                 "Forbes Ave at TCS Hall (CMU Campus)": "North Oakland",
                 "Wilkinsburg Park & Ride": "Wilkinsburg"}

def calculate_distance(row):
    coords_1 = (row['Start Station Latitude'], row['Start Station Longitude'])
    coords_2 = (row['End Station Latitude'], row['End Station Longitude'])
    return distance.distance(coords_1, coords_2).km

def replace_station_name(data, old, new):
    data = data.copy()
    data['Start Station Name'] = data['Start Station Name'].replace(old, new)
    data['End Station Name'] = data['End Station Name'].replace(old, new)
    return data

def replace_station_names(data):
    data = data.copy()
    data = replace_station_name(data, "Burns white at 3 crossings", "Burns White Center at 3 Crossings")
    data = replace_station_name(data, "S 27th St & Sidney St. (Southside Works", "S 27th St & Sidney St. (Southside Works)")
    data = replace_station_name(data, "Shady & Ellsworth", "Shady Ave & Ellsworth Ave")
    data = replace_station_name(data, "Liberty & Stanwix", "Liberty Ave & Stanwix St")
    data = replace_station_name(data, "N Homewood Ave & Finance St", "Tioga St & N Homewood Ave")
    data = replace_station_name(data, "33rd & Penn Ave", "Penn Ave & 33rd St")
    data = replace_station_name(data, "Eliza Furnace Trail at Swineburne St", "Eliza Furnace Trail & Swineburne St")
    data = replace_station_name(data, "Fifth Ave & N Bouquet St", "Fifth Ave & S Bouquet St")
    data = replace_station_name(data, "North Shore Trail", "North Shore Trail & Fort Duquesne Bridge")
    data = replace_station_name(data, "Penn Ave & S Whitfield", "Penn Ave & S Whitfield St")
    data = replace_station_name(data, "Parkview & Blvd of the Allies", "Boulevard of the Allies & Parkview Ave")
    data = replace_station_name(data, "Forbes & Market", "Forbes Ave & Market Square")
    data = replace_station_name(data, "Forbes Ave & Grant St", "Ross St & Fourth Ave")
    return data

def load_data(file_path="\\data\\POGOH\\raw data\\ridership data\\"):
    path = os.getcwd() + file_path
    files = os.listdir(path)
    data = pd.DataFrame()

    for f in files:
        month = pd.read_excel('./data/POGOH/raw data/ridership data/' + f)
        data = pd.concat([data, month])

    return data

def process_data(verbose=False):
    if verbose: print("Loading data...")
    df = load_data()
    stations = pd.read_excel('./data/POGOH/raw data/station data/pogoh-station-locations-october-2023.xlsx')

    df['End Station Id'] = df['End Station Id'].fillna(0)
    df['End Station Id'] = df['End Station Id'].astype('int64')
    df['End Station Name'] = df['End Station Name'].fillna('None')

    stations['Neighborhood'] = stations['Name'].map(neighborhoods)

    if verbose: print("Merging station data...")
    df = pd.merge(df, stations.drop(columns=['Name']).add_prefix('Start Station '), left_on='Start Station Id', right_on='Start Station Id', how='inner')
    df = pd.merge(df, stations.drop(columns=['Name']).add_prefix('End Station '), left_on='End Station Id', right_on='End Station Id', how='inner')

    if verbose: print("Processing cleaning data...")
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['End Date'] = pd.to_datetime(df['End Date']) 

    df['Closed Status'] = df['Closed Status'].fillna("None")

    df['Trip Distance (km)'] = df.apply(calculate_distance, axis=1)
    df['Trip Time'] = df['End Date'] - df['Start Date']

    df = df[(df['Start Station Name'] != "Test-STATION") & (df['End Station Name'] != "Test-STATION")]

    if verbose: print("Replacing station names...")
    df = replace_station_names(df)

    return df