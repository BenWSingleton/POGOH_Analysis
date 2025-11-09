import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
import calendar
import os

url = 'https://data.wprdc.org/dataset/f376ccbc-c851-4b58-8b59-5dd9edc736ee/resource/a27865cf-5621-43f9-9ee9-06f65ba9d544/download/'
path = os.getcwd() + "\\data\\POGOH\\raw data\\ridership data\\"

def check_for_month(month, year, verbose=False): 
    file = f"{month}-{year}.xlsx"
    file_path = path + "\\" + file
    
    if os.path.exists(file_path):
        if verbose: print(f"Found {file}")
        return True
    else:
        if verbose: print(f"Did not find {file}")
        return False

def pull_data(month, year, verbose=False):
    file = f"{month}-{year}.xlsx"
    month_url = url + '/' + file
    response = requests.get(month_url)

    if response.status_code==200:
        if verbose: print(f"Retrieved {file}")
        df = pd.read_excel(BytesIO(response.content))
        file_path = path + '/' + file
        df.to_excel(file_path, index=False)
        if verbose: print(f"Saved {file} to {file_path}")
    else:
        print("Failed to retrieve data")

def get_data(verbose=False):
    start_year = 2022
    start_month = 5

    now = datetime.now()
    end_year = now.year
    end_month = now.month

    for year in range(start_year, end_year+1):
        month_start = start_month if year == start_year else 1
        # End at current month only for the current year
        month_end = end_month if year == end_year else 12

        for month in range(month_start, month_end + 1):
            month_name = calendar.month_name[month].lower()
            month_exists = check_for_month(month_name, year, verbose)
            if not month_exists:
                pull_data(month_name, year, verbose)
        

if __name__ == "__main__":
    pull_data('october', '2025', verbose=True)