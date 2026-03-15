import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
import calendar
import os
from pathlib import Path

url = 'https://data.wprdc.org/dataset/f376ccbc-c851-4b58-8b59-5dd9edc736ee/resource/a27865cf-5621-43f9-9ee9-06f65ba9d544/download/'
path = os.getcwd() + "\\data\\POGOH\\raw data\\ridership data\\"

def check_if_month_data_exists(month: str, year: int, verbose: bool = False) -> bool: 
    file_path = Path(path) / f"{month}-{year}.xlsx"

    exists = file_path.exists()

    if verbose:
        status = "Found" if exists else "Did not find"
        print(f"{status} {file_path.name}")
    
    return exists

def pull_data(month: str, year: int, verbose: bool = False) -> bool:
    file = f"{month}-{year}.xlsx"
    month_url = f"{url}/{file}"
    file_path = Path(path) / file

    try:
        response = requests.get(month_url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if verbose: print(f"Failed to retrieve {file} — {e}")
        return False
    except requests.exceptions.RequestException as e:
        if verbose: print(f"Network error when retrieving {file} — {e}")
        return False

    if verbose: print(f"Retrieved {file}")
    df = pd.read_excel(BytesIO(response.content))
    df.to_excel(file_path, index=False)
    if verbose: print(f"Saved {file} to {file_path}")
    return True

def get_data(start_year: int = 2022, start_month: int = 5, verbose: bool = False):
    now = datetime.now()
    end_year = now.year
    end_month = now.month

    downloaded = []

    for year in range(start_year, end_year + 1):
        month_start = start_month if year == start_year else 1
        month_end = end_month if year == end_year else 12

        for month in range(month_start, month_end + 1):
            month_name = calendar.month_name[month].lower()
            month_exists = check_if_month_data_exists(month_name, year, verbose)
            if not month_exists:
                success = pull_data(month_name, year, verbose)
                if success:
                    downloaded.append(f"{month_name}-{year}.xlsx")

    return downloaded

if __name__ == "__main__":
    get_data(verbose=True)