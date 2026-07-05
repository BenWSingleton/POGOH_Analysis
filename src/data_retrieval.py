import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data" / "pogoh" / "raw_data" / "ridership_data"

URL = 'https://data.wprdc.org/dataset/pogoh-trip-data'

def check_if_month_data_exists(file_name : str, 
                               target_dir, 
                               verbose=True
                               ) -> bool: 
    file_path = target_dir / file_name

    exists = file_path.exists()

    if verbose:
        status = "Found" if exists else "Did not find"
        print(f"{status} {file_path.name}")
    
    return exists

def pull_data(full_link: str, 
              file_name: str, 
              output_dir: Path, 
              verbose: bool = False
              ) -> bool:
    file_path = output_dir / file_name

    try:
        response = requests.get(full_link)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if verbose: print(f"Failed to retrieve {file_name} — {e}")
        return False
    except requests.exceptions.RequestException as e:
        if verbose: print(f"Network error when retrieving {file_name} — {e}")
        return False

    if verbose: print(f"Retrieved {file_name}")

    df = pd.read_excel(BytesIO(response.content))
    df.to_excel(file_path, index=False)

    if verbose: print(f"Saved {file_name} to {file_path}")

    return True

def get_data(verbose: bool = False) -> list[str]:
    site_response = requests.get(URL)
    html = site_response.text
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a", class_="resource-url-analytics")
    
    downloaded = []

    for link in links:
        full_link = str(link.get("href"))
        file_name = Path(urlparse(full_link).path).name

        month_exists = check_if_month_data_exists(file_name, DATA_DIR, verbose)
        if not month_exists:
            success = pull_data(full_link, file_name, DATA_DIR, verbose)
            if success:
                downloaded.append(f"{file_name}")
    
    return downloaded