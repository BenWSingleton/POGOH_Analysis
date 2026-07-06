import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from pathlib import Path
from config import Config

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

def pull_ridership_data(full_link: str, 
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

def scrape_links_from_page(url):
    site_response = requests.get(url)
    html = site_response.text
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a", class_="resource-url-analytics")
    return links

def get_latest_station_data(station_data_url: str,
                            station_dir,
                            verbose: bool = False):
    links = scrape_links_from_page(station_data_url)
    latest = ""
    # TO DO: scan all station files and pull only the latest one

    return links

def get_latest_ridership_data(trip_data_url: str, 
                              ridership_dir: Path,
                              verbose: bool = False) -> list[str]:
    downloaded = []

    links = scrape_links_from_page(trip_data_url)
    for link in links:
        full_link = str(link.get("href"))
        file_name = Path(urlparse(full_link).path).name

        month_exists = check_if_month_data_exists(file_name, ridership_dir, verbose)
        if not month_exists:
            success = pull_ridership_data(full_link, file_name, ridership_dir, verbose)
            if success:
                downloaded.append(f"{file_name}")
    
    return downloaded

def get_data(verbose: bool = False) -> list[str]:
    cfg = Config()

    # TO DO: rework to be a dict w/ downloaded and existing files
    downloaded = get_latest_ridership_data(cfg.trip_data_url,
                                           cfg.ridership_dir,
                                           verbose)

    get_latest_station_data(cfg.station_data_url,
                                          cfg.station_dir,
                                          verbose)

    return downloaded

if __name__ == "__main__":
    cfg = Config()
    print(get_latest_station_data(cfg.station_data_url, cfg.station_dir,True))