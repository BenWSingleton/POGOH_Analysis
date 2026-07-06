from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class Config:
    base_dir: Path = field(default_factory=Path.cwd)

    @property
    def trip_data_url(self) -> str:
        return "https://data.wprdc.org/dataset/pogoh-trip-data"

    @property
    def station_data_url(self) -> str:
        return "https://data.wprdc.org/dataset/station-locations"

    @property
    def ridership_dir(self) -> Path:
        return self.base_dir / "data" / "POGOH" / "raw_data" / "ridership_data"

    @property
    def station_dir(self) -> Path:
        return self.base_dir / "data" / "POGOH" / "raw_data" / "station_data"

    @property
    def station_file(self) -> Path:
        return self.base_dir / "data" / "POGOH" / "raw_data" / "station_data" / "pogoh-station-locations-october-2023.xlsx"