from dataclasses import dataclass, field
from pathlib import Path

@dataclass
class Config:
    base_dir: Path = field(default_factory=Path.cwd)

    @property
    def ridership_dir(self) -> Path:
        return self.base_dir / "data" / "POGOH" / "raw data" / "ridership data"

    @property
    def station_file(self) -> Path:
        return self.base_dir / "data" / "POGOH" / "raw data" / "station data" / "pogoh-station-locations-october-2023.xlsx"