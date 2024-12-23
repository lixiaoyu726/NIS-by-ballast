# given trip based NIS probability aggregate for port
# High-order risk (not finished)


from pathlib import Path
import typing as tp
import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).parent.joinpath("data")

# default record output
TRIP_RECORD_DIR = DATA_DIR.joinpath("trip_record.txt")

trip_file_columns = [
    "o_port",
    "o_port_id",
    "o_port_lat",
    "o_port_lon",
    "o_port_country",
    "d_port",
    "d_port_id",
    "d_port_lat",
    "d_port_lon",
    "d_port_country",
    "vessel_id",
    "vessel_imo",
    "vessel_type",
    "vessel_dwt",
    "arrival_date",
    "departure_date",
    "distance",
    "spd",
    "voyage_duration",
    "stay_duration",
    "ballast_risk",
    "biofouling_risk",
]


class AggregateRisk:
    def __init__(self, trip_file_address: str = TRIP_RECORD_DIR):
        with open(trip_file_address, "r") as f:
            file = f.readlines()
        file = [line.strip().split("|") for line in file]
        file = pd.DataFrame(file, columns=trip_file_columns)
        file = file[(file["ballast_risk"] != 0) | (file["biofouling_risk"] != 0)]
        file.reset_index(drop=True, inplace=True)
        file["ballast_risk"] = file["ballast_risk"].astype(float)
        file["biofouling_risk"] = file["biofouling_risk"].astype(float)
        self.record = file

    def aggregate_one_port(self, port_name: str) -> tp.List[float]:
        F = self.record[self.record["d_port"] == port_name]
        agg_ballast_risk = 1.0
        agg_biofouling_risk = 1.0
        for _, line in F.iter_rows():
            agg_ballast_risk *= 1 - line["ballast_risk"]
            agg_biofouling_risk *= 1 - line["biofouling_risk"]
        agg_ballast_risk = 1 - agg_ballast_risk
        agg_biofouling_risk = 1 - agg_biofouling_risk
        return agg_ballast_risk, agg_biofouling_risk
