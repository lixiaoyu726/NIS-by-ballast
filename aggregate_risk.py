# given trip based NIS probability aggregate for port
# High-order risk (not finished)


from pathlib import Path
import typing as tp
import pandas as pd
import numpy as np

from parsers import PortParser

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
    def __init__(
        self,
        trip_file_address: str = TRIP_RECORD_DIR,
        record_file: tp.Optional[pd.DataFrame] = None,
    ):
        if record_file is not None:
            self.record = record_file
        else:
            with open(trip_file_address, "r") as f:
                file = f.readlines()
            file = [line.strip().split("|") for line in file]
            file = pd.DataFrame(file, columns=trip_file_columns)
            file["ballast_risk"] = file["ballast_risk"].astype(float)
            file["biofouling_risk"] = file["biofouling_risk"].astype(float)
            file = file[
                ((file["ballast_risk"] != 0.0) | (file["biofouling_risk"] != 0.0))
                & ((~file["ballast_risk"].isna()) | (~file["biofouling_risk"].isna()))
            ]
            file.reset_index(drop=True, inplace=True)

            self.record = file

    def aggregate_one_port(self, port_name: str) -> tp.List[float]:
        # given desti_port
        # return agg ballast/biofouling risk
        F = self.record[self.record["d_port"] == port_name]
        agg_ballast_risk = 1.0
        agg_biofouling_risk = 1.0
        agg_ballast_risk, agg_biofouling_risk = self.iter_and_multi(
            F, agg_ballast_risk, agg_biofouling_risk
        )
        agg_ballast_risk = 1 - agg_ballast_risk
        agg_biofouling_risk = 1 - agg_biofouling_risk
        return agg_ballast_risk, agg_biofouling_risk

    def aggregate_by_realm(
        self, port_name: str = "Singapore"
    ) -> tp.Tuple[tp.Dict[str, float]]:
        # given desti_port
        # output biofouling/ballast risk organized by 7 eco_realm
        port_parser = PortParser()
        realm_set = set(port_parser.meow_info["REALM"].values)
        bio_risk_dict = {key: 1.0 for key in realm_set}
        ballast_risk_dict = {key: 1.0 for key in realm_set}

        F = self.record[
            (self.record["d_port"] == port_name)
            & (
                (self.record["ballast_risk"] != 0.0)
                | (self.record["biofouling_risk"] != 0.0)
            )
        ]
        origin_port_list = list(set(F["o_port"].values))
        for origin_port in origin_port_list:
            Q = F[F["o_port"] == origin_port]
            o_port_id = int(Q.iloc[0]["o_port_id"])
            o_port_realm = port_parser.check_meow_region(o_port_id)
            _bio_risk = bio_risk_dict[o_port_realm]
            _ballast_risk = ballast_risk_dict[o_port_realm]
            _bio_risk, _ballast_risk = self.iter_and_multi(Q, _bio_risk, _ballast_risk)
            bio_risk_dict[o_port_realm] = _bio_risk
            ballast_risk_dict[o_port_realm] = _ballast_risk
        
        for key in bio_risk_dict:
            bio_risk_dict[key] = 1.0 - bio_risk_dict[key]
        for key in ballast_risk_dict:
            ballast_risk_dict[key] = 1.0 - ballast_risk_dict[key]
        return bio_risk_dict, ballast_risk_dict

    def aggregate_by_country(self, port_name: str='Singapore') -> tp.Tuple[tp.Dict[str, float]]:
        # given desti_port
        # output biofouling/ballast risk organized by origin port country
        F = self.record[
            (self.record["d_port"] == port_name)
            & (
                (self.record["ballast_risk"] != 0.0)
                | (self.record["biofouling_risk"] != 0.0)
            )
        ]        
        origin_country_list = list(set(F["o_port_country"].values))
        bio_risk_dict = dict()
        ballast_risk_dict = dict()
        for origin_country in origin_country_list:
            Q = F[F["o_port_country"] == origin_country]
            _bio_risk = 1.0
            _ballast_risk = 1.0
            _bio_risk, _ballast_risk = self.iter_and_multi(Q, _bio_risk, _ballast_risk)
            bio_risk_dict[origin_country] = 1.0 - _bio_risk
            ballast_risk_dict[origin_country] = 1.0 - _ballast_risk
        return bio_risk_dict, ballast_risk_dict
            
    @staticmethod
    def iter_and_multi(
        df: pd.DataFrame, agg_ballast_risk=1.0, agg_biofouling_risk=1.0
    ) -> tp.Tuple[float]:
        for _, line in df.iterrows():
            ba_r = line["ballast_risk"]
            bio_r = line["biofouling_risk"]
            if ~pd.isna(ba_r):
                agg_ballast_risk *= 1 - ba_r
            if ~pd.isna(bio_r):
                agg_biofouling_risk *= 1 - bio_r
        return agg_ballast_risk, agg_biofouling_risk
