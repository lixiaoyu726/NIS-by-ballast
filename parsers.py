# RecordParser VesselParser PortParser process origin file
# output VoyageTrip, contains VesselInfo, Desti-PortInfo Origin-PortInfo
import typing as tp
import attr

from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
from pycountry import countries as pc
from pycountry.db import Country as pcountry

DATA_DIR = Path(__file__).parent.joinpath("data")

PLACE_DIR = DATA_DIR.joinpath("places.lst")
VESSEL_DIR = DATA_DIR.joinpath("vessels_1819.txt")


MEOW_DIR = Path(__file__).parent.joinpath("MEOW/meow_ecos.shp")
FEOW_DIR = DATA_DIR.joinpath("FEOW/feow_hydrosheds.shp")
GIVEN_FILE_DIR = DATA_DIR.joinpath("Places_allportdata_mergedSept2017.csv")

TYPE_DICT = {
    "Auto": ["OLC", "PRR", "URC", "URR", "MVE"],
    "Container": ["UBC", "UCC", "UCR"],
    "Tanker": [
        "TAC",
        "TAS",
        "TBK",
        "TCH",
        "TPD",
        "LNG",
        "LNP",
        "LPG",
        "TCR",
        "COO",
        "CBO",
        "TCO",
    ],
    "Passenger": ["MPR", "OFY", "OYT"],
    "Bulk": ["BBU", "BCE", "BOR", "BWC", "UBG"],
    "General": ["BCB", "GCT", "GGC", "GPC"],
    "Other": [
        "XTG",
        "OBA",
        "OTB",
        "XAH",
        "XTS",
        "OSY",
        "OPO",
        "DDR",
        "DTS",
        "XPT",
        "DSS",
        "DTD",
        "OCL",
        "OHL",
        "DHD",
    ],
    "Chemical": ["TAC", "TAS", "TBK", "TCH", "TPD"],
    "Liquified-Gas": ["LNG", "LNP", "LPG"],
    "Oil": ["TCR", "COO", "CBO", "TCO"],
    "Refrigerated-Cargo": ["GRF"],
    "Fishing": ["FFC", "FFF", "FFS", "FTR", "FWH"],
    "Research": ["RRE"],
    "Yacht": ["OYT"],
}
ANTIFOULING_DICT = {
    "Auto": 0.2,
    "Container": 0.19,
    "Tanker": 0.3,
    "Passenger": 0.31,
    "Bulk": 0.42,
    "General": 0.53,
    "Other": 0.63,
    "Chemical": 0.63,
    "Liquified-Gas": 0.63,
    "Oil": 0.63,
    "Refrigerated-Cargo": 0.63,
    "Fishing": 0.63,
    "Research": 0.63,
    "Yacht": 0.63,
}


def vessel_type_code_antifouling():
    output_dict = dict()
    for key, value in TYPE_DICT.items():
        for code in value:
            output_dict[code] = ANTIFOULING_DICT[key]
    return output_dict


def ll_to_sa(lat1, lon1, lat2, lon2):
    # calculate distance(m) and bearing(deg)(bearing of point2) from lat-lon
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    R = 6371.0
    distance = R * c * 1000  # m

    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
    bearing_rad = np.arctan2(x, y)
    bearing_deg = np.degrees(bearing_rad)
    bearing_deg = (bearing_deg + 360) % 360  # deg
    return distance, bearing_deg


class VoyageTrip:
    def __init__(self) -> None:
        self.get_data = False
        self.origin_port = None
        self.desti_port = None
        self.vessel_arrival_desti_date = None
        self.vessel_departure_from_desti_date = None

        self.voyage_duration = None  # day
        self.stay_duration = None  # day

        self.vessel = None
        self.ballast_discharge = None

        self.voyage_avg_sog = None
        self.distance = None

    def check_data(self):
        if not None in (
            self.origin_port,
            self.desti_port,
            self.vessel,
            self.ballast_discharge,
            self.stay_duration,
            self.voyage_duration,
        ):
            self.get_data = True
            distance, _ = ll_to_sa(
                self.origin_port.port_lat,
                self.origin_port.port_lon,
                self.desti_port.port_lat,
                self.desti_port.port_lon,
            )  # m
            self.distance = distance / 1000  # km
            self.voyage_avg_sog = distance / self.voyage_duration  # km / day

    def output_to_str(self):
        origin_info = self.origin_port.output_to_str()
        desti_info = self.desti_port.output_to_str()
        vessel_info = self.vessel.output_to_str()
        output = f"{origin_info}|{desti_info}|{vessel_info}|{self.vessel_arrival_desti_date}|{self.vessel_departure_from_desti_date}|{self.distance}|{self.voyage_avg_sog}|{self.voyage_duration}|{self.stay_duration}\n"
        return output


class PortInfo:
    def __init__(self, id: int, port_name: str) -> None:
        self.port = port_name
        self.id = id
        self.country_name = None
        self.country_alpha3 = None
        self.country_id = None
        self.port_lat = -9999.9
        self.port_lon = -9999.9
        self.get_pos = False

        self.has_meow_region = None
        self.has_feow_region = None
        self.meow_neighbour = None
        self.meow_region = None
        self.meow_province = None
        self.feow_region = None
        self.feow_neighbour = None
        self.min_t = 0.0
        self.max_t = 0.0
        self.range_t = 0.0
        self.yr_mean_t = 0.0
        self.salinity = 0.0
        self.temp_src = ""
        self.sal_src = ""

    def set_geo_info(
        self,
        port_lat: tp.Optional[float],
        port_lon: tp.Optional[float],
        country_name: tp.Optional[str],
        country_alpha3: tp.Optional[str],
        country_id: tp.Optional[int],
    ) -> None:
        if port_lat is not None and port_lon is not None:
            self.port_lat = port_lat
            self.port_lon = port_lon
            self.get_pos = True
        self.country_name = country_name
        self.country_alpha3 = country_alpha3
        self.country_id = country_id

    def set_env_info(self, env_info: pd.DataFrame) -> None:
        self.min_t = float(env_info.iloc[0]["MIN_T"])
        self.max_t = float(env_info.iloc[0]["MAX_T"])
        self.range_t = float(env_info.iloc[0]["RANGE_T"])
        self.yr_mean_t = float(env_info.iloc[0]["YR_MEAN_T"])
        self.salinity = float(env_info.iloc[0]["Salinity"])
        self.temp_src = env_info.iloc[0]["Temp_Src"]
        self.sal_src = env_info.iloc[0]["Sal_Src"]
        if not pd.isna(env_info.iloc[0]["MEOW_region"]):
            neighbour = env_info.iloc[0]["MEOW_Neighbors"].split("|")
            meow_neighbour = [n for n in neighbour if n != "NA"]
            self.set_meow(
                meow_region=env_info.iloc[0]["MEOW_region"],
                meow_province=env_info.iloc[0]["MEOW_province"],
                meow_neighbour=meow_neighbour,
            )
        if not pd.isna(env_info.iloc[0]["FEOW_region"]):
            feow_neighbour = [n for n in neighbour if n != "NA"]
            self.set_feow(
                feow_region=env_info.iloc[0]["FEOW_region"],
                feow_neighbour=feow_neighbour,
            )

    def set_meow(
        self,
        meow_region: str,
        meow_province: str,
        meow_neighbour: tp.List[str],
    ) -> None:
        self.has_meow_region = True
        self.meow_region = meow_region
        self.meow_province = meow_province
        self.meow_neighbour = meow_neighbour

    def set_feow(
        self,
        feow_region: str,
        feow_neighbour: tp.List[str],
    ) -> None:
        self.has_feow_region = True
        self.feow_region = feow_region
        self.feow_neighbour = feow_neighbour

    def output_to_str(self) -> str:
        output = f"{self.port}|{self.id}|{self.port_lat}|{self.port_lon}|{self.country_alpha3}"
        return output


@attr.s
class VesselInfo:
    vessel_id = attr.ib(default=999)
    imo_number = attr.ib(default=111)
    vessel_type = attr.ib(default="OIB")
    built_year = attr.ib(default=1111)
    gross = attr.ib(default=1111.1)
    DWT = attr.ib(default=111.1)
    length = attr.ib(default=111.1)
    breadth = attr.ib(default=111.1)
    depth = attr.ib(default=111.1)
    draft = attr.ib(default=11.1)
    antifouling_factor = attr.ib(default=0.5)

    def output_to_str(self):
        output = f"{self.vessel_id}|{self.imo_number}|{self.vessel_type}|{self.DWT}"
        return output


class PortParser:
    def __init__(self, port_info_dir: str=PLACE_DIR) -> None:
        with open(port_info_dir, "r") as f:
            place_info = f.readlines()
        labels = place_info[0].strip().split("|")
        file = [place_info[i].strip().split("|") for i in range(1, len(place_info))]
        self.place_info = pd.DataFrame(file, columns=labels)
        self.place_info["PLACE ID"] = self.place_info["PLACE ID"].astype(int)
        # self.place_info['LATITUDE DECIMAL'] = self.place_info['LATITUDE DECIMAL'].astype(float)
        # self.place_info['LONGITUDE DECIMAL'] = self.place_info['LONGITUDE DECIMAL'].astype(float)
        self.env_file = pd.read_csv(GIVEN_FILE_DIR)
        self.env_file["ID"] = self.env_file["ID"].astype(int)
        self.meow_info = gpd.read_file(MEOW_DIR)
    
    def check_meow_region(self, port_id:int) -> str:
        port = self.get_port(int(port_id))
        if port is  None:
            return None
        Q = self.meow_info[self.meow_info['ECOREGION']==port.meow_region]
        if len(Q) == 1:
            return Q.iloc[0]['REALM']
        return None


    def get_port(self, data_base_id: int) -> PortInfo:
        base_info = self.place_info[self.place_info["PLACE ID"] == data_base_id]
        if len(base_info) == 0:
            return None
        if len(base_info) > 1:
            raise ValueError("two place ID check results")

        port_name = base_info.iloc[0]["PLACE NAME"]
        port = PortInfo(id=data_base_id, port_name=port_name)

        country_alpha3 = base_info.iloc[0]["COUNTRY CODE"]
        try:
            port_lat = float(base_info.iloc[0]["LATITUDE DECIMAL"])
            port_lon = float(base_info.iloc[0]["LONGITUDE DECIMAL"])
        except Exception:
            port_lat = np.nan
            port_lon = np.nan
        country_id = None
        country_name = None
        country = self.get_by_alpha3(country_alpha3)
        if country is not None:
            country_name = country.name
            country_id = country.numeric

        port.set_geo_info(
            port_lat=port_lat,
            port_lon=port_lon,
            country_name=country_name,
            country_alpha3=country_alpha3,
            country_id=country_id,
        )
        v = self.env_file[self.env_file["ID"] == data_base_id]
        if len(v) == 0:
            self.env_from_other_source()
        if len(v) > 1:
            raise ValueError("Env file two place ID")
        assert v.iloc[0]["NAME"] == port_name
        port.set_env_info(v)
        return port

    def env_from_other_source(self):
        return

    def get_by_id(self, id: int):
        F = pc.get(numeric=str(id))
        return F

    def get_by_alpha3(self, a_code: str):
        try:
            F = pc.get(alpha_3=a_code)
        except Exception:
            return None
        return F


class VesselParser:
    def __init__(self, vessel_info_dir: str) -> None:

        self.antifouling_dict = vessel_type_code_antifouling()
        with open(vessel_info_dir, "r") as f:
            vessel_info = f.readlines()
            labels = vessel_info[0].strip().split("|")
            file = [
                vessel_info[i].strip().split("|") for i in range(1, len(vessel_info))
            ]
            self.vessel_info = pd.DataFrame(file, columns=labels)
            self.vessel_info["VESSEL ID"] = self.vessel_info["VESSEL ID"].astype(int)

    def antifouling_factor(self, vessel_type: str) -> float:
        if vessel_type in self.antifouling_dict:
            return self.antifouling_dict[vessel_type]
        return 0.5

    def get_vessel(self, vessel_id: int):
        vessel_info = self.vessel_info[self.vessel_info["VESSEL ID"] == vessel_id]
        if len(vessel_info) == 0:
            return None
        if len(vessel_info) > 1:
            raise ValueError("multi vessel info")
        vessel_type = vessel_info.iloc[0]["VESSEL TYPE"]
        anti_f = self.antifouling_factor(vessel_type)
        vessel_info = VesselInfo(
            vessel_id=chg_mode(vessel_info.iloc[0]["VESSEL ID"], "int"),
            imo_number=chg_mode(vessel_info.iloc[0]["IMO"], "int"),
            vessel_type=vessel_type,
            antifouling_factor=anti_f,
            built_year=vessel_info.iloc[0]["BUILT"],
            gross=chg_mode(vessel_info.iloc[0]["GROSS"], "float"),
            DWT=chg_mode(vessel_info.iloc[0]["DWT"], "float"),
            length=chg_mode(
                vessel_info.iloc[0]["LENGTH OVERALL"],
                "float",
            ),
            breadth=chg_mode(
                vessel_info.iloc[0]["BREADTH EXTREME"],
                "float",
            ),
            depth=chg_mode(vessel_info.iloc[0]["DEPTH"], "float"),
            draft=chg_mode(vessel_info.iloc[0]["DRAFT"], "float"),
        )
        return vessel_info


def chg_mode(value, tg_type):
    try:
        if tg_type == "int":
            return int(value)
        else:
            return float(value)
    except Exception:
        return None


class RecordParser:
    def __init__(
        self,
        record_dir: str,
        vessel_info_dir: str,
        port_info_dir: str,
    ) -> None:
        self._read_init_file(record_dir)

        self.record_name = f"record-{record_dir}"
        self.vessel_parser = VesselParser(vessel_info_dir)
        self.port_parser = PortParser(port_info_dir)

    def _read_init_file(self, record_dir: str) -> None:
        with open(record_dir, "r") as f:
            sv_15 = f.readlines()
        labels = sv_15[0].strip().split("|")
        file = [sv_15[i].strip().split("|") for i in range(1, len(sv_15))]
        sv_15 = pd.DataFrame(file, columns=labels)
        self.record = sv_15

    def process_one_record(self, one_record: pd.Series) -> tp.Optional[VoyageTrip]:
        vessel_id = int(one_record["VESSEL ID"])
        place_id = int(one_record["PLACE ID"])
        arrival_date_str = one_record["ARRIVAL DATE"]
        # arrival_date = datetime.strptime(arrival_date_str, "%Y-%m-%d %H:%M:%S")
        sail_date_str = one_record["SAIL DATE"]
        # sail_date = datetime.strptime(sail_date_str, "%Y-%m-%d %H:%M:%S")

        ballast = round(float(one_record["BALLAST DISCHARGE"]), 4)
        stay_duration = float(one_record["STAY DURATION"])
        voyage_duration = float(one_record["DURATION"])

        route = one_record["ROUT"]
        if not pd.isna(route):
            route = route.split("-")
            route = [int(r) for r in route]
        else:
            return None
        assert route[1] == place_id
        vessel = self.vessel_parser.get_vessel(vessel_id)
        origin_port = self.port_parser.get_port(route[0])
        desti_port = self.port_parser.get_port(route[1])
        one_trip = VoyageTrip()
        one_trip.origin_port = origin_port
        one_trip.desti_port = desti_port
        one_trip.vessel = vessel
        one_trip.ballast_discharge = ballast
        one_trip.stay_duration = stay_duration
        one_trip.voyage_duration = voyage_duration
        one_trip.vessel_departure_from_desti_date = sail_date_str
        one_trip.vessel_arrival_desti_date = arrival_date_str
        one_trip.check_data()
        return one_trip

    def iter_rows(self):
        for (
            row_idth,
            one_record,
        ) in self.record.iterrows():

            yield row_idth, one_record
