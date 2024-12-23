# NIS probability given VoyageTrip calculate ballast_water_probability & biofouling_probability
 
import math
import typing as tp

import numpy as np
import pandas as pd

from parsers import PortInfo, VoyageTrip


class NIS:
    def __init__(self):
        self._init_parameters()

    def _init_parameters(self):
        self.establish_alpha = 0.00015
        self.establish_temp_standard = 2
        self.establish_salinity_standard = 10
        self.ballast_lambda = 3.22 * 1e-6
        self.ballast_mu = 0.02
        self.treatment = 1

        self.biofuling_tro_1 = 1.29 * 1e-7
        self.biofuling_tro_2 = 8.316 * 1e-5
        self.biofuling_tro_3 = 0.0149

        self.biofuling_tem_1 = 1.4 * 1e-9
        self.biofuling_tem_2 = 1.6566 * 1e-5
        self.biofuling_tem_3 = 5.19 * 1e-3

        self.biofuling_gama = 0.008
        self.biofuling_tropical_lat = 0.35

    def biofouling_probability(
        self,
        origin_port_lat: float,
        anti_p: float,
        avg_sog: float,
        stay_duration: float,
    ) -> float:
        A = math.exp(-self.biofuling_gama * avg_sog)
        if abs(origin_port_lat) <= 0.35:
            prob = (
                anti_p
                * (
                    self.biofuling_tro_1 * stay_duration**3
                    - self.biofuling_tro_2 * stay_duration**2
                    + self.biofuling_tro_3 * stay_duration
                )
                * A
            )
        else:
            prob = (
                anti_p
                * (
                    self.biofuling_tem_1 * stay_duration**3
                    - self.biofuling_tem_2 * stay_duration**2
                    + self.biofuling_tem_3 * stay_duration
                )
                * A
            )

        return prob

    def ballast_water_intro_probability(
        self, ballast_water: float, voyage_duration: float, treatment: float
    ) -> float:
        p_intro = (
            treatment
            * (1 - math.exp(-self.ballast_lambda * ballast_water))
            * math.exp(-self.ballast_mu * voyage_duration)
        )
        return p_intro

    def establish_probability(self, dt, ds) -> float:
        p_estab = self.establish_alpha * math.exp(
            -0.5
            * (
                (dt / self.establish_temp_standard) ** 2
                + (ds / self.establish_salinity_standard) ** 2
            )
        )
        return p_estab

    def calculate_by_voyage(
        self, voyage_info: VoyageTrip
    ) -> tp.Optional[tp.List[float]]:

        p_indigenous = self.process_indigenous(
            voyage_info.origin_port, voyage_info.desti_port
        )
        if p_indigenous == 0:
            return [0.0,0.0]
        dt = abs(
            voyage_info.desti_port.yr_mean_t - voyage_info.origin_port.yr_mean_t
        )
        ds = abs(
            voyage_info.desti_port.salinity - voyage_info.origin_port.salinity
        )
        p_establish = self.establish_probability(dt, ds)

        p_by_ballast = self.ballast_water_intro_probability(
            voyage_info.ballast_discharge, voyage_info.voyage_duration, self.treatment
        )
        p_by_biofouling = self.biofouling_probability(
            origin_port_lat=voyage_info.origin_port.port_lat,
            anti_p=voyage_info.vessel.antifouling_factor,
            avg_sog=voyage_info.voyage_avg_sog,
            stay_duration=voyage_info.stay_duration,
        )
        p_ballast = p_indigenous * p_establish * p_by_ballast
        p_biofouling = p_indigenous * p_establish * p_by_biofouling
        return [p_ballast, p_biofouling]

    def process_indigenous(self, port_s: PortInfo, port_d: PortInfo) -> None:
        if port_s.has_meow_region and port_d.has_meow_region:
            if port_s.meow_region != port_d.meow_region:
                return 1.0
            if port_s.meow_region in port_d.meow_neighbour:
                return 0.5
            if port_d.meow_region in port_s.meow_neighbour:
                return 0.5
            return 0.0
        if port_s.has_feow_region and port_d.has_feow_region:
            if port_s.feow_region != port_d.feow_region:
                return 1.0
            if port_s.feow_region in port_d.feow_neighbour:
                return 0.5
            if port_d.feow_region in port_s.feow_neighbour:
                return 0.5
            return 0.0
        return 1.0


