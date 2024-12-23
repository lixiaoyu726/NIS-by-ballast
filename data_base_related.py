# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 14:19:40 2024

@author: 49701
"""
import typing as tp
import sqlite3 as sqlite
from pathlib import Path

import pandas as pd
import numpy as np
import tqdm

DATA_FILE_DIR = Path(__file__).parent.joinpath('data')

STATIC_DATA_BASE_DIR = DATA_FILE_DIR.joinpath('static.db')
MATCHED_STATIC_DATA_BASE_DIR = DATA_FILE_DIR.joinpath('matched_static.db')
DYNAMIC_DATA_BASE_DIR = DATA_FILE_DIR.joinpath('dynamic.db')
TRIP_DATA_BASE_DIR = DATA_FILE_DIR.joinpath('trip.db')
TRIP_RECORD_TEXT_DIR = DATA_FILE_DIR.joinpath('trip_record_text.txt')
TRIP_TEMP_RECORD_DIR = DATA_FILE_DIR.joinpath('trip_temp_record.txt')
TRIP_ERROR_RECORD_DIR = DATA_FILE_DIR.joinpath('trip_record_error.txt') 

I = 'INTEGER'
R = 'REAL'
T = 'TEXT'

BASIC_FEATURES = ['LLI NO','IMO', 'Name','Flag','DWT', 'LLI Vessel Type','Call Sign','Built Year',
            'Gross Tonnage','Length Overall','Length Between Perpendicular',
            'Breadth Extreme','Breadth Moulded','Depth', 'Draught', 'Freeboard',
            'TEU Capacity','Beneficial Owner','MMSI','Commercial Operator', 
            ]

TABLE_NAME_MATCHED_STATIC = 'static_info'
TABLE_NAME = 'static_info'
TABLE_NAME_DUP = 'static_info_dup'
TABLE_NAME_DYNAMIC = 'dynamic_info'
TABLE_NAME_TRIP = 'trip_info'

static_data_base_name_and_type = {'IMO': I, 'MMSI':I, 'LLI': I, 'DWT':R, 'length':R, 'width':R, 'draught':R, 'freeboard':R,
                  'depth':R, 'TEU':R, 'owner':T, 'operator':T, 'name':T, 'flag':T, 'vessel_type':T, 'call_sign':T}

matched_static_data_base_name_and_type = {'MMSI':I, 'IMO': I, 'LLI': I, 'name':T,'former_name':T, 'status': T, 'flag': T, 'built_year': I, 'call_sign': T,'vessel_type':T,
                                          'gross_tonnage':R, 'length': R, 'width': R, 'depth': R, 'draught': R, 'free_board':R, 
                                          'TEU_capacity': I,'DWT':R, 'net_weight':R,'max_speed':R,
                                          'registered_owner':T, 'technical_manager': T, 'third_party_operator':T, 'commercial_operator':T, 'registry_port':T,
                                          'engine_type':T, 'engine_model':T, 'engine_power':R, 'engine_built_by':T, 'designed_by':T, 
                                          'owner':T, 'manager':T, 'builder':T,'cs':T,}

dynamic_data_base_name_and_type = {'A':I, "B":I, "C":R, 'D':R, 'E':R, 'F':R}

trip_data_base_name_and_type = {'A':I, "timestamp":I, "lat":R, 'lon':R, 'kn':R, 'F':R,'sog':R,'cog':R,
                                'avg_kn':R,
                                'navigation_status':I,'berthing':I, 'trip_start_port':I,'trip_target_port':I,
                                'target_port':T, 'departure_port':T, 'prev_port':T, 'next_port':T,
                                'trip_id':I, 'total_trip_id':I}

def init_matched_static_data_base():
    db = sqlite.connect(MATCHED_STATIC_DATA_BASE_DIR)
    cur = db.cursor()
    s = ''
    for key, item_type in matched_static_data_base_name_and_type.items():
        s += f'{key} {item_type},'
    s = s[:-1]
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_MATCHED_STATIC} ({s})""")
    db.close()

def init_dynamic_data_base():
    db = sqlite.connect(DYNAMIC_DATA_BASE_DIR)
    cur = db.cursor()
    s = ''
    for key, item_type in dynamic_data_base_name_and_type.items():
        s += f'{key} {item_type},'
    s = s[:-1]
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_DYNAMIC} ({s})""")
    db.close()

def init_static_data_base():
    static_data_base = sqlite.connect(STATIC_DATA_BASE_DIR)
    cur = static_data_base.cursor()
    s = ''
    for key, item_type in static_data_base_name_and_type.items():
        s += f'{key} {item_type},'
    s = s[:-1]
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({s})""" )
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_DUP} ({s})""" )
    static_data_base.close()

def init_trip_data_base():
    db = sqlite.connect(TRIP_DATA_BASE_DIR)
    cur = db.cursor()
    s = ''
    for key, item_type in trip_data_base_name_and_type.items():
        s += f'{key} {item_type},'
    s = s[:-1]
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_TRIP} ({s})""")
    db.close()

    
    # cur.execute("""CREATE TABLE IF NOT EXISTS static_info (
    #                 MMSI INTEGER,
    #                 DWT REAL,
    #                 length REAL,
    #                 width REAL,
    #                 draught REAL,
    #                 freeboard REAL,
    #                 depth REAL,
    #                 TEU REAL,
    #                 owner TEXT,
    #                 operator TEXT,
    #                 name TEXT,
    #                 flag TEXT,
    #                 vessel_type TEXT,
    #                 call_sign TEXT,)
    #             """)





class DataBase:
    def __init__(self, data_base_dir: tp.Optional[str]=None):
        self.data_base_dir = None
        self.conn = None
        self.cur = None
        self.connected = False
    
    def add_item_from_df(self, df:pd.DataFrame, table:str=TABLE_NAME) -> None:
        raise NotImplementedError()
        
    def __enter__(self) -> "DataBase":
        if not self.data_base_dir:
            raise ValueError('no data base dir')
        self.conn = sqlite.connect(self.data_base_dir)
        self.cur = self.conn.cursor()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.connected = False
        self.cur = None
        self.conn.__exit__(exc_type, exc_val, exc_tb)
        self.conn = None
        
    def connect(self) -> None:
        self.conn = sqlite.connect(self.data_base_dir)
        self.connected = True
        self.cur = self.conn.cursor()
    
    def close(self) -> None:
        if self.conn:
            self.conn.close()
        self.cur = None
        self.conn = None
        self.connected = False
        
    def execute(self, *args) -> sqlite.Cursor:
        if not self.cur:
            raise ValueError('no connection to database')
        f = self.cur.execute(*args)
        return f

class StaticBase(DataBase):
    def __init__(self, data_base_dir: tp.Optional[str] = None):

        if data_base_dir:
            self.data_base_dir = data_base_dir
        else:
            self.data_base_dir = STATIC_DATA_BASE_DIR
            

    
    def add_item_from_df(self, df: pd.DataFrame, table=TABLE_NAME) -> None:
        if not self.conn:
            raise ValueError('no connection made with database')
        s = ''
        for key in static_data_base_name_and_type.keys():
            s+= f':{key},'
        s = s[:-1]
        
        with self.conn:
            for idth, row in tqdm.tqdm(df.iterrows()):
                try:
                    self.cur.execute(f"""INSERT INTO {table} VALUES ({s})""",  
                            self.draw_data(row))
                    
                except Exception:
                    print(idth, row['MMSI'])
        
    @staticmethod
    def draw_data(row):
           width = StaticBase.process_brodth(row)
           length = StaticBase.process_length(row)
           mmsi = int(row['MMSI'])
           lli = int(row['LLI NO'])
           dwt = float(row['DWT'])
           draught = float(row['Draught'])
           freeboard = float(row['Freeboard'])
           depth = float(row['Depth'])
           teu = float(row['TEU Capacity'])
           owner = row['Beneficial Owner']
           operator = row['Commercial Operator']
           name = row['Name']
           flag = row['Flag']
           call_sign = row['Call Sign']
           vessel_type = row['LLI Vessel Type']
           return {'MMSI':mmsi,'LLI':lli, 'DWT':dwt, 'length':length, 'width':width,'draught':draught,
                   'depth':depth,'freeboard':freeboard, 'TEU':teu, 'owner':owner, 'operator':operator,
                   'name':name, 'flag':flag, 'vessel_type':vessel_type, 'call_sign':call_sign
               }
    @staticmethod
    def process_brodth(line: pd.Series) -> float:
        if pd.isna(line['Breadth Extreme']) and pd.isna(line['Breadth Moulded']):
            return line['Breadth Extreme']
        if not pd.isna(line['Breadth Extreme']):
            if not pd.isna(line['Breadth Moulded']):
                return (line['Breadth Extreme'] + line['Breadth Moulded']) / 2
            else:
                return line['Breadth Extreme']
        else:
            return line['Breadth Moulded']
    
    
    @staticmethod
    def process_length(line: pd.Series) -> float:
        if pd.isna(line['Length Overall']) and pd.isna(line['Length Between Perpendicular']):
            return line['Length Overall']
        if not pd.isna(line['Length Overall']):
            if not pd.isna(line['Length Between Perpendicular']):
                return (line['Length Overall'] + line['Length Between Perpendicular']) / 2
            else:
                return line['Length Overall']
        else:
            return line['Length Between Perpendicular']     
    


class MatchedStaticBase(DataBase):
    def __init__(self, data_base_dir: tp.Optional[str] = None):

        if data_base_dir:
            self.data_base_dir = data_base_dir
        else:
            self.data_base_dir = MATCHED_STATIC_DATA_BASE_DIR

        self.data_base_name_and_type = matched_static_data_base_name_and_type    
        s  = ''
        for key in self.data_base_name_and_type.keys():
            s+= f':{key},'
        s = s[:-1]
        self.insert_s = s
        self.table_name = TABLE_NAME_MATCHED_STATIC
    
    def select_by_imo(self, imo:tp.Union[float, list[int]]) -> pd.DataFrame:
        if not isinstance(imo, list):
            imo = [imo]
        query = f"""
                SELECT *
                FROM {self.table_name}
                WHERE IMO IN ({','.join('?' for _ in imo)})
                """
        with self:
            f = self.cur.execute(query, imo)
            sv = f.fetchall()
        return self.put_data(sv)

    def add_item_from_df(self, df: pd.DataFrame, table=TABLE_NAME_MATCHED_STATIC) -> None:
        with self:
            for idth, row in tqdm.tqdm(df.iterrows()):
                try:
                    self.cur.execute(f"""INSERT INTO {table} VALUES ({self.insert_s})""",  
                            self.draw_data(row))
                except Exception as e:
                    print(e)
                    print(idth)
    def add_one_row(self, row: pd.Series, table=TABLE_NAME_MATCHED_STATIC) -> None:
        with self:
            self.cur.execute(f"""INSERT INTO {table} VALUES ({self.insert_s})""",  self.draw_data(row))

        
    @staticmethod
    def draw_data(row):
           imo = int(row['IMO'])
           mmsi = row['MMSI_x']
           lli = int(row['LLI_NO'])
           name = row['Name']
           former_name = row['Former names']       
           status = row['Status']
           flag = row['Flag_x']
           built_year = row['Built Year']
           call_sign = row['Call Sign']
           vessel_type = row['LLI Vessel Type']

           gross_tonnage = row['Gross Tonnage']
           width = MatchedStaticBase.process_brodth(row)
           length = MatchedStaticBase.process_length(row)
           depth = float(row['Depth'])
           draught = float(row['Draught'])
           freeboard = float(row['Freeboard'])
           teu = float(row['TEU Capacity'])
           dwt = MatchedStaticBase.process_dwt(row)           
           net_weight = row['Net Weight Tonnage']
           max_speed = row['SpeedType']

           registered_owner= row['Registered Owner']
           technical_manager= row['Technical Manager'] 
           third_po= row['Third Party Operator']
           commercial_operator= row['Commercial Operator']
           registry_port =row['Port Of Registry']

           engine_type =row['Engine type']
           engine_model =row['Engine model']
           engine_power =row['Engine power']
           engine_built_by =row['Engine Built By']
           designed_by =row['Designed By'] 

           owner = row['Owner']
           manager = row['Manager']
           builder = row['Builder']
           cs = row['Classification society']
          
           
           return {'IMO':imo, 'MMSI':mmsi,'LLI':lli, 'name':name, 'former_name':former_name, 'status':status, 'flag':flag, 
                   'built_year':built_year, 'call_sign':call_sign, 'vessel_type':vessel_type,
                   'gross_tonnage':gross_tonnage, 'length':length, 'width':width,'depth':depth,  'draught':draught, 'free_board':freeboard,
                   'TEU_capacity':teu, 'DWT':dwt, 'net_weight':net_weight, 'max_speed':max_speed,
                   'registered_owner':registered_owner, 'technical_manager': technical_manager, 'third_party_operator':third_po, 'commercial_operator':commercial_operator, 'registry_port':registry_port,
                   'engine_type':engine_type, 'engine_model':engine_model, 'engine_power':engine_power, 'engine_built_by':engine_built_by, 'designed_by':designed_by, 
                   'owner':owner,  'manager':manager, 'builder':builder,'cs':cs,    
               }

    @staticmethod
    def process_brodth(line: pd.Series) -> float:
        if pd.isna(line['Breadth Extreme']) and pd.isna(line['Breadth']):
            return line['Breadth']
        if not pd.isna(line['Breadth Extreme']):
            return line['Breadth Extreme']
        else:
            return line['Breadth']
            
    @staticmethod
    def process_length(line: pd.Series) -> float:
        if pd.isna(line[ "Length Overall"]) and pd.isna(line['Length']):
            return line['Length']
        if not pd.isna(line['Length Overall']):

            return line['Length Overall']
        else:
            return line['Length']
    @staticmethod
    def process_dwt(line: pd.Series) -> float:
        if not pd.isna(line['DWT']):
            return line['DWT']
        if not pd.isna(line['Deadweight']):
            return line['Deadweight']
        return line['DWT']
    
    @staticmethod
    def put_data(output:tp.List[tp.List]) -> pd.DataFrame:
        df = pd.DataFrame(output, columns= matched_static_data_base_name_and_type.keys())
        return df
    


class FirstDynamicBase(DataBase):
    def __init__(self, data_base_dir: tp.Optional[str]=DYNAMIC_DATA_BASE_DIR) -> None:
        if data_base_dir:
            self.data_base_dir = data_base_dir
        else:
            self.data_base_dir = DYNAMIC_DATA_BASE_DIR
    
    def add_item(self, file_content:tp.List[tp.List[float]]) -> None:
        if not self.conn:
            raise ValueError('no connection made with database')
        s = ''
        for key in dynamic_data_base_name_and_type.keys():
            s+= f':{key},'
        s = s[:-1]       
        with self.conn:
            for  row in tqdm.tqdm(file_content):
                self.cur.execute(f"""INSERT INTO {TABLE_NAME_DYNAMIC} VALUES ({s})""",  
                        self.draw_data(row))
                
    @staticmethod
    def draw_data(row):
           return {'A':row[0],'B':row[1], 'C':row[2], 'D':row[3],'E':row[4], 'F':row[5]}
         
        

class TripDataBase(DataBase):
    def __init__(self, data_base_dir=TRIP_DATA_BASE_DIR) -> None:
        if data_base_dir:
            self.data_base_dir = data_base_dir
        else:
            self.data_base_dir = TRIP_DATA_BASE_DIR
        self.table_name = TABLE_NAME_TRIP
    
    def add_item(self, df:pd.DataFrame) -> None:
        if not self.conn:
            raise ValueError('no connection made with database')
        s = ''
        for key in trip_data_base_name_and_type.keys():
            s+= f':{key},'
        s = s[:-1]
        with self.conn:
            for _, row in df.iterrows():
                self.cur.execute(f"""INSERT INTO {TABLE_NAME_TRIP} VALUES ({s})""",  
                        self.draw_data(row))
        
    def select_by_port(self, port_name:str, departure:tp.Optional[bool]=True, target:tp.Optional[bool]=True) -> pd.DataFrame:
        output = []
        with self:
            if departure:
                f = self.execute( f"""SELECT * FROM {self.table_name} WHERE departure_port = ?""", (port_name, ))
                sv = f.fetchall()
                output.extend(sv)
            if target:
                f = self.execute(f""" SELECT * FROM {self.table_name} WHERE target_port = ?""", (port_name, ))
                sv = f.fetchall()
                output.extend(sv)
            return self.put_data(output)
    
    def select_by_id(self, trip_id:tp.Union[int, tp.List[int]], coords_only = False) -> pd.DataFrame:
        if isinstance(trip_id, int):
            trip_id = [trip_id]
        with self:
            if coords_only:
                query = f"""
                SELECT lat, lon
                FROM {self.table_name}
                WHERE total_count IN ({','.join('?' for _ in trip_id)})
                """
                f = self.execute(query, trip_id)
                sv = f.fetchall()
                return pd.DataFrame(sv, columns=['lat', 'lon'])

            else:
                query = f"""
                SELECT *
                FROM {self.table_name}
                WHERE total_count IN ({','.join('?' for _ in trip_id)})
                """
                f = self.execute(query, trip_id)
                sv = f.fetchall()
                return self.put_data(sv)

    def select_by_enquery(self, enquery:str, items:tp.Sequence[tp.Any]) -> pd.DataFrame:
        with self:
            f = self.execute(enquery, items)
            sv = f.fetchall()
        return self.put_data(sv)
    
    @staticmethod
    def put_data(output:tp.List[tp.List]) -> pd.DataFrame:
        df = pd.DataFrame(output, columns= trip_data_base_name_and_type.keys())
        return df
    
    @staticmethod
    def draw_data(row):
           pp_dict = dict()
           for name in trip_data_base_name_and_type.keys():
               pp_dict[name] = row[name]
           return pp_dict
        