# process origin trip file
# output trip records with NIO probability as well as error info



from pathlib import Path
from datetime import datetime

from parsers import RecordParser
from nis_probability import NIS

# data files address
DATA_DIR = Path(__file__).parent.joinpath("data")
# port information file
place_dir = DATA_DIR.joinpath("places.lst")
# vessel information file
vessel_dir = DATA_DIR.joinpath("vessels_1819.txt")
# origin record file
record_dir = DATA_DIR.joinpath('moves_cleaned_2015.txt')
# record output
TRIP_RECORD_DIR = DATA_DIR.joinpath('trip_record.txt')
# record process error
TRIP_ERROR_RECORD_DIR = DATA_DIR.joinpath('trip_record_error.txt') 

record_parser = RecordParser(record_dir = record_dir, vessel_info_dir=vessel_dir, port_info_dir=place_dir)
nis = NIS() # Nonindigenous invasion calculation

trip_record = open(TRIP_RECORD_DIR,'w')
error_record = open(TRIP_ERROR_RECORD_DIR,'w')

try:
    for row_idth, one_record in record_parser.iter_rows():
        print(row_idth)
        now = str(datetime.now())[:19]
        try: 
            one_trip = record_parser.process_one_record(one_record)
            ballast_probability, biofouling_probability = nis.calculate_by_voyage(one_trip)
            trip_info = one_trip.output_to_str()
            s = f'{trip_info.strip()}|{ballast_probability}|{biofouling_probability}\n'
            trip_record.write(s)
        except Exception as e:
            error_record.write(f'{row_idth}|{e}|{now}\n')
finally:
    error_record.close()
    trip_record.close()
    



