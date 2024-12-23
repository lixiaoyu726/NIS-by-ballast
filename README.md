# NIS by ballast
calculate NIS probability through ballast water using voyage trip

# parsers.py
process origin file
trip record from moves_cleand_*.txt
vessel info from vessel_1819.txt
port info from places.lst

# process_origin_trips.py 
process origin file
output:
    voyage trip with NIS probability
    'o_port|o_port_id|o_port_lat|o_port_lon|o_port_country|d_port|d_port_id|d_port_lat|d_port_lon|d_port_country|vessel_id|vessel_imo|vessel_type|vessel_dwt|arrival_date|departure_date|distance|spd|voyage_duration|stay_duration|ballast_risk|biofouling_risk'
