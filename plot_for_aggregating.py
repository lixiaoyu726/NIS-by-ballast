# Demo file for ilustrating aggregated risk

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plot
from matplotlib.patches import Patch
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from aggregate_risk import AggregateRisk

realm_dict = {'Arctic':'red',
     'Central Indo-Pacific':'blue',
     'Eastern Indo-Pacific':'green',
     'Southern Ocean':'orange',
     'Temperate Australasia':'purple',
     'Temperate Northern Atlantic':'cyan',
     'Temperate Northern Pacific':'magenta',
     'Temperate South America':'yellow',
     'Temperate Southern Africa':'brown',
     'Tropical Atlantic':'pink',
     'Tropical Eastern Pacific':'gray',
     'Western Indo-Pacific':'lime'}

realm_list = list(realm_dict.keys())
realm_color_list = [realm_dict[key] for key in realm_list]

agg_class = AggregateRisk()
# In[]
# Demo for top 10 risk ports
port_list = list(agg_class.record["d_port"].value_counts()[:10].index)
def port_list_agg(port_list=port_list):
    country_agg = dict()
    realm_agg = dict()
    
    for port in port_list:
        bio_r_d, ballast_r_d = agg_class.aggregate_by_country(port)
        country_agg[port] = dict()
        country_agg[port]['ballast'] = ballast_r_d
        country_agg[port]['biofouling'] = bio_r_d
        bio_r_d, ballast_r_d = agg_class.aggregate_by_realm(port)
        realm_agg[port] = dict()
        realm_agg[port]['ballast'] = ballast_r_d
        realm_agg[port]['biofouling'] = bio_r_d
    return country_agg, realm_agg
    

# In[]
# aggregate by country
def plot_one_port_agg_by_country(d_port='Busan', risk_type='ballast', top_number=4):
    bio_r_d, ballast_r_d = agg_class.aggregate_by_country(d_port)
    if risk_type =='ballast':
        record = pd.DataFrame(list(ballast_r_d.items()), columns=['country', 'risk'])
    else:
        record = pd.DataFrame(list(bio_r_d.items()), columns=['country', 'risk'])
        
    record = record.sort_values(by='risk', ascending=False)
    record.reset_index(drop=True, inplace=True)
    record['risk'] = record['risk']  / record['risk'].sum() * 100
    risk_list = list(record['risk'].values[:top_number])
    country_list = list(record['country'].values[:top_number])
    others_risk = record.iloc[top_number+1:]['risk'].sum()
    risk_list.append(others_risk)
    country_list.append('OTHERS')
    explode = [0] * len(risk_list)
    explode[0] = .1
    fig, ax = plot.subplots()
    ax.pie(risk_list,explode = explode,labels=country_list, shadow=True, autopct='%1.1f%%',startangle=90,counterclock=False)
    plot.legend(country_list, loc="best")
    ax.set_title(f'Aggregate risk by {risk_type} for port {d_port}')
    plot.show()

def plot_ports_agg_by_country(port_list=port_list, risk_type='ballast', top_number=5):
    fig = plot.figure(figsize=(15, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent([-20, 180, -20, 70], crs=ccrs.PlateCarree())
    ax.stock_img()  
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    for port in port_list:
        bio_r_d, ballast_r_d = agg_class.aggregate_by_country(port)
        one_record =  agg_class.record[agg_class.record['d_port']==port].iloc[0]
        port_lat = float(one_record['d_port_lat'])
        port_lon = float(one_record['d_port_lon'])
        if risk_type =='ballast':
            record = pd.DataFrame(list(ballast_r_d.items()), columns=['country', 'risk'])
        else:
            record = pd.DataFrame(list(bio_r_d.items()), columns=['country', 'risk'])
        record = record.sort_values(by='risk', ascending=False)
        record.reset_index(drop=True, inplace=True)
        record['risk'] = record['risk']  / record['risk'].sum() * 100
        risk_list = list(record['risk'].values[:top_number])
        country_list = list(record['country'].values[:top_number])
        others_risk = record.iloc[top_number+1:]['risk'].sum()
        risk_list.append(others_risk)
        country_list.append('OTHERS')
        explode = [0] * len(risk_list)
        explode[0] = .1
        inset_ax = add_pie(ax, port_lon+7, port_lat+7,size=2)
        inset_ax.pie(risk_list,explode = explode,labels=country_list, shadow=True, autopct='%1.1f%%',startangle=90,counterclock=False)
        ax.plot(port_lon, port_lat, 'ro', transform=ccrs.PlateCarree())
        ax.text(port_lon, port_lat, port, fontsize=12, ha='right', transform=ccrs.PlateCarree()) 
    ax.set_title('Aggregate risk by country through ballast water',fontsize=26)
    plot.show()     
         
def add_pie(ax, center_lon, center_lat, size=0.4):
    inset_ax = inset_axes(
        ax,
        width=size,
        height=size,
        loc="center",
        bbox_to_anchor=(center_lon, center_lat),
        bbox_transform=ax.transData,
        borderpad=0,
    )
    inset_ax.set_aspect("equal")
    return inset_ax



def plot_ports_agg_by_realm(port_list=port_list):
    # plot risk for port in port_list
    # todo: only plot biofouling risk by now
    realm_agg = dict()
    for port in port_list:
        ballast_r_d, bio_r_d = agg_class.aggregate_by_realm(port)
        realm_agg[port] = {"biofouling": bio_r_d, "ballast": ballast_r_d}
        one_record =  agg_class.record[agg_class.record['d_port']==port].iloc[0]
        port_lat = float(one_record['d_port_lat'])
        port_lon = float(one_record['d_port_lon'])
        realm_agg[port]['port_lat'] = port_lat
        realm_agg[port]['port_lon'] = port_lon

    realm_agg = add_pie_size(realm_agg)
    # plot agg risk by realm 
    fig = plot.figure(figsize=(15, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent([-40, 180, -20, 90], crs=ccrs.PlateCarree())
    ax.stock_img()  
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    for port, agg_info in realm_agg.items():
        lon = agg_info['port_lon']
        lat =  agg_info['port_lat']
        pie_sizes = agg_info['ballast_pie_size']
        pie_colors= realm_color_list
        inset_ax = add_pie(ax, (lon+5), (lat+5), size=1.75)
        inset_ax.pie(pie_sizes, colors=pie_colors)
        ax.plot(lon, lat, 'ro', transform=ccrs.PlateCarree())
        ax.text(lon, lat, port, fontsize=12, ha='right', transform=ccrs.PlateCarree())
        
        
    legend_handles = [
        Patch(facecolor=color, label=label) for label, color in zip(realm_list, realm_color_list)
    ]

    ax.legend(handles=legend_handles, loc="upper left", fontsize=10, title_fontsize=12)

    ax.set_title('Aggregate risk by eco-realm through ballast water', fontsize=26)
    plot.show()

def add_pie_size(plot_dict):
    for key, port_value in plot_dict.items():
        bio_pie_size = []
        ballast_pie_size= []
        total = 0.0
        for bio_value in port_value['biofouling'].values():
            total += bio_value
        if total != 0.0:
            for key in realm_dict.keys():
                bio_pie_size.append(port_value['biofouling'][key]/total * 100)
        port_value['bio_pie_size'] = bio_pie_size
        total = 0.0
        for ballast_value in port_value['ballast'].values():
            total += ballast_value
        if total != 0.0:
            for key in realm_list:
                ballast_pie_size.append(port_value['ballast'][key]/total * 100)
        port_value['ballast_pie_size']= ballast_pie_size
    return plot_dict





