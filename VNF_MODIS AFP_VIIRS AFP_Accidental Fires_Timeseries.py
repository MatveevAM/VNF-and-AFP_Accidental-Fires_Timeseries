# Imports
import collections
from math import *
# from multiprocessing.pool import ThreadPool as Thread
from multiprocessing import Pool, Process, Manager, Value, Event
import multiprocessing
from time import *
import datetime
from datetime import timedelta
import os
# import glob
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
from matplotlib import colors
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)
from matplotlib.ticker import FormatStrFormatter
# from matplotlib.pyplot import text
from scipy import stats
from scipy.stats import t
import atexit
import statistics
from shapely import wkt
from shapely.geometry import Point, Polygon
import functools as ft # to merger 3 dfs
# import pyproj

start_time = time()

# Files
cwd = os.getcwd() # current py directory
path_save = 'Results/'
path_save_source = path_save + 'Source tables/'
path_save_scatters = path_save + 'Scatters/'
path_save_dashboard = path_save + 'Dashboards/'
# path_save_nrt_dashboard = path_save_dashboard + 'NRT/'
# path_save_field_table = path_save_dashboard + 'Field Tables/'
# path_save_field_daily_plots = path_save_dashboard + 'Field Daily Mean Graphs/'
if os.path.exists(path_save) == False: os.makedirs(path_save)
if os.path.exists(path_save_source) == False: os.makedirs(path_save_source)
if os.path.exists(path_save + 'Daily summed datasets') == False: os.makedirs(path_save + 'Daily summed datasets')
if os.path.exists(path_save_dashboard) == False: os.makedirs(path_save_dashboard)
# if os.path.exists(path_save_nrt_dashboard) == False: os.makedirs(path_save_nrt_dashboard)
# if os.path.exists(path_save_field_table) == False: os.makedirs(path_save_field_table)
# if os.path.exists(path_save_field_daily_plots) == False: os.makedirs(path_save_field_daily_plots)

source_file = 'Combined/VNF and AFP_Actual Flares_2022.xlsx'
datasets = ['VNF', 'AFP']
datasets_sources = ['VNF', 'MODIS AFP', 'VIIRS AFP']
datasets_updated = ['VNF', 'VNF_pct', 'AFP']
processed_datasets_names = ['VNF', 'VNF_pct', 'MODIS AFP', 'VIIRS AFP']
datasets_extracted = ['MODIS AFP', 'VIIRS AFP', 'VNF', 'VNF_pct']
VNF_datasets = ['VNF', 'VNF_pct']
scatter_datasets = [ # VNF vs VAFP
                    ['VNF_max_daily', 'VAFP_max_daily'], ['VNF_max_overpass', 'VAFP_max_overpass'], 
                    ['VNF_PctRH_75_daily', 'VAFP_PctFRP_75_daily'], ['VNF_PctRH_75_overpass', 'VAFP_PctFRP_75_overpass'], 
                    ['VNF_SumRH_daily', 'VAFP_SumFRP_daily'], ['VNF_SumRH_overpass', 'VAFP_SumFRP_overpass'],
                    # bcm
                    ['VNF_PctRH_75_bcm_overpass', 'VAFP_PctFRP_75_overpass'], ['VNF_max_bcm_overpass', 'VAFP_max_overpass'],
                    ['VNF_PctRH_75_bcm_daily', 'MAFP_PctFRP_75_daily'], ['VNF_max_bcm_daily', 'MAFP_max_daily'],
                    # Day vs night
                    ['VAFP_night', 'VAFP_day'], ['MAFP_night', 'MAFP_day'],
                    # VNF vs MODIS AFP
                    ['VNF_max_daily', 'MAFP_max_daily'], 
                    # VIIRS vs MODIS AFP
                    ['VAFP_max_daily', 'MAFP_max_daily'], ['VAFP_max_overpass', 'MAFP_max_overpass'],
                    # VNF vs MODIS AFP, more
                    ['VNF_PctRH_75_daily', 'MAFP_PctFRP_75_daily'], ['VNF_SumRH_daily', 'MAFP_SumFRP_daily'],
                    # VIIRS vs MODIS AFP, more
                    ['VAFP_PctFRP_75_daily', 'MAFP_PctFRP_75_daily'], ['VAFP_SumRH_daily', 'MAFP_SumRH_daily']
                   ]

# Dicts
groupby_dict = collections.defaultdict(dict)
dateonly_dict = collections.defaultdict(dict)
daynight_dict = collections.defaultdict(dict)
satellite_dict = collections.defaultdict(dict)
bowtie_dict = collections.defaultdict(dict)
separate_graph_dict = collections.defaultdict(dict)

# Keys
groupby_key = ['datetime', 'group_columns', 'rename_columns', 'sort_columns', 'reindex_columns']
dateonly_key = ['group_mean_dateonly', 'group_mean_dateonly_columns', 'group_mean_dateonly_reindex']
daynight_key = ['group_mean_daynight', 'group_mean_daynight_columns', 'group_mean_daynight_reindex']
satellite_key = ['group_mean_satellite', 'group_mean_satellite_columns', 'group_mean_satellite_reindex']
bowtie_key = ['groupby', 'group_columns', 'reindex_columns']
separate_graph_key = ['X_column', 'Y_column', 'Title', 'Xlabel', 'Ylabel', 'Vs']

# Lists
groupby_list  = [ # VNF
            [['Date_Mscan', 'Field'], # datetime
             {'RHI' : ['sum', 'count'], 'RH' : 'sum', 'Temp_BB' : 'mean', 'Area_BB': 'sum', 'Cloud_Mask' : 'first', 
                            'Satellite' : 'first', 'Field' : 'first', 'Lat_GMTCO' : 'first', 'Lon_GMTCO': 'first'}, # group_columns
             {'count' : 'Ndtct_calc', 'Date_Mscan' : 'Date', 'Lat_GMTCO' : 'Lat', 'Lon_GMTCO' : 'Lon'}, # rename_columns
             ['Field', 'Date'], # sort_columns
             ['Lat', 'Lon', 'Field', 'Date', 'SumRHI_calc', 'SumRH_calc', 'Satellite', 'Ndtct_calc', 'Temp_BB', 'Area_BB', 'Cloud_Mask', 'ClusterID'] # reindex_columns
             ],
            # VNF_pct
            [['date', 'Field'], 
             {'Field' : 'first', 'Ndtct' : 'count', 'Npct_75' : 'first', 'PctRH_75' : 'first', 'Npct_50' : 'first', # datetime
                            'PctRH_50' : 'first', 'Npct_25' : 'first', 'PctRH_25' : 'first', 'Nfit' : 'first', 'SumRH' : 'first', 'MaxRH' : 'first', # group_columns
                            'Tmin' : 'first', 'Tavg' : 'first', 'Tmax' : 'first', 'Temp_BB' : 'first', 'Area_BB' : 'first', 'Lat' : 'first', 'Lon' : 'first',
                            'SATZ' : 'first', 'Flowrate_km3_per_day': 'first'},
             {'sum' : 'Ndtct_samescan', 'date' : 'Date'}, # rename_columns
             ['Field', 'Date'], # sort_columns
             ['Lat', 'Lon', 'Field', 'Date', 'SumRH', 'MaxRH', 'Ndtct', 'Npct_75', 'PctRH_75', 'Npct_50', 'PctRH_50', 
              'Npct_25', 'PctRH_25', 'Nfit', 'Tmin', 'Tavg', 'Tmax', 'Temp_BB', 'Area_BB', 'SATZ', 'Flowrate_km3_per_day'] # reindex_columns
             ],
            # AFP
            [['acq_date', 'scan', 'track', 'Field', 'satellite'], # datetime
             {'frp' : ['sum', 'count'], 'MaxFRP' : 'last', 'PctFRP_75' : 'last', 'PctFRP_50' : 'last', 'PctFRP_25' : 'last', 
              'Npct_75' : 'last', 'Npct_50' : 'last', 'Npct_25' : 'last', 'latitude' : 'first', 'longitude' : 'first', 
              'acq_time' : 'first', 'satellite' : 'first',  'instrument' : 'first', 'daynight' : 'first', 'Field' : 'first'}, # group_columns
             {'count' : 'Ndtct_samescan', 'satellite' : 'Satellite', 'acq_date': 'Date', 'latitude' : 'Lat', 'longitude' : 'Lon', 'acq_time' : 'Local Time'}, # rename_columns
             ['Field', 'Date', 'Local Time', 'Satellite'], # sort_columns
             ['Lat', 'Lon', 'Field', 'Date', 'Local Time', 'daynight', 'instrument', 'SumFRP', 'MaxFRP',
              'PctFRP_75', 'PctFRP_50', 'PctFRP_25', 'Npct_75', 'Npct_50', 'Npct_25', 'Ndtct_samescan', 'Satellite', 'scan', 'track'] # reindex_column           
                ]
                ]

dateonly_list = [ # VNF
                  [['Field', 'Date Day'], {'SumRHI_calc': ['sum', 'mean'], 'SumRH_calc': ['sum', 'mean'], 
                                       'Ndtct_calc' : ['count', 'sum'], 'Temp_BB': 'mean', 'Area_BB': 'mean',
                                       'Lat' : 'first', 'Lon' : 'first', 'Field' : 'first', 'Date Day': 'first'}, # group_mean_dateonly
                  ['index_date_sum', 'Lat', 'Lon', 'Field', 'Date', 'Ndtct_scans', 'Ndtct_all', 'Daily_Mean_RHI', 'Daily_Sum_RHI', 'Daily_Mean_RH', 'Daily_Sum_RH', 'Mean_Temp_BB', 'Mean_Area_BB']], # group_mean_dateonly_reindex
                  # VNF_pct
                  [['Field', 'Date Day'], {'SumRH' : ['sum', 'mean'], 'MaxRH' : 'max', 'Ndtct' : ['count', 'sum'], 
                                           'Npct_75': 'sum', 'Npct_50': 'sum', 'Npct_25': 'sum', 'Nfit': 'sum',
                                           'PctRH_75': 'mean', 'PctRH_50': 'mean', 'PctRH_25': 'mean', 
                                           'bcm_MaxRH_annual_eq': 'max', 'bcm_PctRH_75_annual_eq': 'mean',
                                           'Temp_BB': 'mean', 'Area_BB': 'mean', 'Lat' : 'first', 'Lon' : 'first'}, # group_mean_dateonly
                   ['index_date_sum', 'Lat', 'Lon', 'Field', 'Date', 'Ndtct_scans', 'Ndtct_all', 'Daily_Mean_SumRH', 'Daily_SumRH', 'Daily_MaxRH', # group_mean_dateonly_reindex
                    'DM_PctRH_75', 'DM_PctRH_50', 'DM_PctRH_25', 'DM_Npct_75', 'DM_Npct_50', 'DM_Npct_25', 'DM_Nfit', 'Mean_Temp_BB', 'Mean_Area_BB',
                    'DM_MaxRH_bcm', 'DM_PctRH_75_bcm']],
                  # AFP
                  [['Field', 'Date'], {'SumFRP' : ['sum', 'mean'], 'Ndtct_samescan' : ['count', 'sum'], 'MaxFRP': 'max', 'Npct_75': 'sum', 'Npct_50': 'sum', 'Npct_25': 'sum',
                  'PctFRP_75': 'mean', 'PctFRP_50': 'mean', 'PctFRP_25': 'mean', 'Lat' : 'first', 'Lon' : 'first', # group_mean_dateonly,  
                  'Field' : 'first'}, 
                   ['index_date_sum', 'Lat', 'Lon', 'Field', 'Date', 'Ndtct_scans', 'Ndtct_all', 'Daily_Mean_SumFRP', 'Daily_SumFRP', 'Daily_MaxFRP', 'DM_PctFRP_75', 'DM_PctFRP_50', 'DM_PctFRP_25', 'DM_Npct_75', 'DM_Npct_50', 'DM_Npct_25']]
                ]
daynight_list = [ # VNF
                 ['', '', ''],    
                  # AFP
                 [['Field', 'Date', 'daynight'], {'SumFRP' : ['sum', 'mean'], 'Ndtct_samescan' : ['count', 'sum'], 'MaxFRP': 'max', 'Npct_75': 'sum', 'Npct_50': 'sum', 'Npct_25': 'sum',
                 'PctFRP_75': 'mean', 'PctFRP_50': 'mean', 'PctFRP_25': 'mean', 'Lat' : 'first', 'Lon' : 'first', # group_mean_dateonly,  
                 'Field' : 'first', 'daynight' : 'first'}, ['index_date_sum', 'Lat', 'Lon', 'Field', 'Date', 'daynight', 'Ndtct_scans', 'Ndtct_all', 'Daily_Mean_SumFRP', 'Daily_SumFRP', 'Daily_MaxFRP', 'DM_PctFRP_75', 'DM_PctFRP_50', 'DM_PctFRP_25', 'DM_Npct_75', 'DM_Npct_50', 'DM_Npct_25']]
                ]
satellite_list = [ # VNF
                 ['', '', ''],    
                  # AFP
                 [['Field', 'Date', 'Satellite'], {'SumFRP' : ['sum', 'mean'], 'Ndtct_samescan' : ['count', 'sum'], 'MaxFRP': 'max', 'Npct_75': 'sum', 'Npct_50': 'sum', 'Npct_25': 'sum',
                 'PctFRP_75': 'mean', 'PctFRP_50': 'mean', 'PctFRP_25': 'mean', 'Lat' : 'first', 'Lon' : 'first', # group_mean_dateonly,  
                 'Field' : 'first', 'Satellite' : 'first'}, ['index_date_sum', 'Lat', 'Lon', 'Field', 'Date', 'Satellite', 'Ndtct_scans', 'Ndtct_all', 'Daily_Mean_SumFRP', 'Daily_SumFRP', 'Daily_MaxFRP', 'DM_PctFRP_75', 'DM_PctFRP_50', 'DM_PctFRP_25', 'DM_Npct_75', 'DM_Npct_50', 'DM_Npct_25']]
                 ]

bowtie_list =    [ # VNF
                  [['Field', 'Samescan'], # groupby
                   {'Date': 'first', 'SumRHI_calc' : ['max', 'count'], 'Ndtct_calc': 'last', 'SumRH_calc' : 'max',
                    'Temp_BB' : 'last', 'Area_BB': 'last', 'Cloud_Mask' : 'first', 'Satellite' : 'first',
                    'Field' : 'first', 'Lat' : 'last', 'Lon': 'last', 'Date Day': 'first', 'Local Time': 'first'}, # group_columns
                    ['Lat', 'Lon', 'Field', 'Date', 'SumRHI_calc', 'SumRH_calc', 'N_Bowtie_Scans', 
                     'Temp_BB', 'Area_BB', 'Satellite', 'Ndtct_calc', 'Temp_BB', 'Area_BB', 'Cloud_Mask', 'Date Day', 'Local Time'] # reindex_columns
                  ],    
                  # VNF_pct
                  [
                   ['Field', 'Samescan'], # groupby
                   {'Date': 'first', 'SumRH' : ['max', 'count'], 'MaxRH': 'last', 'Ndtct': 'last', 'Nfit': 'last',
                    'Npct_75': 'last', 'Npct_50': 'last', 'Npct_25': 'last', 'PctRH_75': 'last', 'PctRH_50': 'last', 'PctRH_25': 'last', 
                    'Temp_BB' : 'last', 'Area_BB': 'last', 'SATZ' : 'first', 'Flowrate_km3_per_day': 'last', 'Field' : 'first',
                    'Lat' : 'last', 'Lon': 'last', 'Date Day': 'first', 'Local Time': 'first'}, # group_columns
                    ['Lat', 'Lon', 'Field', 'Date', 'SumRH', 'N_Bowtie_Scans', 'Ndtct', 'Npct_75', 'PctRH_75', 'Npct_50', 'PctRH_50', 'Npct_25', 'PctRH_25', 'Nfit', 
                                   'MaxRH', 'Temp_BB', 'Area_BB', 'SATZ', 'Flowrate_km3_per_day', 'Date Day', 'Local Time'] # reindex_columns    # reindex_columns 
                   ]
                  ]

separate_graph_list = [# X_column, Y_column, Title, Xlabel, Ylabel
                       # ['VNF_max_daily', 'VAFP_max_daily'], 
                       ['Daily_MaxRH', 'Daily_MaxFRP', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Daily}$ $\bf{max}$ Radiant Heat", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: max, daily'],
                       # ['VNF_max_overpass', 'VAFP_max_overpass'], 
                       ['MaxRH', 'MaxFRP', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Overpass}$ $\bf{max}$ Radiant Heat", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: max, overpass'],
                       # ['VNF_PctRH_75_daily', 'VAFP_PctFRP_75_daily'],
                       ['DM_PctRH_75', 'DM_PctFRP_75', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Daily}$ $\bf{sum}$ of $\bf{>75}$% FRP/RH from max Radiant Heat", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: 75% sum, daily'],
                       # ['VNF_PctRH_75_overpass', 'VAFP_PctFRP_75_overpass'],
                       ['PctRH_75', 'PctFRP_75', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Overpass}$ $\bf{sum}$ of $\bf{>75}$% FRP/RH from max Radiant Heat", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: 75% sum, overpass'],
                       # ['VNF_SumRH_daily', 'VAFP_SumFRP_daily']
                       ['Daily_Mean_SumRH', 'Daily_Mean_SumFRP', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Daily}$ $\bf{mean}$ $\bf{sum}$ of FRP/RH per overpass", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: sum, daily'],
                       # ['VNF_SumRH_overpass', 'VAFP_SumFRP_overpass'],
                       ['SumRH', 'SumFRP', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Overpass}$ $\bf{sum}$ of FRP/RH", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: sum, overpass'],
                       # ['VNF_PctRH_75_bcm_overpass', 'VAFP_PctFRP_75_overpass'],
                       ['bcm_PctRH_75_annual_eq', 'PctFRP_75', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Overpass}$ $\bf{sum}$ of $\bf{>75}$% FRP vs bcm", 'VIIRS Nightfire Radiant Heat volume proxy, $\it{bcm}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: 75% sum, overpass'],
                       # ['VNF_max_bcm_overpass', 'VAFP_max_overpass'],
                       ['bcm_MaxRH_annual_eq', 'MaxFRP', 'VIIRS Nightfire vs VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Overpass}$ $\bf{max}$ FRP vs bcm", 'VIIRS Nightfire Radiant Heat volume proxy, $\it{bcm}$', 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs VAFP: max, overpass'],
                       # ['VNF_PctRH_75_bcm_overpass', 'MAFP_PctFRP_75_daily'],
                       ['DM_PctRH_75_bcm', 'DM_PctFRP_75', 'VIIRS Nightfire vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Overpass}$ $\bf{sum}$ of $\bf{>75}$% FRP vs bcm", 'VIIRS Nightfire Radiant Heat volume proxy, $\it{bcm}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs MAFP: 75% sum, daily'],
                       # ['VNF_max_bcm_overpass', 'MAFP_max_daily'],
                       ['DM_MaxRH_bcm', 'Daily_MaxFRP', 'VIIRS Nightfire vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Overpass}$ $\bf{max}$ FRP vs bcm", 'VIIRS Nightfire Radiant Heat volume proxy, $\it{bcm}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs MAFP: max, daily'],
                       # ['VAFP_night', 'VAFP_day'],
                       ['Daily_MaxFRP_X', 'Daily_MaxFRP_Y', 'VIIRS Applications Related Active Fire Product C2\n' + r"$\bf{Night}$ vs $\bf{day}$ max FRP", 'Nighttime Fire Radiative Power, MW', 'Daytime Fire Radiative Power, $\it{MW}$', 'Day vs night: VAFP'],
                       # ['MAFP_night', 'MAFP_day'],
                       ['Daily_MaxFRP_X', 'Daily_MaxFRP_Y', 'MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Night}$ vs $\bf{day}$ max FRP", 'Nighttime Fire Radiative Power, MW', 'Daytime Fire Radiative Power, $\it{MW}$', 'Day vs night: MAFP'],
                       # ['VNF_max_daily', 'MAFP_max_daily'],
                       ['Daily_MaxRH', 'Daily_MaxFRP', 'VIIRS Nightfire vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ $\bf{max}$ RH/FRP", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs MAFP: max, daily'],
                       # ['VAFP_max_daily', 'MAFP_max_daily'], 
                       ['Daily_MaxFRP_X', 'Daily_MaxFRP_Y', 'VIIRS Applications Related Active Fire Product C2 vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ $\bf{max}$ FRP", 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VAFP vs MAFP: max, daily'],
                       # ['VAFP_max_overpass', 'MAFP_max_overpass']]
                       ['MaxFRP_X', 'MaxFRP_Y', 'VIIRS Applications Related Active Fire Product C2 vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Overpass}$ $\bf{max}$ FRP", 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VAFP vs MAFP: max, overpass'],
                       # ['VNF_PctRH_75_daily', 'MAFP_PctFRP_75_daily']
                       ['DM_PctRH_75', 'DM_PctFRP_75', 'VIIRS Nightfire vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ $\bf{sum}$ of $\bf{>75}$% FRP/RH from max Radiant Heat", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs MAFP: 75% sum, daily'],
                       # ['VNF_SumRH_daily', 'MAFP_SumFRP_daily']
                       ['Daily_Mean_SumRH', 'Daily_Mean_SumFRP', 'VIIRS Nightfire vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ $\bf{mean}$ $\bf{sum}$ of FRP/RH per overpass", 'VIIRS Nightfire Radiant Heat, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VNF vs MAFP: sum, daily'], 
                       # ['VAFP_PctFRP_75_daily', 'MAFP_PctFRP_75_daily'], 
                       ['DM_PctFRP_75_X', 'DM_PctFRP_75_Y', 'VIIRS Applications Related Active Fire Product C2 vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ mean sum of $\bf{>75}$% FRP", 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VAFP vs MAFP: 75% sum, daily'],
                       # ['VAFP_SumRH_daily', 'MAFP_SumRH_daily']
                       ['Daily_Mean_SumFRP_X', 'Daily_Mean_SumFRP_Y', 'VIIRS Applications Related Active Fire Product C2 vs MODIS MOD14/MYD14 Anomalies product C6.1\n' + r"$\bf{Daily}$ mean $\bf{sum}$ FRP", 'VIIRS AFP Fire Radiative Power, $\it{MW}$', 'MODIS AFP Fire Radiative Power, $\it{MW}$', 'VAFP vs MAFP: sum, daily']
                      ]

# Generating combined dicts
for j, dataset_updated in enumerate(datasets_updated):
    for i, column in enumerate(groupby_key):
        groupby_dict[dataset_updated][column] = groupby_list[j][i]
    for i, column in enumerate(dateonly_key):
        dateonly_dict[dataset_updated][column] = dateonly_list[j][i]

for j, dataset in enumerate(datasets):
    for i, column in enumerate(daynight_key):
        daynight_dict[dataset][column] = daynight_list[j][i]
    for i, column in enumerate(satellite_key):
        satellite_dict[dataset][column] = satellite_list[j][i]

for j, dataset_extracted in enumerate(VNF_datasets):
    for i, column in enumerate(bowtie_key):
        bowtie_dict[dataset_extracted][column] = bowtie_list[j][i]

for j, scatter_dataset in enumerate(scatter_datasets):
    for i, column in enumerate(separate_graph_key):
        separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]][column] = separate_graph_list[j][i]

# Vars
source = {'VNF' : 'VNF/', 'AFP' : 'AFP/'}
crs = "epsg:4326"
ROI = gpd.read_file('ROI/ROI.shp', crs = crs)
maxrh_to_bcm = 0.8307 * 12 / 1000 # from monthly reported data correlation
sumrh75_to_bcm = 0.0116038041494348 # Tulsa coef for no 3D correction
# geod = pyproj.Geod(ellps = 'WGS84') # to calc distances

# Fonts for graphs
font_title = {'family': 'serif', 'size': 10}
font_labels = {'family': 'serif', 'size': 10}
font_labels_2 = {'family': 'serif', 'size': 10}
font_text = {'family': 'sans-serif'}
font_text_2 = {'family': 'sans-serif', 'size': '9.5'}
# plt.rc('font', **font_suptitle)
plt.rc('font', **font_title)
plt.rc('font', **font_labels)
matplotlib.rcParams['mathtext.fontset'] = 'custom'
matplotlib.rcParams['mathtext.rm'] = 'Palatino Linotype'
matplotlib.rcParams['mathtext.it'] = 'Palatino Linotype:italic'
plt.rcParams['figure.figsize'] = [19, 7]
plt.rcParams['figure.dpi'] = 400
csfont = {'fontname': 'Courier'}
pd.io.formats.excel.ExcelFormatter.header_style = None # remove pandas default formatting from saved excel tables

# # Declare dicts
# Read tables
obj = {}
folders = {}
files = {}
df_mafp_buildup = {}
df_vafp_buildup = {}
df_vnf_buildup = {}
df_vnfpct_buildup = {}

# Intersect
df_intersect = {}
intersect = {}
gdf = {}
intersected_sources = {}
dropped = {}
df = {}
frp_pct = {} # dict for AFP values
col = {}

# ================================== Supportive functions for table processing ===========================================

# Add ':' to AFP Local Time
def AFP_local_time(x):
    if int(x) < 10: x = '00:0' + str(x)
    elif int(x) < 60: x = '00:' + str(x)
    elif int(x) < 1000: x = '0' + x[:1] + ':' + x[1:]
    else: x = x[:2] + ':' + x[2:]
    return x

# Rename VIIRS AFP satellite in Sat column
def VAFP_rename_satellite(x):
    if x  == 1: x = 'NOAA-20'
    elif x == 'N': x = 'SNPP'
    return x

def limfindclosest(xmax):
    n = 0
    while xmax < 10 ** n: n += 1 # 10^n stepping
    else: xmax = (xmax // 10 ** (n + 1)) * 10 ** (n + 1) + 10 ** (n + 1)
    return xmax

def merge_and_correct_times(df1, df2, suf1, suf2):
    df_m = pd.merge(df1, df2, on = ['Field', 'Date Time'], suffixes = (suf1, suf2), how = 'outer')
    df_m = df_m.sort_values(by = ['Date Time'])
    df_m.reset_index(inplace = True, drop = True) # reset index
    df_m.index += 1 # shift index by +1
    df_m.index.name = 'index'
    if suf2 == '_VAFP':
        e = 0
        for (i, row) in df_m.iterrows(): 
            if i > 1: 
                df_m.loc[i, 'Timedelta'] = (df_m.loc[i, 'Date Time'] - df_m.loc[i - 1, 'Date Time']).total_seconds()
                if df_m.loc[i, 'Timedelta'] > 300: df_m.loc[i, 'Samescan_err'] = e
                elif df_m.loc[i, 'Timedelta'] <= 300: 
                    df_m.loc[i, 'Samescan_err'] = e
                    df_m.loc[i - 1, 'Samescan_err'] = e
                e += 1
            elif i == 1: 
                df_m.loc[i, 'Samescan_err'] = 0
                e = 1
        df_m = df_m.groupby(['Samescan_err'], as_index = False).first()
        df_m = df_m.drop(['Samescan_err', 'Timedelta'], axis = 1)
    return df_m

# ================================ Read & Process =============================================
# Read source tables
def read_source_tables(datasets_extracted_sources, dataset):
    # Data sources
    folders[dataset] = []
    files[dataset] = []
    msw = 0
    vsw = 0
    vnfsw = 0
    vnfpctsw = 0
    try:   
        obj[dataset] = os.scandir(source[dataset])
        for i, entry in enumerate(obj[dataset]):
            if entry.is_dir(): folders[dataset].append(entry.name) # list dirs only
        print(dataset + ': succesfully read all '  + str(i + 1) + ' test cases.')
        
        # Read & process reported and VNF 
        for reg in folders[dataset]:
            for file in os.listdir(dataset + '/' + reg):
                # print(file)
                if file.endswith(".csv"):
                    print('Reading: ' + dataset + '/' + reg + '/' + file + '…')
                    files[dataset].append(file) # On this step, test expression reads only 2022 year data
                    
                    # MAFP
                    if 'M-C61' in file:
                        msw += 1
                        dftemp = pd.read_csv(dataset + '/' + reg + '/' + file) # parse_dates
                        if msw == 1: df_mafp_buildup[dataset] = dftemp
                        else: df_mafp_buildup[dataset] = pd.concat([df_mafp_buildup[dataset], dftemp])
                    
                    # VAFP
                    elif 'SV-C2' in file or 'J1V-C2' in file:
                        vsw += 1
                        dftemp = pd.read_csv(dataset + '/' + reg + '/' + file) # parse_dates
                        if vsw == 1: df_vafp_buildup[dataset] = dftemp
                        else: df_vafp_buildup[dataset] = pd.concat([df_vafp_buildup[dataset], dftemp])

                    # VNF
                    elif '_vnf_' in file:
                        vnfsw += 1
                        dftemp = pd.read_csv(dataset + '/' + reg + '/' + file) # parse_dates
                        dftemp = dftemp.drop_duplicates(subset = 'id_Key') # drop duplicates
                        if vnfsw == 1: df_vnf_buildup[dataset] = dftemp
                        else: df_vnf_buildup[dataset] = pd.concat([df_vnf_buildup[dataset], dftemp])
                        
                    # VNF_pct
                    elif '_pct_' in file:
                        vnfpctsw += 1
                        dftemp = pd.read_csv(dataset + '/' + reg + '/' + file) # parse_dates
                        dftemp = dftemp.drop_duplicates() # drop duplicates
                        if vnfpctsw == 1: df_vnfpct_buildup[dataset] = dftemp
                        else: df_vnfpct_buildup[dataset] = pd.concat([df_vnfpct_buildup[dataset], dftemp])
        
        # Pass to intersect function and save source files
        if dataset == 'AFP':
            datasets_extracted_sources['MODIS AFP'] = df_mafp_buildup[dataset]
            datasets_extracted_sources['VIIRS AFP'] = df_vafp_buildup[dataset]
        elif dataset == 'VNF':
            datasets_extracted_sources['VNF'] = df_vnf_buildup[dataset]
            datasets_extracted_sources['VNF_pct'] = df_vnfpct_buildup[dataset]
        print(dataset + ' source tables read successfully.')
    except:
        print('\n\nError with: processing tables ' + dataset + '.\n\n')

# Intersect
def intersect_with_ROI(intersected_sources, datasets_extracted_sources, dataset_extracted):
    try:    
        df_intersect[dataset_extracted] = datasets_extracted_sources[dataset_extracted]
        # csv to geopandas
        if dataset_extracted == 'VNF_pct':
            gdf[dataset_extracted] = gpd.GeoDataFrame(
                df_intersect[dataset_extracted], geometry = gpd.points_from_xy(df_intersect[dataset_extracted].Lon, df_intersect[dataset_extracted].Lat, crs = crs))
        elif dataset_extracted == 'VNF':
            gdf[dataset_extracted] = gpd.GeoDataFrame(
                df_intersect[dataset_extracted], geometry = gpd.points_from_xy(df_intersect[dataset_extracted].Lon_GMTCO, df_intersect[dataset_extracted].Lat_GMTCO, crs = crs))
        else:
            gdf[dataset_extracted] = gpd.GeoDataFrame(
                df_intersect[dataset_extracted], geometry = gpd.points_from_xy(df_intersect[dataset_extracted].longitude, df_intersect[dataset_extracted].latitude, crs = crs))
        # Intersection
        intersect[dataset_extracted] = gpd.sjoin(gdf[dataset_extracted], ROI, how = 'left', predicate = 'within')
        intersect[dataset_extracted] = intersect[dataset_extracted].drop(['index_right', 'geometry'], axis = 1)
        intersect[dataset_extracted] = intersect[dataset_extracted].rename({'ID' : 'Field_ID', 'FIELD' : 'Field'}, axis = 1)
        intersect[dataset_extracted] = intersect[dataset_extracted].dropna(subset = ['Field'])       
        # pass to next
        intersected_sources[dataset_extracted] = intersect[dataset_extracted]                        
        # Save and count drops
        intersect[dataset_extracted].to_csv(path_save_source + dataset_extracted + '_source.csv', index = False)
        dropped[dataset_extracted] = len(df_intersect[dataset_extracted]) - len(intersect[dataset_extracted])
        print(dataset_extracted + ': intersection complete. ' + str(dropped[dataset_extracted]) + ' values found outside the ROI were discarded.')
    except:
        print('\n\nError with: intersect ' + dataset_extracted + '.\n\n')

# Sumifs
def process_tables(scatter_dict, intersected_sources, return_dict, dataset_extracted):
    name = multiprocessing.current_process().name
    try:         
        # Passing dataset from the previous function
        df[dataset_extracted] = intersected_sources[dataset_extracted]
        if dataset_extracted in ('MODIS AFP', 'VIIRS AFP'): dataset = 'AFP'
        # elif dataset_extracted == 'VNF_pct': dataset = 'VNF_pct'
        else: dataset = dataset_extracted
        # FRP 75/50/25% max
        if dataset == 'AFP':
            # Empty columns
            df[dataset_extracted]["PctFRP_25"] = np.nan
            df[dataset_extracted]["PctFRP_50"] = np.nan
            df[dataset_extracted]["PctFRP_75"] = np.nan
            df[dataset_extracted]["Npct_25"] = np.nan
            df[dataset_extracted]["Npct_50"] = np.nan
            df[dataset_extracted]["Npct_75"] = np.nan
            df[dataset_extracted]["MaxFRP"] = np.nan
            # reset index
            df[dataset_extracted].reset_index(inplace = True, drop = True) # reset index
            df[dataset_extracted].index += 1 # shift index by +1
            df[dataset_extracted].index.name = 'index'
            frppool = [] # all frp values in samescan
            cols = np.array([[0, 3], [1, 4], [2, 5]])
            for ie, elem in enumerate([0.25, 0.5, 0.75]):
                col[elem] = cols[ie]
                frp_pct[elem] = []
            # df to arrays
            clause_col = df[dataset_extracted].loc[:, ['scan', 'track', 'acq_date', 'Field', 'satellite']].values
            frp_col = df[dataset_extracted].loc[:, 'frp'].values
            pct_col = df[dataset_extracted].loc[:, ['PctFRP_25', 'PctFRP_50', 'PctFRP_75', 'Npct_25', 'Npct_50', 'Npct_75', 'MaxFRP']].values
            dataset_length = len(df[dataset_extracted])
            for i in range(dataset_length):
                if np.array_equal(clause_col[i], clause_col[i - 1]) == True: frppool.append(frp_col[i]) # if current == prev
                else:
                    if frppool != []: # if prev, i - 1, was built up, but current != prev
                        frpmax = max(frppool)
                        for frp_value in frppool:
                            for elem in [0.25, 0.5, 0.75]:
                                if frp_value >= frpmax * elem: frp_pct[elem].append(frp_value)
                        for elem in [0.25, 0.5, 0.75]:
                            pct_col[i - 1, col[elem][0]] = np.sum(frp_pct[elem])
                            pct_col[i - 1, col[elem][1]] = int(np.count_nonzero(frp_pct[elem]))
                            pct_col[i - 1, 6] = max(frp_pct[elem])
                        frppool = []
                    else: 
                        for elem in [0.25, 0.5, 0.75]: # declare ones
                            pct_col[i - 1, col[elem][0]] = frp_col[i - 1]
                            pct_col[i - 1, col[elem][1]] = 1
                            pct_col[i - 1, 6] = frp_col[i - 1]
                    if i < dataset_length - 1: # if next == current: start buildup
                        if np.array_equal(clause_col[i], clause_col[i + 1]) == True: frppool.append(frp_col[i])
                        else: frppool = []
                    for ie, elem in enumerate([0.25, 0.5, 0.75]): frp_pct[elem] = [] # clear paras
                if i % 10000 == 0: print(dataset_extracted + ': calculating pct sums for position', str(i) + '…')
            pct_col = np.array(pct_col)
            df[dataset_extracted]["PctFRP_25"] = np.array(pct_col[:,0])
            df[dataset_extracted]["PctFRP_50"] = np.array(pct_col[:,1])
            df[dataset_extracted]["PctFRP_75"] = np.array(pct_col[:,2])
            df[dataset_extracted]["Npct_25"] = np.array(pct_col[:,3])
            df[dataset_extracted]["Npct_50"] = np.array(pct_col[:,4])
            df[dataset_extracted]["Npct_75"] = np.array(pct_col[:,5])
            df[dataset_extracted]["MaxFRP"] = np.array(pct_col[:,6])
            print(dataset_extracted + ': pct sums calculated.')
        
        # Processing
        if dataset_extracted != 'VNF_pct': # VNF_pct is already summed_up   
            df[dataset_extracted] = df[dataset_extracted].groupby(groupby_dict[dataset]['datetime'], as_index = False).agg(groupby_dict[dataset]['group_columns']).reset_index()
            df[dataset_extracted].columns = df[dataset_extracted].columns.droplevel(-1)
        df[dataset_extracted] = df[dataset_extracted].rename(groupby_dict[dataset]['rename_columns'], axis = 'columns')
        # Renaming second rows
        if dataset == 'VNF':
            df[dataset_extracted].columns.values[2] = 'SumRHI_calc'
            df[dataset_extracted].columns.values[3] = 'Ndtct_calc'
            df[dataset_extracted].columns.values[4] = 'SumRH_calc'
        elif dataset == 'AFP':
            df[dataset_extracted].columns.values[4] = 'SumFRP'
            df[dataset_extracted].columns.values[5] = 'Ndtct_samescan'
        df[dataset_extracted] = df[dataset_extracted].sort_values(by = groupby_dict[dataset]['sort_columns'])
        df[dataset_extracted] = df[dataset_extracted].reindex(columns = groupby_dict[dataset]['reindex_columns'])
        if 'VNF' in dataset: 
            df[dataset_extracted]['Date'] = pd.to_datetime(df[dataset_extracted]['Date'])
            df[dataset_extracted]['Date Day'] = [x + '/' + y + '/' + z for x, y, z in zip(df[dataset_extracted]['Date'].dt.month.astype('str'), df[dataset_extracted]['Date'].dt.day.astype('str'), df[dataset_extracted]['Date'].dt.year.astype('str'))]
            # [x + ':' + y + ':' + z for x, y, z in zip(df[dataset_extracted]['Date'].dt.hour.astype('str'), df[dataset_extracted]['Date'].dt.minute.astype('str'), df[dataset_extracted]['Date'].dt.second.astype('str'))]
            # Removed seconds to vlookup vs AFP
            df[dataset_extracted]['Local Time'] = [x + ':' + y for x, y in zip(df[dataset_extracted]['Date'].dt.hour.astype('str'), df[dataset_extracted]['Date'].dt.minute.astype('str'))]
            # index
            if dataset_extracted == 'VNF':
                df_vnf = df[dataset_extracted]
                df_vnf.reset_index(inplace = True, drop = True) # reset index
                df_vnf.index += 1 # shift index by +1
                df_vnf.index.name = 'index'
                df[dataset_extracted] = df_vnf
            elif dataset_extracted == 'VNF_pct':
                df_vnfpct = df[dataset_extracted]
                df_vnfpct = df_vnfpct.loc[df_vnfpct["SumRH"] != 0] # drop zeros
                df_vnfpct.reset_index(inplace = True, drop = True) # reset index
                df_vnfpct.index += 1 # shift index by +1
                df_vnfpct.index.name = 'index'
                df[dataset_extracted] = df_vnfpct
            print(dataset_extracted + ': removing bowties…')
            df[dataset_extracted].reset_index(inplace = True, drop = True) # reset index
            df[dataset_extracted].index += 1 # shift index by +1
            df[dataset_extracted].index.name = 'index' # shift index by +1

            # Bowtie
            df[dataset_extracted]['Time_delta'] = df[dataset_extracted].Date.diff()
            df[dataset_extracted]['Time_delta'] = df[dataset_extracted]['Time_delta'].apply(lambda x : x.total_seconds())
            df[dataset_extracted]['Time_delta'].loc[0] = -1 # set first cell to a no-nan value
            df[dataset_extracted]['Time_delta'] = df[dataset_extracted]['Time_delta'].apply(lambda x : np.NaN if round(x, 0) == 35 or round(x,0) == 37 else x) # drop the 35-37 sec 12.2017 problem
            df[dataset_extracted] = df[dataset_extracted].dropna(subset = ['Time_delta'])
            df[dataset_extracted] = df[dataset_extracted].drop_duplicates(subset = ['Lon', 'Field', 'Date Day'], keep = 'last') # drop duplicate
            # index
            df[dataset_extracted].reset_index(inplace = True, drop = True) # reset index
            df[dataset_extracted].index += 1 # shift index by +1
            df[dataset_extracted].index.name = 'index' # shift index by +1
            # index samescans with bowties
            val = 0
            samescan = 0
            for i, row in df[dataset_extracted].iterrows():
                if 0 <= int(df[dataset_extracted].at[i, 'Time_delta']) <= 5:
                    val = 1
                    df[dataset_extracted].at[i, 'Samescan'] = samescan
                    df[dataset_extracted].at[i - 1, 'Samescan'] = samescan
                else: 
                    val = 0
                    samescan += 1
                    df[dataset_extracted].at[i, 'Samescan'] = samescan
                df[dataset_extracted].at[i, 'Bowtie'] = val
                # index
            df[dataset_extracted].reset_index(inplace = True, drop = True) # reset index
            df[dataset_extracted].index += 1 # shift index by +1
            df[dataset_extracted].index.name = 'index' # shift index by +1
            # Save with bowtie
            df[dataset_extracted].to_csv(path_save + dataset_extracted + '_with bowtie_sum.csv')
            print(dataset_extracted + ': sumifs file with bowtie saved successfully.')
            
            # Removing bowtie
            if dataset_extracted == 'VNF':
                df_vnf = df[dataset_extracted]
                rhmax = []
                imax = 1
                for i, row in df_vnf.iterrows():
                    if i > 1:
                        while df_vnf.at[i, 'Samescan'] == df_vnf.at[i - 1, 'Samescan']:
                            if rhmax == [] or df_vnf.at[i, 'SumRH_calc'] > max(rhmax): 
                                rhmax.append(df_vnf.at[i, 'SumRH_calc'])
                                imax = i
                            else: break
                        else:
                            if imax == '': imax = i
                            else:
                                df_vnf.at[i - 1, 'Ndtct_calc'] = df_vnf.at[imax, 'Ndtct_calc']
                                df_vnf.at[i - 1, 'Temp_BB'] = df_vnf.at[imax, 'Temp_BB'] # change last
                                df_vnf.at[i - 1, 'Area_BB'] = df_vnf.at[imax, 'Area_BB']
                                df_vnf.at[i - 1, 'Lat'] = df_vnf.at[imax, 'Lat']
                                df_vnf.at[i - 1, 'Lon'] = df_vnf.at[imax, 'Lon']
                            rhmax = []
                            imax = i
                            rhmax.append(df_vnf.at[i, 'SumRH_calc'])
                df[dataset_extracted] = df_vnf
                return_dict['VNF'] = df[dataset_extracted]
                
            elif dataset_extracted == 'VNF_pct':
                df_vnfpct = df[dataset_extracted]
                rhmax = []
                imax = 1
                for i, row in df_vnfpct.iterrows():
                    if i > 1:
                        while df_vnfpct.at[i, 'Samescan'] == df_vnfpct.at[i - 1, 'Samescan']:
                            # print(samescan)
                            if rhmax == [] or df_vnfpct.at[i, 'SumRH'] > max(rhmax):
                                rhmax.append(df_vnfpct.at[i, 'SumRH'])
                                imax = i
                            else: break
                        else:
                            if imax == '': imax = i
                            else:
                                df_vnfpct.at[i - 1, 'Npct'] = df_vnfpct.at[imax, 'Ndtct'] # change last
                                df_vnfpct.at[i - 1, 'Npct_75'] = df_vnfpct.at[imax, 'Npct_75']
                                df_vnfpct.at[i - 1, 'Npct_50'] = df_vnfpct.at[imax, 'Npct_50']
                                df_vnfpct.at[i - 1, 'Npct_25'] = df_vnfpct.at[imax, 'Npct_25']
                                df_vnfpct.at[i - 1, 'PctRH_75'] = df_vnfpct.at[imax, 'PctRH_75']
                                df_vnfpct.at[i - 1, 'PctRH_50'] = df_vnfpct.at[imax, 'PctRH_50']
                                df_vnfpct.at[i - 1, 'PctRH_25'] = df_vnfpct.at[imax, 'PctRH_25']
                                df_vnfpct.at[i - 1, 'Nfit'] = df_vnfpct.at[imax, 'Nfit']
                                df_vnfpct.at[i - 1, 'MaxRH'] = df_vnfpct.at[imax, 'MaxRH']
                                df_vnfpct.at[i - 1, 'Temp_BB'] = df_vnfpct.at[imax, 'Temp_BB']
                                df_vnfpct.at[i - 1, 'Area_BB'] = df_vnfpct.at[imax, 'Area_BB']
                                df_vnfpct.at[i - 1, 'SATZ'] = df_vnfpct.at[imax, 'SATZ']
                                df_vnfpct.at[i - 1, 'Lat'] = df_vnfpct.at[imax, 'Lat']
                                df_vnfpct.at[i - 1, 'Lon'] = df_vnfpct.at[imax, 'Lon']
                            rhmax = []
                            imax = i
                            rhmax.append(df_vnfpct.at[i, 'SumRH'])
                df[dataset_extracted] = df_vnfpct
            
            print(dataset_extracted + ': sumifs file with bowtie corrected saved successfully.')
            df[dataset_extracted] = df[dataset_extracted].groupby(bowtie_dict[dataset_extracted]['groupby'], as_index = False).agg(bowtie_dict[dataset_extracted]['group_columns'])
            df[dataset_extracted].columns = df[dataset_extracted].columns.droplevel(-1)
            df[dataset_extracted].columns.values[3] = 'N_Bowtie_Scans'
            df[dataset_extracted]["N_Bowtie_Scans"] = df[dataset_extracted]["N_Bowtie_Scans"].apply(lambda x : x - 1)
            df[dataset_extracted].set_index('Samescan')
            df[dataset_extracted].index.name = 'index' # shift index by +1
            df[dataset_extracted].index += 1 # shift index by +1
            df[dataset_extracted] = df[dataset_extracted].reindex(columns = bowtie_dict[dataset_extracted]['reindex_columns'])
            # bcm conv
            if dataset_extracted == 'VNF_pct':
                df[dataset_extracted]["bcm_MaxRH_annual_eq"] = df[dataset_extracted]["MaxRH"].apply(lambda x : x * maxrh_to_bcm)
                df[dataset_extracted]["bcm_PctRH_75_annual_eq"] = df[dataset_extracted]["PctRH_75"].apply(lambda x : x * sumrh75_to_bcm)
            df[dataset_extracted].to_csv(path_save + dataset_extracted + '_removed bowtie_sum.csv')
            return_dict['VNF_pct'] = df[dataset_extracted]
        
        if dataset == 'AFP':
            # Transform time  by adding ':'
            df[dataset_extracted]['Local Time'] = df[dataset_extracted]['Local Time'].astype('str').transform(AFP_local_time)
            if dataset_extracted == 'MODIS AFP':
                df_mafp = df[dataset_extracted].query("instrument == 'MODIS'")
                df_mafp.reset_index(inplace = True, drop = True) # reset index
                df_mafp.index += 1 # shift index by +1
                df_mafp.index.name = 'index' # shift index by +1
                return_dict['MODIS AFP'] = df_mafp
                df_mafp.to_csv(path_save + 'MODIS AFP' + '_sum.csv')
            elif dataset_extracted == 'VIIRS AFP': 
                df_vafp = df[dataset_extracted].query("instrument == 'VIIRS'")
                df_vafp.reset_index(inplace = True, drop = True) # reset index
                df_vafp.index += 1 # shift index by +1
                df_vafp.index.name = 'index' # shift index by +1
                df_vafp['Satellite'] = df_vafp['Satellite'].transform(VAFP_rename_satellite) # rename VAFP sattellite
                return_dict['VIIRS AFP'] = df_vafp
                df_vafp.to_csv(path_save + 'VIIRS AFP' + '_sum.csv')    
        if dataset_extracted == 'VNF_pct': scatter_dict['VNF_overpass'] = df[dataset_extracted]
        elif dataset_extracted == 'VIIRS AFP': scatter_dict['VAFP_overpass'] = df_vafp
        elif dataset_extracted == 'MODIS AFP': scatter_dict['MAFP_overpass'] = df_mafp
        print(dataset_extracted + ': sumifs file successsfully processed.')
    except:
        print('\n\nError with: processing tables ' + dataset + '.\n\n')
    # return return_dict

# daily sums/means
def sum_tables(scatter_dict, return_dict, processed_dataset):
    name = multiprocessing.current_process().name
    df_vafp = return_dict['VIIRS AFP']
    df_mafp = return_dict['MODIS AFP']
    df_vnf = return_dict['VNF']
    df_vnfpct = return_dict['VNF_pct']
    try:
        print('Processing sum tables for ' + processed_dataset + '…')
        if processed_dataset in ('MODIS AFP', 'VIIRS AFP'): 
            dataset = 'AFP'
            if processed_dataset == 'MODIS AFP': df_proxy = df_mafp
            elif processed_dataset == 'VIIRS AFP': df_proxy = df_vafp
            
            # dateonly
            df_proxy_mean_dateonly = df_proxy.groupby(dateonly_dict[dataset]['group_mean_dateonly'], as_index = False).agg(dateonly_dict[dataset]['group_mean_dateonly_columns']).reset_index()
            df_proxy_mean_dateonly.columns = df_proxy_mean_dateonly.columns.droplevel(-1)
            # Renaming, rearranging and sorting columns
            df_proxy_mean_dateonly.columns.values[0] = 'index_date_sum'
            df_proxy_mean_dateonly.columns.values[2] = 'Daily_SumFRP'
            df_proxy_mean_dateonly.columns.values[3] = 'Daily_Mean_SumFRP'
            df_proxy_mean_dateonly.columns.values[4] = 'Ndtct_scans'
            df_proxy_mean_dateonly.columns.values[5] = 'Ndtct_all'
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.rename({'MaxFRP': 'Daily_MaxFRP', 'PctFRP_75': 'DM_PctFRP_75', 'PctFRP_50': 'DM_PctFRP_50', 'PctFRP_25': 'DM_PctFRP_25', 
                                                                    'Npct_75': 'DM_Npct_75', 'Npct_50': 'DM_Npct_50', 
                                                                    'Npct_25': 'DM_Npct_25'}, axis = 1)
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.reindex(columns = dateonly_dict[dataset]['group_mean_dateonly_reindex'])
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.sort_values(by = dateonly_dict[dataset]['group_mean_dateonly'])
            df_proxy_mean_dateonly.reset_index(inplace = True, drop = True) # reset index
            df_proxy_mean_dateonly.index += 1 # shift index by +1
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.drop(['index_date_sum'], axis = 1)
            df_proxy_mean_dateonly.index.name = 'index_dateonly' # shift index by +1
            # Save
            df_proxy_mean_dateonly.to_csv(path_save + 'Daily summed datasets/' + processed_dataset + '_mean_dateonly.csv')
            
            # daynight
            df_proxy_mean_daynight = df_proxy.groupby(daynight_dict[dataset]['group_mean_daynight'], as_index = False).agg(daynight_dict[dataset]['group_mean_daynight_columns']).reset_index()
            df_proxy_mean_daynight.columns = df_proxy_mean_daynight.columns.droplevel(-1)
            # Renaming, rearranging and sorting columns
            df_proxy_mean_daynight.columns.values[0] = 'index_date_sum'
            df_proxy_mean_daynight.columns.values[2] = 'Daily_SumFRP'
            df_proxy_mean_daynight.columns.values[3] = 'Daily_Mean_SumFRP'
            df_proxy_mean_daynight.columns.values[4] = 'Ndtct_scans'
            df_proxy_mean_daynight.columns.values[5] = 'Ndtct_all'
            df_proxy_mean_daynight = df_proxy_mean_daynight.rename({'MaxFRP': 'Daily_MaxFRP', 'PctFRP_75': 'DM_PctFRP_75', 'PctFRP_50': 'DM_PctFRP_50', 'PctFRP_25': 'DM_PctFRP_25', 'Npct_75': 'DM_Npct_75', 'Npct_50': 'DM_Npct_50', 'Npct_25': 'DM_Npct_25'}, axis = 1)
            df_proxy_mean_daynight = df_proxy_mean_daynight.reindex(columns = daynight_dict[dataset]['group_mean_daynight_reindex'])
            df_proxy_mean_daynight = df_proxy_mean_daynight.sort_values(by = daynight_dict[dataset]['group_mean_daynight'])
            df_proxy_mean_daynight.reset_index(inplace = True, drop = True) # reset index
            df_proxy_mean_daynight.index += 1 # shift index by +1
            df_proxy_mean_daynight = df_proxy_mean_daynight.drop(['index_date_sum'], axis = 1)
            df_proxy_mean_daynight.index.name = 'index_daynight' # shift index by +1
            # Save
            df_proxy_mean_daynight.to_csv(path_save + 'Daily summed datasets/' + processed_dataset + '_mean_daynight.csv', index = False)
            
            # satellite
            df_proxy_mean_satellite = df_proxy.groupby(satellite_dict[dataset]['group_mean_satellite'], as_index = False).agg(satellite_dict[dataset]['group_mean_satellite_columns']).reset_index()
            df_proxy_mean_satellite.columns = df_proxy_mean_satellite.columns.droplevel(-1)
            # Renaming, rearranging and sorting columns
            df_proxy_mean_satellite.columns.values[0] = 'index_date_sum'
            df_proxy_mean_satellite.columns.values[2] = 'Daily_SumFRP'
            df_proxy_mean_satellite.columns.values[3] = 'Daily_Mean_SumFRP'
            df_proxy_mean_satellite.columns.values[4] = 'Ndtct_scans'
            df_proxy_mean_satellite.columns.values[5] = 'Ndtct_all'
            df_proxy_mean_satellite = df_proxy_mean_satellite.rename({'MaxFRP': 'Daily_MaxFRP', 'PctFRP_75': 'DM_PctFRP_75', 'PctFRP_50': 'DM_PctFRP_50', 'PctFRP_25': 'DM_PctFRP_25', 'Npct_75': 'DM_Npct_75', 'Npct_50': 'DM_Npct_50', 'Npct_25': 'DM_Npct_25'}, axis = 1)
            df_proxy_mean_satellite = df_proxy_mean_satellite.reindex(columns = satellite_dict[dataset]['group_mean_satellite_reindex'])
            df_proxy_mean_satellite = df_proxy_mean_satellite.sort_values(by = satellite_dict[dataset]['group_mean_satellite'])
            df_proxy_mean_satellite.reset_index(inplace = True, drop = True) # reset index
            df_proxy_mean_satellite.index += 1 # shift index by +1
            df_proxy_mean_satellite = df_proxy_mean_satellite.drop(['index_date_sum'], axis = 1)
            df_proxy_mean_satellite.index.name = 'index_satellite' # shift index by +1
            # Save
            df_proxy_mean_satellite.to_csv(path_save + 'Daily summed datasets/' + processed_dataset + '_mean_satellite.csv', index = False)

        else: 
            if processed_dataset == 'VNF': 
                df_proxy = df_vnf
                dataset = 'VNF'
            elif processed_dataset == 'VNF_pct': 
                df_proxy = df_vnfpct
                dataset = 'VNF_pct'    
            
            # dateonly
            df_proxy_mean_dateonly = df_proxy.groupby(dateonly_dict[dataset]['group_mean_dateonly'], as_index = False).agg(dateonly_dict[dataset]['group_mean_dateonly_columns']).reset_index()
            df_proxy_mean_dateonly.columns = df_proxy_mean_dateonly.columns.droplevel(-1)
            if processed_dataset == 'VNF_pct': df_proxy_mean_dateonly = df_proxy_mean_dateonly.rename({'bcm_MaxRH_annual_eq': 'DM_MaxRH_bcm', 'bcm_PctRH_75_annual_eq': 'DM_PctRH_75_bcm'}, axis = 1)
            if processed_dataset == 'VNF': 
                df_proxy_mean_dateonly.columns.values[0] = 'index_date_sum'
                df_proxy_mean_dateonly.columns.values[1] = 'Daily_Sum_RHI'
                df_proxy_mean_dateonly.columns.values[2] = 'Daily_Mean_RHI'
                df_proxy_mean_dateonly.columns.values[3] = 'Daily_Sum_RH'
                df_proxy_mean_dateonly.columns.values[4] = 'Daily_Mean_RH'
                df_proxy_mean_dateonly.columns.values[5] = 'Ndtct_scans'
                df_proxy_mean_dateonly.columns.values[6] = 'Ndtct_all'
                df_proxy_mean_dateonly.columns.values[7] = 'Mean_Temp_BB'
                df_proxy_mean_dateonly.columns.values[8] = 'Mean_Area_BB'
                df_proxy_mean_dateonly.columns.values[12] = 'Date'

            elif processed_dataset == 'VNF_pct':
                df_proxy_mean_dateonly.columns.values[0] = 'index_date_sum'
                df_proxy_mean_dateonly.columns.values[2] = 'Date'
                df_proxy_mean_dateonly.columns.values[3] = 'Daily_SumRH'
                df_proxy_mean_dateonly.columns.values[4] = 'Daily_Mean_SumRH'
                df_proxy_mean_dateonly.columns.values[5] = 'Daily_MaxRH'
                df_proxy_mean_dateonly.columns.values[6] = 'Ndtct_scans'
                df_proxy_mean_dateonly.columns.values[7] = 'Ndtct_all'
                df_proxy_mean_dateonly.columns.values[14] = 'DM_PctRH_75'
                df_proxy_mean_dateonly.columns.values[13] = 'DM_PctRH_50'
                df_proxy_mean_dateonly.columns.values[12] = 'DM_PctRH_25'
                df_proxy_mean_dateonly.columns.values[10] = 'DM_Npct_75'
                df_proxy_mean_dateonly.columns.values[9] = 'DM_Npct_50'
                df_proxy_mean_dateonly.columns.values[8] = 'DM_Npct_25'
                df_proxy_mean_dateonly.columns.values[11] = 'DM_Nfit'
                df_proxy_mean_dateonly.columns.values[17] = 'Mean_Temp_BB'
                df_proxy_mean_dateonly.columns.values[18] = 'Mean_Area_BB'
            
            # Renaming, rearranging and sorting columns
            df_proxy_mean_dateonly['Date'] = pd.to_datetime(df_proxy_mean_dateonly['Date'], format = '%m/%d/%Y')
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.reindex(columns = dateonly_dict[dataset]['group_mean_dateonly_reindex'])
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.sort_values(by = ['Field', 'Date'])
            df_proxy_mean_dateonly.reset_index(inplace = True, drop = True) # reset index
            df_proxy_mean_dateonly.index += 1 # shift index by +1
            df_proxy_mean_dateonly = df_proxy_mean_dateonly.drop(['index_date_sum'], axis = 1)
            df_proxy_mean_dateonly.index.name = 'index_dateonly' # shift index by +1
            # if processed_dataset == 'VNF_pct': df_vnfpct_proxy = df_proxy_mean_dateonly
            df_proxy_mean_dateonly.to_csv(path_save + 'Daily summed datasets/' + processed_dataset + '_mean_dateonly.csv')
        # transfer dict
        if processed_dataset == 'VIIRS AFP': 
            scatter_dict['VAFP_daily'] = df_proxy_mean_dateonly
            scatter_dict['VAFP_daynight'] = df_proxy_mean_daynight
        elif processed_dataset == 'MODIS AFP': 
            scatter_dict['MAFP_daily'] = df_proxy_mean_dateonly
            scatter_dict['MAFP_daynight'] = df_proxy_mean_daynight
        elif processed_dataset == 'VNF_pct': 
            scatter_dict['VNF_daily'] = df_proxy_mean_dateonly
        print(processed_dataset + ' date mean FRPs by date, day/night, or satellite saved successfully.')
    except:
        print('\n\nError with: processing tables ' + processed_dataset + '.\n\n')

# Global scatter
def scatterplots(scatter_dict, scatter_dataset, csv_dict, frp_to_bcm_dict):
    name = multiprocessing.current_process().name
    # Transfer datasets   
    for dataset in scatter_dataset:
        if 'VNF' in dataset:
            # VNF scatter_dict
            if 'daily' in dataset: proxy = scatter_dict['VNF_daily']
            elif 'overpass' in dataset: 
                proxy = scatter_dict['VNF_overpass']
                proxy = proxy.drop(['Date'], axis = 1)
                proxy = proxy.rename({'Date Day' : 'Date'}, axis = 1)
        elif 'VAFP' in dataset:
            # VAFP scatter_dict
            if 'daily' in dataset: proxy = scatter_dict['VAFP_daily']
            elif 'overpass' in dataset: proxy = scatter_dict['VAFP_overpass']
            elif 'day' in dataset or 'night' in dataset: proxy = scatter_dict['VAFP_daynight']
        elif 'MAFP' in dataset:
            # MAFP scatter_dict
            if 'daily' in dataset: proxy= scatter_dict['MAFP_daily']
            elif 'overpass' in dataset: proxy = scatter_dict['MAFP_overpass']
            elif 'day' in dataset or 'night' in dataset: proxy = scatter_dict['MAFP_daynight']
        if dataset == scatter_dataset[0]: X = proxy
        elif dataset == scatter_dataset[1]: Y = proxy

    try:
        if 'night' not in scatter_dataset[0]: 
            if 'daily' in scatter_dataset[0]: 
                # Crutch
                if 'VNF' in scatter_dataset[0]: X = pd.read_csv(path_save + 'Daily summed datasets/VNF_pct_mean_dateonly.csv')
                df_merge = pd.merge(X, Y, on = ['Field', 'Date'], suffixes = ('_X', '_Y'))
            elif 'overpass' in scatter_dataset[0]: 
                if 'VNF' in scatter_dataset[0]: 
                    # Crutch
                    X = X.rename({'Date Day' : 'Date'}, axis = 1)
                    X['Date'] = pd.to_datetime(X['Date'])
                    Y['Date'] = pd.to_datetime(Y['Date'])
                    df_merge = pd.merge(X, Y, on = ['Field', 'Date', 'Local Time'], suffixes = ('_X', '_Y'))
                else: df_merge = pd.merge(X, Y, on = ['Field', 'Date', 'Local Time'], suffixes = ('_X', '_Y'))
        elif 'night' in scatter_dataset[0]: 
            if 'night' in scatter_dataset[0]:
                X = X.query("daynight == 'N'")
                Y = Y.query("daynight == 'D'")
                df_merge = pd.merge(X, Y, on = ['Field', 'Date'], suffixes = ('_X', '_Y'))
        df_merge = df_merge.loc[df_merge[separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['X_column']] != 0]
        df_merge = df_merge.loc[df_merge[separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Y_column']] != 0]
        X = df_merge[separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['X_column']]
        Y = df_merge[separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Y_column']]
        n_intersects = len(X)
        if 'VNF' in scatter_dataset[0]: eq_x = r'$\ RH_{VNF}$'
        elif 'VAFP' in scatter_dataset[0]: eq_x = r'$\ FRP_{VAFP}$'
        elif 'MAFP' in scatter_dataset[0]: eq_x = r'$\ FRP_{MAFP}$'
        if 'VNF' in scatter_dataset[1]: eq_y = r'$\ RH_{VNF}$'
        elif 'VAFP' in scatter_dataset[1]: eq_y = r'$\ FRP_{VAFP}$'
        elif 'MAFP' in scatter_dataset[1]: eq_y = r'$\ FRP_{MAFP}$'
        # Regression
        gradient, intercept, r_value, p_value, std_err = stats.linregress(X, Y) # Paras
        r_value = r_value ** 2 # R²
        mn = np.min(X)
        mx = np.max(X)
        # Plotting graph
        print(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Vs'] + ': plotting separate graphs…')
        # Color
        if 'MAFP' in scatter_dataset[1] or 'MAFP' in scatter_dataset[0]: color = 'magenta'
        else: color = 'navy'
        if 'night' in scatter_dataset[0]: color = 'grey'
        fig, ax = plt.subplots(figsize = (10, 6), dpi = 400)
        plt.plot(X, Y,
                  '.', linestyle = '',
                  ms = 10, mec = 'k', mew = 0.5, alpha = .85, mfc = color) # mec # mec = edge color, mfc = face color, 'k' = black, mew = edge width
        # Plot the regression lines
        # Title and labels
        plt.title(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Title'], fontdict = font_title)
        plt.xlabel(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Xlabel'], fontdict = font_labels)
        plt.ylabel(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Ylabel'], fontdict = font_labels)
        # axes lims
        axes = plt.gca()
        xmin, xmax = plt.gca().get_xlim() 
        ymin, ymax = plt.gca().get_ylim()
        ax.set(xlim = (0, limfindclosest(xmax)),
                ylim = (0, limfindclosest(ymax)))
        # regression line
        print(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Vs'] + ': regression…')
        x1 = np.linspace(mn, xmax)
        y1 = gradient * x1 + intercept
        # Conf. intervals
        MSE = sum((gradient * X -  Y) ** 2) / (len(X) - 1);
        SSX = sum(X ** 2);
        tinv = lambda p, df: abs(t.ppf(p / 2, df))
        ts = tinv(0.025, len(X) - 1)
        dyline = ts * sqrt(MSE) * (1 + np.linspace(mn, mx) ** 2 / SSX)
        error = statistics.mean(dyline)
        cilow = gradient * x1 + intercept - dyline
        cihigh = gradient * x1 + intercept + dyline
        # plots
        plt.plot(x1, y1, '-k', linewidth = 0.75, alpha = .85) # regression
        plt.plot(x1, cilow, '--r') # conf int low
        plt.plot(x1, cihigh, '--r') # conf int high
        # plottext
        # Graph the new min/maxes again for eq. text position
        xmin, xmax = plt.gca().get_xlim() 
        ymin, ymax = plt.gca().get_ylim()
        # text variables
        if float(intercept) >= 0: sign = ' + ' + str(round(intercept, 1))
        else: sign = ' – ' + str(abs(round(intercept, 1)))                        
        eq = str(eq_y) + ' = ' + str(round(gradient, 3)) + ' ×' + str(eq_x) + sign + ' ± ' + str(round(error, 1))
        eq_r2 = 'R² = ' + str(round(r_value, 3))
        plt.text(xmax * 0.5, ymax * 0.87, eq + '\n\n' + eq_r2, fontsize = 12, ha = 'center', va = 'center')
        # Regression paras to be saved in a list
        fit_paras = np.array([scatter_dataset[0] + ' vs ' + scatter_dataset[1], round(gradient, 4), round(intercept, 2), round(r_value, 3), p_value, round(std_err, 4), round(error, 2), int(n_intersects)])
        # save bcm vs frp corr
        if scatter_dataset == ['VNF_PctRH_75_daily', 'VAFP_PctFRP_75_daily']: frp_to_bcm_dict['VAFP_75pct'] = 0.2943 # gradient # 0.5746 # 0.2943
        if scatter_dataset == ['VNF_max_daily', 'VAFP_max_daily']: frp_to_bcm_dict['VAFP_max'] = 0.3277  # gradient # 0.26 # 0.3433
        if scatter_dataset == ['VNF_PctRH_75_daily', 'MAFP_PctFRP_75_daily']: frp_to_bcm_dict['MAFP_75pct'] = 0.3907 # gradient # 0.4645 # 0.3907
        if scatter_dataset == ['VNF_max_daily', 'MAFP_max_daily']: frp_to_bcm_dict['MAFP_max'] = 0.5232 # gradient # 0.29 # 0.3523
        # Save plot                
        if os.path.exists(path_save_scatters) == False: os.makedirs(path_save_scatters) # if does not exist
        plt.savefig(path_save_scatters + scatter_dataset[0] + ' vs ' + scatter_dataset[1] + '.png', bbox_inches = 'tight')
        print(separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Vs'] + ': by region graph: file successfully saved.')
        # fit paras
        csv_dict[separate_graph_dict[scatter_dataset[0]+scatter_dataset[1]]['Vs']] = fit_paras
    except:
        print('\n\nError with: processing tables ' + scatter_dataset[0] + ' vs ' + scatter_dataset[1] + '.\n\n')

# Calculating numbers of entities in each catalog
def n_entries(scatter_dict, dataset_source, fields, unique_objects):
    name = multiprocessing.current_process().name
    try:    
        df_current = fields[dataset_source] # pass the dict entry: sumifs table
        unique_objects_dataset = df_current['Field'].unique()
        if dataset_source == 'VNF': unique_objects['VNF'] = unique_objects_dataset
        elif dataset_source == 'MODIS AFP': unique_objects['MODIS AFP'] = unique_objects_dataset
        elif dataset_source == 'VIIRS AFP': unique_objects['VIIRS AFP'] = unique_objects_dataset
    except:
        print('\n\nError with: calculating number of entries for ' + dataset_source + '.\n\n')

# Dashborads
def dashboards(fields, field, frp_to_bcm_dict, max_dict):
    name = multiprocessing.current_process().name
    try: 
        if os.path.exists(path_save_dashboard + field + '/') == False: os.makedirs(path_save_dashboard + field + '/')
        VNF = fields['VNF'].query("Field == @field")
        datelim = []
        rhlim = []
        datelim2 = []
        rhlim2 = []
        if len(VNF['Date']) != 0: 
            VNF_datelim = [min(VNF['Date']), max(VNF['Date'])]
            VNF_rhlim = [min(VNF['MaxRH']), max(VNF['MaxRH'])]
            datelim2.append(VNF_datelim)
            rhlim2.append(VNF_rhlim)
        VAFP = fields['VIIRS AFP'].query("Field == @field")
        VAFP['Date_Conv'] = pd.to_datetime(VAFP['Date'] + ' ' + VAFP['Local Time'])
        if len(VAFP['Date_Conv']) != 0: 
            VAFP_datelim = [min(VAFP['Date_Conv']), max(VAFP['Date_Conv'])]
            VAFP_rhlim = [min(VAFP['MaxFRP']), max(VAFP['MaxFRP'])]
            datelim2.append(VAFP_datelim)
            rhlim2.append(VAFP_rhlim)
        MAFP = fields['MODIS AFP'].query("Field == @field")
        MAFP['Date_Conv'] = pd.to_datetime(MAFP['Date'] + ' ' + MAFP['Local Time'])
        if len(MAFP['Date_Conv']) != 0: 
            MAFP_datelim = [min(MAFP['Date_Conv']), max(MAFP['Date_Conv'])]
            MAFP_rhlim = [min(MAFP['MaxFRP']), max(MAFP['MaxFRP'])]
            datelim2.append(MAFP_datelim)
            rhlim2.append(MAFP_rhlim)
        for elem in datelim2:
            for num in elem: datelim.append(num)
        for elem in rhlim2:
            for num in elem: rhlim.append(num)
        datelim = [min(datelim), max(datelim)]
        rhlim = [0, max(rhlim)]
        
# =============================================================================
#         # Plot: all time
# =============================================================================
        datelim = [datelim[0] - timedelta(days = 5), datelim[1] + timedelta(days = 5)]
        fig, axes = plt.subplots(1, 3, figsize = (19, 7), dpi = 400)
        plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
        # Temp
        # Mean lines
        date_proxy = pd.date_range(datelim[0], datelim[1]).to_pydatetime() # mean temp line
        if len(VNF['Date']) != 0:
            mean_temp = [VNF['Temp_BB'].mean() for elem in np.zeros(len(date_proxy), )]
        #     mean_rh = [VNF['MaxRH'].mean() for elem in np.zeros(len(date_proxy), )]
        # if len(VAFP['Date_Conv']) != 0: mean_vfrp = [VAFP['MaxFRP'].mean() for elem in np.zeros(len(date_proxy), )]
        # if len(MAFP['Date_Conv']) != 0: mean_mfrp = [MAFP['MaxFRP'].mean() for elem in np.zeros(len(date_proxy), )]
        
        # Temperature
        ax1 = plt.subplot2grid((3, 1), (0, 0))
        if len(VNF['Date']) != 0: 
            ax1.scatter(VNF['Date'], VNF['Temp_BB'],
                    label = "Temp, K", color = 'red', zorder = 10, **plot_kwds1)
            ax1.plot(date_proxy, mean_temp, '--k', linewidth = 0.75, alpha = .85)
        plt.title('Температура по VIIRS Nightfire', fontdict = font_text_2)
        ax1.set_ylabel('Температура, K')
        ax1.set_xlim(datelim)
        # Add mean temp tick
        if len(VNF['Date']) != 0: 
            props = dict(facecolor = 'white', edgecolor = 'none', pad = 10)
            a = (datelim[1] - datelim[0]).total_seconds() / 2
            datelabelx = datelim[0] + datetime.timedelta(seconds = a)
            ymin, ymax = plt.gca().get_ylim()
            ylimtemp = ax1.get_ylim()
            ylimtemp = ylimtemp[0] + 0.85 * (ylimtemp[1] - ylimtemp[0])
            plt.text(x = datelabelx, y = ylimtemp, s = 'T ср. = ' + str(int(VNF['Temp_BB'].mean())) + ' K', ha = 'center', va = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp
        
        # ax1.set_ylim([0, 3000])
        # if len(VNF['Date']) != 0: ax1.legend(loc = 'upper left')
        
        # # Max RH/FRP
        # ax2 = plt.subplot2grid((3, 1), (1, 0))
        # if len(VAFP['Date_Conv']) != 0:
        #     ax2.scatter(VAFP['Date_Conv'], VAFP['MaxFRP'],
        #             label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
        #     ax2.plot(date_proxy, mean_vfrp, '--', color = 'magenta', linewidth = 0.75, alpha = .85)
        # if len(MAFP['Date_Conv']) != 0:
        #     ax2.scatter(MAFP['Date_Conv'], MAFP['MaxFRP'],
        #             label = "MaxFRP, MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
        #     ax2.plot(date_proxy, mean_mfrp, '--', color = 'navy', linewidth = 0.75, alpha = .85)
        # if len(VNF['Date']) != 0:
        #     ax2.scatter(VNF['Date'], VNF['MaxRH'],
        #             label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1) 
        #     ax2.plot(date_proxy, mean_rh, '--', color = 'gold', linewidth = 0.75, alpha = .85)
        # plt.title('Энергия излучения (MaxRH или MaxFRP)')
        # ax2.set_ylabel('Radiative heat / \nFire radiative power, МВт')
        # ax2.set_xlim(datelim)
        # ax2.set_ylim([0, rhlim[1] * 1.1])
        # ax2.legend(loc = 'upper left')
            
        # MaxRH vs bcm
        ax2 = plt.subplot2grid((3, 1), (1, 0))
        if len(VAFP['Date_Conv']) != 0:
            VAFP['MaxFRP_bcm'] = VAFP['MaxFRP'].apply(lambda x: x * maxrh_to_bcm / frp_to_bcm_dict['VAFP_max'] / 365 * 1000)
            ax2.scatter(VAFP['Date_Conv'], VAFP['MaxFRP_bcm'],
                    label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
            mean_vfrp_max_bcm = [VAFP['MaxFRP_bcm'].mean() for elem in np.zeros(len(date_proxy), )]
            ax2.plot(date_proxy, mean_vfrp_max_bcm, '--', color = 'magenta', linewidth = 0.75, alpha = .85)
        if len(MAFP['Date_Conv']) != 0:
            MAFP['MaxFRP_bcm'] = MAFP['MaxFRP'].apply(lambda x: x * maxrh_to_bcm / frp_to_bcm_dict['MAFP_max'] / 365 * 1000)
            ax2.scatter(MAFP['Date_Conv'], MAFP['MaxFRP_bcm'],
                    label = "MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
            mean_mfrp_max_bcm = [MAFP['MaxFRP_bcm'].mean() for elem in np.zeros(len(date_proxy), )]
            ax2.plot(date_proxy, mean_mfrp_max_bcm, '--', color = 'navy', linewidth = 0.75, alpha = .85)
        if len(VNF['Date']) != 0:
            VNF['bcm_MaxRH_annual_eq'] = VNF['bcm_MaxRH_annual_eq'].apply(lambda x: x / 365 * 1000)
            ax2.scatter(VNF['Date'], VNF['bcm_MaxRH_annual_eq'],
                    label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1) 
            mean_maxrh_bcm = [VNF['bcm_MaxRH_annual_eq'].mean() for elem in np.zeros(len(date_proxy), )]
            ax2.plot(date_proxy, mean_maxrh_bcm, '--', color = 'gold', linewidth = 0.75, alpha = .85)
        plt.title('Оценка объёмов сжигания по корреляции по MaxRH и MaxFRP (~ корреляция по ежемесячным отчётностям)', fontdict = font_text_2)
        ax2.set_ylabel('Объём сжигания,\nмлн. м³ в день')
        ax2.set_xlim(datelim)
        ax2.set_ylim(bottom = 0)
        # ax2.legend(loc = 'upper left')
        
        # Sum75pct vs bcm
        ax3 = plt.subplot2grid((3, 1), (2, 0))
        if len(VAFP['Date_Conv']) != 0:
            VAFP['PctFRP_75_bcm'] = VAFP['PctFRP_75'].apply(lambda x: x * sumrh75_to_bcm / frp_to_bcm_dict['VAFP_75pct'])
            ax3.scatter(VAFP['Date_Conv'], VAFP['PctFRP_75_bcm'],
                    label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
            mean_vfrp_75pct_bcm = [VAFP['PctFRP_75_bcm'].mean() for elem in np.zeros(len(date_proxy), )]
            ax3.plot(date_proxy, mean_vfrp_75pct_bcm, '--', color = 'magenta', linewidth = 0.75, alpha = .85)
        if len(MAFP['Date_Conv']) != 0:
            MAFP['PctFRP_75_bcm'] = MAFP['PctFRP_75'].apply(lambda x: x * sumrh75_to_bcm / frp_to_bcm_dict['MAFP_75pct'])
            ax3.scatter(MAFP['Date_Conv'], MAFP['PctFRP_75_bcm'],
                    label = "MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
            mean_mfrp_75pct_bcm = [MAFP['PctFRP_75_bcm'].mean() for elem in np.zeros(len(date_proxy), )]
            ax3.plot(date_proxy, mean_mfrp_75pct_bcm, '--', color = 'navy', linewidth = 0.75, alpha = .85)
        if len(VNF['Date']) != 0:
            is_flowrate_empty = VNF['Flowrate_km3_per_day'].replace('', np.nan)
            if is_flowrate_empty.dropna().empty == True: VNF['Flowrate_km3_per_day'] = VNF['bcm_PctRH_75_annual_eq'].apply(lambda x: x * 1000)
            ax3.scatter(VNF['Date'], VNF['Flowrate_km3_per_day'].apply(lambda x: x / 1000),
                    label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1) 
            mean_75pctrh_bcm = [VNF['Flowrate_km3_per_day'].mean() / 1000 for elem in np.zeros(len(date_proxy), )]
            ax3.plot(date_proxy, mean_75pctrh_bcm, '--', color = 'gold', linewidth = 0.75, alpha = .85)
        plt.title('Оценка объёмов сжигания по корреляции по сумме >75% от MaxRH и MaxFRP за пролёт (~ экспериментальная корреляция)', fontdict = font_text_2)
        ax3.set_ylabel('Объём сжигания,\nмлн. м³ в день')
        ax3.set_xlim(datelim)
        ax3.set_ylim(bottom = 0)
        l = ax3.legend(loc = 'upper left')
        l.set_zorder(12)
        ax3.set_xlabel('Дата', fontdict = font_labels)
        # Title
        plt.text(x = 0.5, y = 0.95, s = str(field), fontdict = font_text, fontsize = 13.5, ha = "center", transform = fig.transFigure)
        plt.text(x = 0.5, y = 0.92, s = "Наблюдения за всё время", fontdict = font_text, fontsize = 9.5, ha = "center", transform = fig.transFigure)        
        plt.subplots_adjust(top = 0.87, hspace = 0.34)
        # Pass max values
        if len(VNF['Date']) != 0: 
            max_dict['VNF_max' + field] = max(VNF['bcm_MaxRH_annual_eq'])
            max_dict['VNF_75' + field] = max(VNF['bcm_PctRH_75_annual_eq'])
            max_dict['VNF_max_idx' + field] = VNF.loc[VNF['bcm_MaxRH_annual_eq'].idxmax(), 'Date']
            max_dict['VNF_75_idx' + field] = VNF.loc[VNF['bcm_PctRH_75_annual_eq'].idxmax(), 'Date']
        else:
            max_dict['VNF_max' + field] = 0
            max_dict['VNF_75' + field] = 0
            max_dict['VNF_max_idx' + field] = 0
            max_dict['VNF_75_idx' + field] = 0
        if len(VAFP['Date_Conv']) != 0: 
            max_dict['VAFP_max' + field] = max(VAFP['MaxFRP_bcm'])
            max_dict['VAFP_75' + field] = max(VAFP['PctFRP_75_bcm'])
            max_dict['VAFP_max_idx' + field] = VAFP.loc[VAFP['MaxFRP_bcm'].idxmax(), 'Date_Conv']
            max_dict['VAFP_75_idx' + field] = VAFP.loc[VAFP['PctFRP_75_bcm'].idxmax(), 'Date_Conv']
        else:
            max_dict['VAFP_max' + field] = 0
            max_dict['VAFP_75' + field] = 0
            max_dict['VAFP_max_idx' + field] = 0
            max_dict['VAFP_75_idx' + field] = 0
        if len(MAFP['Date_Conv']) != 0: 
            max_dict['MAFP_max' + field] = max(MAFP['MaxFRP_bcm'])
            max_dict['MAFP_75' + field] = max(MAFP['PctFRP_75_bcm']) 
            max_dict['MAFP_max_idx' + field] = MAFP.loc[MAFP['MaxFRP_bcm'].idxmax(), 'Date_Conv']
            max_dict['MAFP_75_idx' + field] = MAFP.loc[MAFP['PctFRP_75_bcm'].idxmax(), 'Date_Conv']
        else:
            max_dict['MAFP_max' + field] = 0
            max_dict['MAFP_75' + field] = 0
            max_dict['MAFP_max_idx' + field] = 0
            max_dict['MAFP_75_idx' + field] = 0
        # Save
        plt.savefig(path_save_dashboard + field + '/' + field + '_0_dashboard.png', bbox_inches = 'tight')
        print(field + ' all time dashboard saved successfully.')
# =============================================================================
#         # Plot: NRT (2022)
# =============================================================================
        # datelim = [datetime.date(2022, 1, 1), datetime.datetime.today()] # 1/1/2022 to now
        datelim = [pd.Timestamp('2022-1-1'), datetime.datetime.today()] # 1/1/2022 to now
        date_proxy = pd.date_range(datelim[0], datelim[1]).to_pydatetime() # mean temp line
        if len(VNF['Date']) != 0: 
            VNF_temp = VNF[(VNF['Date'] > '2022-01-01')]
            # VNF_temp['Temp_BB_2022'] = VNF_temp['Temp_BB'] 
            if len(VNF_temp['Date']) > 0: mean_temp = [VNF_temp['Temp_BB'].mean() for elem in np.zeros(len(date_proxy), )]
        fig, axes = plt.subplots(1, 3, figsize = (19, 7), dpi = 400)
        plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
        # Temp
        ax1 = plt.subplot2grid((3, 1), (0, 0))
        if len(VNF['Date']) != 0: 
            ax1.scatter(VNF_temp['Date'], VNF_temp['Temp_BB'],
                    label = "Temp, K", color = 'red', zorder = 10, **plot_kwds1)
            if len(VNF_temp['Temp_BB']) > 0: ax1.plot(date_proxy, mean_temp, '--k', linewidth = 0.75, alpha = .85)
        plt.title('Температура по VIIRS Nightfire', fontdict = font_text_2)
        ax1.set_ylabel('Температура, K')
        ax1.set_xlim(datelim)
        if len(VNF['Date']) != 0:  
            if len(VNF_temp['Temp_BB']) > 0:  
                props = dict(facecolor = 'white', edgecolor = 'none', pad = 10)
                a = (datelim[1] - datelim[0]).total_seconds() / 2 
                datelabelx = datelim[0] + datetime.timedelta(seconds = a)
                ymin, ymax = plt.gca().get_ylim()
                ylimtemp = ax1.get_ylim()
                ylimtemp = ylimtemp[0] + 0.85 * (ylimtemp[1] - ylimtemp[0])
                plt.text(x = datelabelx, y = ylimtemp, s = 'T ср. = ' + str(int(VNF['Temp_BB'].mean())) + ' K', ha = 'center', va = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp

        # # Max RH/FRP
        # ax2 = plt.subplot2grid((3, 1), (1, 0))
        # if len(VAFP['Date_Conv']) != 0: ax2.scatter(VAFP['Date_Conv'], VAFP['MaxFRP'],
        #             label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
        # if len(MAFP['Date_Conv']) != 0: ax2.scatter(MAFP['Date_Conv'], MAFP['MaxFRP'],
        #             label = " MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
        # if len(VNF['Date']) != 0: ax2.scatter(VNF['Date'], VNF['MaxRH'],
        #             label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1)    
        # plt.title('Энергия излучения (MaxRH или MaxFRP)')
        # ax2.set_ylabel('Radiative heat / \nFire radiative power, МВт')
        # ax2.set_xlim(datelim)
        # ax2.set_ylim([0, rhlim[1] * 1.1])
        # ax2.legend(loc = 'upper left')
        # MaxRH vs bcm
        ax2 = plt.subplot2grid((3, 1), (1, 0))
        if len(VAFP['Date_Conv']) != 0:
            # VAFP['MaxFRP_bcm'] = VAFP['MaxFRP'].apply(lambda x: x / frp_to_bcm_dict['VAFP_max'])
            ax2.scatter(VAFP['Date_Conv'], VAFP['MaxFRP_bcm'],
                    label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
        if len(MAFP['Date_Conv']) != 0:
            # MAFP['MaxFRP_bcm'] = MAFP['MaxFRP'].apply(lambda x: x / frp_to_bcm_dict['MAFP_max'])
            ax2.scatter(MAFP['Date_Conv'], MAFP['MaxFRP_bcm'],
                    label = "MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
        if len(VNF['Date']) != 0:
            VNF['bcm_MaxRH_annual_eq'] = VNF['bcm_MaxRH_annual_eq']
            ax2.scatter(VNF['Date'], VNF['bcm_MaxRH_annual_eq'],
                    label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1) 
        plt.title('Оценка объёмов сжигания по корреляции по MaxRH и MaxFRP (~ корреляция по ежемесячным отчётностям)', fontdict = font_text_2)
        ax2.set_ylabel('Объём сжигания,\nмлн. м³ в день')
        ax2.set_xlim(datelim)
        ax2.set_ylim(bottom = 0)
        
        # Sum75pct vs bcm
        ax3 = plt.subplot2grid((3, 1), (2, 0))
        if len(VAFP['Date_Conv']) != 0:
            # VAFP['PctFRP_75_bcm'] = VAFP['PctFRP_75'].apply(lambda x: x * frp_to_bcm_dict['VAFP_75pct'])
            ax3.scatter(VAFP['Date_Conv'], VAFP['PctFRP_75_bcm'],
                    label = "VIIRS Active Fire Product", color = 'magenta', zorder = 10, **plot_kwds1)
        if len(MAFP['Date_Conv']) != 0:
            # MAFP['PctFRP_75_bcm'] = MAFP['PctFRP_75'].apply(lambda x: x * frp_to_bcm_dict['MAFP_75pct'])
            ax3.scatter(MAFP['Date_Conv'], MAFP['PctFRP_75_bcm'],
                    label = "MODIS Active Fire Product", color = 'navy', zorder = 10, **plot_kwds1)
        if len(VNF['Date']) != 0:
            is_flowrate_empty = VNF['Flowrate_km3_per_day'].replace('', np.nan)
            if is_flowrate_empty.dropna().empty == True: VNF['Flowrate_km3_per_day'] = VNF['bcm_PctRH_75_annual_eq'].apply(lambda x: x * 1000)
            ax3.scatter(VNF['Date'], VNF['Flowrate_km3_per_day'].apply(lambda x: x / 1000),
                    label = "VIIRS Nightfire", color = 'gold', zorder = 10, **plot_kwds1) 
        plt.title('Оценка объёмов сжигания по корреляции по сумме >75% от MaxRH и MaxFRP за пролёт (~ экспериментальная корреляция)', fontdict = font_text_2)
        ax3.set_ylabel('Объём сжигания\nмлн. м³ в день')
        ax3.set_xlim(datelim)
        ax3.set_ylim(bottom = 0)
        ax3.legend(loc = 'upper left')
        ax3.set_xlabel('Дата', fontdict = font_labels)
        # Title
        plt.text(x = 0.5, y = 0.95, s = str(field), fontsize = 14, ha = "center", transform = fig.transFigure)
        plt.text(x = 0.5, y = 0.92, s = "Наблюдения за 2022 г.", fontsize = 9.5, ha = "center", transform = fig.transFigure)        
        plt.subplots_adjust(top = 0.87, hspace = 0.34)
        # Pass max values
        VNF_temp = VNF[(VNF['Date'] > '2022-01-01')]
        VAFP_temp = VAFP[(VAFP['Date'] > '2022-01-01')]
        MAFP_temp = MAFP[(MAFP['Date'] > '2022-01-01')]
        
        if len(VNF_temp['Date']) != 0: 
            max_dict['VNF_max_2022' + field] = max(VNF_temp['bcm_MaxRH_annual_eq'])
            max_dict['VNF_75_2022' + field] = max(VNF_temp['bcm_PctRH_75_annual_eq'])
            max_dict['VNF_max_idx_2022' + field] = VNF_temp.loc[VNF_temp['bcm_MaxRH_annual_eq'].idxmax(), 'Date']
            max_dict['VNF_75_idx_2022' + field] = VNF_temp.loc[VNF_temp['bcm_PctRH_75_annual_eq'].idxmax(), 'Date']
        else:
            max_dict['VNF_max_2022' + field] = 0
            max_dict['VNF_75_2022' + field] = 0
            max_dict['VNF_max_idx_2022' + field] = 0
            max_dict['VNF_75_idx_2022' + field] = 0
        if len(VAFP_temp['Date_Conv']) != 0: 
            max_dict['VAFP_max_2022' + field] = max(VAFP_temp['MaxFRP_bcm'])
            max_dict['VAFP_75_2022' + field] = max(VAFP_temp['PctFRP_75_bcm'])
            max_dict['VAFP_max_idx_2022' + field] = VAFP_temp.loc[VAFP_temp['MaxFRP_bcm'].idxmax(), 'Date']
            max_dict['VAFP_75_idx_2022' + field] = VAFP_temp.loc[VAFP_temp['PctFRP_75_bcm'].idxmax(), 'Date']
        else:
            max_dict['VAFP_max_2022' + field] = 0
            max_dict['VAFP_75_2022' + field] = 0
            max_dict['VAFP_max_idx_2022' + field] = 0
            max_dict['VAFP_75_idx_2022' + field] = 0
        if len(MAFP_temp['Date_Conv']) != 0: 
            max_dict['MAFP_max_2022' + field] = max(MAFP_temp['MaxFRP_bcm'])
            max_dict['MAFP_75_2022' + field] = max(MAFP_temp['PctFRP_75_bcm']) 
            max_dict['MAFP_max_idx_2022' + field] = MAFP_temp.loc[MAFP_temp['MaxFRP_bcm'].idxmax(), 'Date']
            max_dict['MAFP_75_idx_2022' + field] = MAFP_temp.loc[MAFP_temp['PctFRP_75_bcm'].idxmax(), 'Date']
        else:
            max_dict['MAFP_max_2022' + field] = 0
            max_dict['MAFP_75_2022' + field] = 0
            max_dict['MAFP_max_idx' + field] = 0
            max_dict['MAFP_75_idx' + field] = 0
        # Save
        plt.savefig(path_save_dashboard + field + '/' + field + '_0_dashboard_NRT.png', bbox_inches = 'tight')
        print(field + ' NRT dashboard saved successfully.')
    except:
        print('\n\nError with dashboard for ' + field + '.\n\n')

# Field table
def field_tables(fields, field, frp_to_bcm_dict, max_dict):
    name = multiprocessing.current_process().name
    try: 
        novnf = 0
        VNF = fields['VNF'].query("Field == @field")
        VAFP = fields['VIIRS AFP'].query("Field == @field")
        MAFP = fields['MODIS AFP'].query("Field == @field")
        if VNF.empty: 
            novnf = 1
            print('Note:', field, 'has no VNF dataset.')
        # Max → bcm
        if novnf == 0: VNF['MaxRH_mcm_d'] = VNF['bcm_MaxRH_annual_eq'].apply(lambda x: x / 365 * 1000)
        VAFP['MaxFRP_mcm_d'] = VAFP['MaxFRP'].apply(lambda x: x * maxrh_to_bcm / frp_to_bcm_dict['VAFP_max'])
        MAFP['MaxFRP_mcm_d'] = MAFP['MaxFRP'].apply(lambda x: x * maxrh_to_bcm / frp_to_bcm_dict['MAFP_max'])
        # Pct75 → bcm
        print(field, '1')
        is_flowrate_empty = VNF['Flowrate_km3_per_day'].replace('', np.nan)
        if is_flowrate_empty.dropna().empty == True: VNF['Flowrate_km3_per_day'] = VNF['bcm_PctRH_75_annual_eq'].apply(lambda x: x * 1000)
        VNF['Pct75_RH_mcm_d'] = VNF['Flowrate_km3_per_day'].apply(lambda x: x / 1000)
        print(field, '2')
        VAFP['PctFRP_75_mcm_d'] = VAFP['PctFRP_75'].apply(lambda x: x * sumrh75_to_bcm / frp_to_bcm_dict['VAFP_75pct'])
        MAFP['PctFRP_75_mcm_d'] = MAFP['PctFRP_75'].apply(lambda x: x * sumrh75_to_bcm / frp_to_bcm_dict['MAFP_75pct'])
        # Pre-process
        if novnf == 0: VNF = VNF.rename({'Date': 'Date_VNF_init', 'Date Day': 'Date'}, axis = 1)
        if novnf == 0: VNF = VNF.drop(['bcm_MaxRH_annual_eq', 'bcm_PctRH_75_annual_eq'], axis = 1) 
        if novnf == 0: VNF['Date Time'] = pd.to_datetime(VNF['Date'] + ' ' + VNF['Local Time'])
        VAFP['Date Time'] = pd.to_datetime(VAFP['Date'] + ' ' + VAFP['Local Time'])
        MAFP['Date Time'] = pd.to_datetime(MAFP['Date'] + ' ' + MAFP['Local Time'])
        # Merge
        if novnf == 0: df_merge = merge_and_correct_times(VNF, VAFP, '_VNF', '_VAFP')
        else: df_merge = VAFP
        df_merge = merge_and_correct_times(df_merge, MAFP, '', '_MAFP')
        
        df_merge.reset_index(inplace = True, drop = True) # reset index
        df_merge.index += 1 # shift index by +1
        df_merge.index.name = 'index'
        # Buildup
        df_merge['MaxRH_mean_mcm_d'] = 0
        df_merge['Pct75_RH_mean_mcm_d'] = 0
        df_merge['Main Dataset'] = np.nan
        if novnf == 0: df_merge = df_merge.rename({'Date_VNF' : 'Date Day', 'Local Time': 'Local Time_MAFP', 'Local Time_VNF': 'Local Time'}, axis = 1)
        else: df_merge = df_merge.rename({'Date' : 'Date Day'}, axis = 1)
        cm = 0
        c75 = 0
        for (i, row) in df_merge.iterrows(): 
            if novnf == 0: 
                if pd.isnull(df_merge.loc[i, 'Date Day']) == True:
                    # if no VNF, but VAFP
                    if pd.isnull(df_merge.loc[i, 'Date_VAFP']) == False:
                        df_merge.loc[i, 'Lat_VNF'] = df_merge.loc[i, 'Lat_VAFP']
                        df_merge.loc[i, 'Lon_VNF'] = df_merge.loc[i, 'Lon_VAFP']
                        df_merge.loc[i, 'Date Day'] = df_merge.loc[i, 'Date_VAFP']
                        df_merge.loc[i, 'Local Time'] = df_merge.loc[i, 'Local Time_VAFP']
                        df_merge.loc[i, 'Main Dataset'] = 'VIIRS AFP'
                    # MAFP
                    else:
                        df_merge.loc[i, 'Lat_VNF'] = df_merge.loc[i, 'Lat']
                        df_merge.loc[i, 'Lon_VNF'] = df_merge.loc[i, 'Lon']
                        df_merge.loc[i, 'Date Day'] = df_merge.loc[i, 'Date']
                        df_merge.loc[i, 'Local Time'] = df_merge.loc[i, 'Local Time_MAFP']
                        df_merge.loc[i, 'Main Dataset'] = 'MODIS AFP'
                        df_merge.loc[i, 'daynight'] = df_merge.loc[i, 'daynight_MAFP']
                if pd.isnull(df_merge.loc[i, 'Main Dataset']) == True: df_merge.loc[i, 'Main Dataset'] = 'VNF'
                # VNF
                if pd.isnull(df_merge.loc[i, 'MaxRH_mcm_d']) == False: 
                    df_merge.loc[i, 'MaxRH_mean_mcm_d'] += df_merge.loc[i, 'MaxRH_mcm_d']
                    cm += 1
                if pd.isnull(df_merge.loc[i, 'Pct75_RH_mcm_d']) == False: 
                    df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] += df_merge.loc[i, 'Pct75_RH_mcm_d']    
                    c75 += 1
                # VAFP
                if pd.isnull(df_merge.loc[i, 'MaxFRP_mcm_d']) == False: 
                    df_merge.loc[i, 'MaxRH_mean_mcm_d'] += df_merge.loc[i, 'MaxFRP_mcm_d']
                    cm += 1
                if pd.isnull(df_merge.loc[i, 'PctFRP_75_mcm_d']) == False: 
                    df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] += df_merge.loc[i, 'PctFRP_75_mcm_d']
                    c75 += 1
                # MAFP
                if pd.isnull(df_merge.loc[i, 'MaxFRP_mcm_d_MAFP']) == False:
                    df_merge.loc[i, 'MaxRH_mean_mcm_d'] += df_merge.loc[i, 'MaxFRP_mcm_d_MAFP']
                    cm += 1
                if pd.isnull(df_merge.loc[i, 'PctFRP_75_mcm_d_MAFP']) == False:
                    df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] += df_merge.loc[i, 'PctFRP_75_mcm_d_MAFP']
                    c75 += 1
            # if only AFP is available
            else:
                if pd.isnull(df_merge.loc[i, 'Date Day']) == True:
                    df_merge.loc[i, 'Lat'] = df_merge.loc[i, 'Lat_MAFP']
                    df_merge.loc[i, 'Lon'] = df_merge.loc[i, 'Lon_MAFP']
                    df_merge.loc[i, 'Date Day'] = df_merge.loc[i, 'Date_MAFP']
                    df_merge.loc[i, 'Local Time'] = df_merge.loc[i, 'Local Time_MAFP']
                    df_merge.loc[i, 'Main Dataset'] = 'MODIS AFP'
                    df_merge.loc[i, 'daynight'] = df_merge.loc[i, 'daynight_MAFP']
                else: df_merge.loc[i, 'Main Dataset'] = 'VIIRS AFP'
                # VAFP
                if pd.isnull(df_merge.loc[i, 'MaxFRP_mcm_d']) == False: 
                    df_merge.loc[i, 'MaxRH_mean_mcm_d'] += df_merge.loc[i, 'MaxFRP_mcm_d']
                    cm += 1
                if pd.isnull(df_merge.loc[i, 'PctFRP_75_mcm_d']) == False: 
                    df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] += df_merge.loc[i, 'PctFRP_75_mcm_d']
                    c75 += 1
                # MAFP
                if pd.isnull(df_merge.loc[i, 'MaxFRP_mcm_d_MAFP']) == False:
                    df_merge.loc[i, 'MaxRH_mean_mcm_d'] += df_merge.loc[i, 'MaxFRP_mcm_d_MAFP']
                    cm += 1
                if pd.isnull(df_merge.loc[i, 'PctFRP_75_mcm_d_MAFP']) == False:
                    df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] += df_merge.loc[i, 'PctFRP_75_mcm_d_MAFP']
                    c75 += 1
            # Sum
            if cm == 0: cm = 1
            if c75 == 0: c75 = 1
            df_merge.loc[i, 'MaxRH_mean_mcm_d'] = df_merge.loc[i, 'MaxRH_mean_mcm_d'] / cm
            df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] = df_merge.loc[i, 'Pct75_RH_mean_mcm_d'] / c75
            cm = 0
            c75 = 0
        if novnf == 0: 
        # VNF   
            df_merge = df_merge.drop(['Date_VNF_init', 'N_Bowtie_Scans', 'Ndtct', 
                                      'Npct_75_VNF', 'Npct_50_VNF', 'PctRH_50', 'Npct_25_VNF', 'PctRH_25', 'Nfit',
                                      'Lat_VAFP', 'Lon_VAFP', 'Date_VAFP', 'Local Time_VAFP', 'instrument', 'PctFRP_50',
                                      'PctFRP_25', 'Npct_75_VAFP', 'Npct_50_VAFP', 'Npct_25_VAFP', 'Ndtct_samescan',
                                      'scan', 'track', 'Lat', 'Lon', 'Date', 'Local Time_MAFP', 'daynight_MAFP', 
                                      'instrument_MAFP', 'PctFRP_50_MAFP', 'PctFRP_25_MAFP', 'Npct_75', 'Npct_50', 'Npct_25',
                                      'Ndtct_samescan_MAFP', 'scan_MAFP', 'track_MAFP'], axis = 1)
            df_merge = df_merge.rename({'Lat_VNF' : 'Lat', 'Lon_VNF': 'Lon', 'Satellite': 'Satellite_VAFP',
                                        'SumFRP': 'SumFRP_VAFP', 'PctFRP_75': 'PctFRP_75_VAFP', 'MaxFRP': 'MaxFRP_VAFP', 
                                        'MaxFRP_mcm_d': 'MaxFRP_mcm_d_VAFP', 'PctFRP_75_mcm_d': 'PctFRP_75_mcm_d_VAFP', 'daynight': 'daynight_AFP'}, axis = 1)
        # VAFP
        else:
            df_merge = df_merge.drop(['instrument', 'PctFRP_50', 'PctFRP_25', 'Npct_75', 'Npct_50', 'Npct_25', 'Ndtct_samescan',
                                      'scan', 'track', 'Lat_MAFP', 'Lon_MAFP', 'Date_MAFP', 'Local Time_MAFP', 'daynight_MAFP',
                                      'instrument_MAFP', 'PctFRP_50_MAFP', 'PctFRP_25_MAFP', 'Npct_75_MAFP', 'Npct_50_MAFP',
                                      'Npct_25_MAFP', 'Ndtct_samescan_MAFP', 'scan_MAFP', 'track_MAFP'], axis = 1)
            df_merge = df_merge.rename({'Satellite': 'Satellite_VAFP', 'SumFRP': 'SumFRP_VAFP', 'MaxFRP': 'MaxFRP_VAFP',
                                        'PctFRP_75': 'PctFRP_75_VAFP', 'MaxFRP_mcm_d': 'MaxFRP_mcm_d_VAFP', 
                                        'PctFRP_75_mcm_d': 'PctFRP_75_mcm_d_VAFP'}, axis = 1)
        if novnf == 0:
            df_merge['Date Day'] = pd.to_datetime(df_merge['Date Day'])
            df_merge['Date Day'] = [x + '/' + y + '/' + z for x, y, z in zip(df_merge['Date Day'].dt.month.astype('str'), df_merge['Date Day'].dt.day.astype('str'), df_merge['Date Day'].dt.year.astype('str'))]

        # Daily
        if novnf == 0: df_daily_merge = df_merge.groupby(['Date Day'], as_index = False).agg({'Field': 'first', 'Lat': 'first', 'Lon': 'first', 
                                                                               'SumRH': 'mean', 'SumFRP_VAFP': 'mean', 'SumFRP_MAFP': 'mean',
                                                                               'MaxRH': 'mean', 'MaxFRP_VAFP': 'mean', 'MaxFRP_MAFP': 'mean', 
                                                                               'PctRH_75': 'mean', 'PctFRP_75_VAFP': 'mean', 'PctFRP_75_MAFP': 'mean', 
                                                                               'MaxRH_mcm_d': 'mean', 'Pct75_RH_mcm_d': 'mean',
                                                                               'MaxFRP_mcm_d_VAFP': 'mean', 'PctFRP_75_mcm_d_VAFP': 'mean',
                                                                               'MaxFRP_mcm_d_MAFP': 'mean', 'PctFRP_75_mcm_d_MAFP': 'mean',
                                                                               'MaxRH_mean_mcm_d': 'mean', 'Pct75_RH_mean_mcm_d': 'mean'}).reset_index()
        else: df_daily_merge = df_merge.groupby(['Date Day'], as_index = False).agg({'Field': 'first', 'Lat': 'first', 'Lon': 'first', 
                                                                               'SumFRP_VAFP': 'mean', 'SumFRP_MAFP': 'mean',
                                                                               'MaxFRP_VAFP': 'mean', 'MaxFRP_MAFP': 'mean',
                                                                               'PctFRP_75_VAFP': 'mean', 'PctFRP_75_MAFP': 'mean', 
                                                                               'MaxFRP_mcm_d_VAFP': 'mean', 'PctFRP_75_mcm_d_VAFP': 'mean',
                                                                               'MaxFRP_mcm_d_MAFP': 'mean', 'PctFRP_75_mcm_d_MAFP': 'mean',
                                                                               'MaxRH_mean_mcm_d': 'mean', 'Pct75_RH_mean_mcm_d': 'mean'}).reset_index()    
        df_daily_merge['Date Day'] = pd.to_datetime(df_daily_merge['Date Day'])
        df_daily_merge = df_daily_merge.sort_values(by = ['Date Day'])
        df_daily_merge.reset_index(inplace = True, drop = True) # reset index
        df_daily_merge.index += 1 # shift index by +1
        df_daily_merge.index.name = 'index'
        df_daily_merge = df_daily_merge.drop(['index'], axis = 1)

        # Save tables
        df_daily_merge.to_csv(path_save_dashboard + field + '/' + field + '_1_Detections Daily Means.csv')
        df_merge.to_csv(path_save_dashboard + field + '/' + field + '_1_Detections All.csv')
        print(field + ' table saved successfully.')
        
        # =============================================================================
        #         Graphs
        # =============================================================================
        # maxRH
        datelim = [min(df_daily_merge['Date Day']), max(df_daily_merge['Date Day'])]
        date_proxy = pd.date_range(datelim[0], datelim[1]).to_pydatetime() # mean temp line 
        mean_max_rh = [df_daily_merge['MaxRH_mean_mcm_d'].mean() for elem in np.zeros(len(date_proxy), )]
        fig, ax = plt.subplots(figsize = (19, 7), dpi = 400)
        plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
        if novnf == 0: ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxRH_mcm_d'],
                    label = "VNF", color = 'gold', zorder = 10, **plot_kwds1)
        ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxFRP_mcm_d_VAFP'],
                    label = "VIIRS AFP", color = 'magenta', zorder = 10, **plot_kwds1)
        ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxFRP_mcm_d_MAFP'],
                    label = "MODIS AFP", color = 'navy', zorder = 10, **plot_kwds1)
        ax.plot(df_daily_merge['Date Day'], df_daily_merge['MaxRH_mean_mcm_d'], linestyle = 'dotted', color = 'dimgray', linewidth = 0.75,
                    alpha = .85, label = "Среднее", zorder = 11)
        ax.plot(date_proxy, mean_max_rh, linestyle = 'dashed', color = 'black', linewidth = 0.75, alpha = .85)
        props = dict(facecolor = 'white', edgecolor = 'none', pad = 10)
        a = (datelim[1] - datelim[0]).total_seconds() / 2
        datelabelx = datelim[0] + datetime.timedelta(seconds = a)
        ylimtemp = ax.get_ylim()
        ylimtemp = ylimtemp[0] + 0.89 * (ylimtemp[1] - ylimtemp[0])
        ax.set_ylim(bottom = 0)
        # ax.set_xlim(datelim[0], datelim[1])
        xt = ax.get_xticks() # add latest date to graph
        ds = mktime(datelim[0].timetuple()) / 60 / 60 / 24 + 3 / 24 # first day
        de = mktime(datelim[1].timetuple()) / 60 / 60 / 24 + 3 / 24 # last day
        xt = np.append(xt, de)
        xt = np.append(xt, ds)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b-%Y'))
        ax.set_xticks(xt)
        ax.set_xlim(ds, de)
        plt.legend(loc = 'upper left')
        # Max/mean bcm observed
        mean_mcm = r"$\bf{" + str(round(float(df_daily_merge['MaxRH_mean_mcm_d'].mean()), 2)) + "}$"
        mean_bcm = r"$\bf{" + str(round(df_daily_merge['MaxRH_mean_mcm_d'].mean() * 365 / 1000, 3)) + "}$"
        sum_mcm = r"$\bf{" + str(round(df_daily_merge['MaxRH_mean_mcm_d'].sum(), 1)) + "}$"
        m_bcm = [max_dict['VNF_max' + field], max_dict['VAFP_max' + field], max_dict['MAFP_max' + field]]
        max_bcm = max(m_bcm)
        max_dataset = m_bcm.index(max_bcm) # which dataset max is from
        if max_dataset == 0: 
            max_dataset = 'VNF'
            max_datetime = str(max_dict['VNF_max_idx' + field])
            max_datetime = max_datetime[:max_datetime.find('.')]
        elif max_dataset == 1: 
            max_dataset = 'VIIRS AFP'
            max_datetime = max_dict['VAFP_max_idx' + field]
        elif max_dataset == 2: 
            max_dataset = 'MODIS AFP'
            max_datetime = max_dict['MAFP_max_idx' + field] 
        max_datetime = str(max_datetime)
        max_bcm = r"$\bf{" + str(round(max_bcm, 1)) + "}$"
        days_obs = df_daily_merge['Date Day'].count()
        if days_obs == 1 or days_obs % 10 == 1: days_name = "день"
        elif days_obs in (2, 3, 4) or days_obs % 10 in (2, 3, 4): days_name = "дня"
        else: days_name = "дней"
        if days_obs in (11, 12, 13, 14, 15, 16, 17, 18, 19): days_name = "дней"
        plt.text(x = datelabelx, y = ylimtemp, fontdict = font_labels, s = 'V ср. = ' + mean_mcm + ' млн. м³ в день, или ' + mean_bcm + ' млрд. м³ в год\nВсего сожжено за период наблюдения (' + str(days_obs) + ' ' + days_name + '): ' + sum_mcm + ' млн. м³\nМаксимальное сжигание за пролёт: экв. ' + max_bcm + ' млн. м³ в день (зафиксировано ' + max_dataset + ', время: ' + max_datetime + ')', ha = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp
        ax.set_xlabel('Дата', fontdict = font_labels_2)
        ax.set_ylabel('Объёмы сжигания, млн. м³', fontdict = font_labels_2)
        # Title
        plt.text(x = 0.5, y = 0.95, s = str(field), fontsize = 13.5, ha = "center", transform = fig.transFigure)
        plt.text(x = 0.5, y = 0.92, s = "Усреднённые за день объёмы сжигания по " + r"$\bf{" + "MaxRH" + "}$" + " (~ корреляция по отчётностям)", fontsize = 10.5, ha = "center", transform = fig.transFigure)        
        plt.subplots_adjust(top = 0.91, hspace = 0.34)
        plt.savefig(path_save_dashboard + field + '/' + field + '_2_daily mean_MaxRH_graph.png', bbox_inches = 'tight')
        print(field + ' only MaxRH graph saved successfully.')
     
        # PctRH_75
        mean_75_rh = [df_daily_merge['Pct75_RH_mean_mcm_d'].mean() for elem in np.zeros(len(date_proxy), )]
        fig, ax = plt.subplots(figsize = (19, 7), dpi = 400)
        plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
        if novnf == 0: ax.scatter(df_daily_merge['Date Day'], df_daily_merge['Pct75_RH_mcm_d'],
                    label = "VNF", color = 'gold', zorder = 10, **plot_kwds1)
        ax.scatter(df_daily_merge['Date Day'], df_daily_merge['PctFRP_75_mcm_d_VAFP'],
                    label = "VIIRS AFP", color = 'magenta', zorder = 10, **plot_kwds1)
        ax.scatter(df_daily_merge['Date Day'], df_daily_merge['PctFRP_75_mcm_d_MAFP'],
                    label = "MODIS AFP", color = 'navy', zorder = 10, **plot_kwds1)
        ax.plot(df_daily_merge['Date Day'], df_daily_merge['Pct75_RH_mean_mcm_d'], linestyle = 'dotted', color = 'dimgray', linewidth = 0.75,
                    alpha = .85, label = "Среднее", zorder = 11)
        ax.plot(date_proxy, mean_75_rh, linestyle = 'dashed', color = 'black', linewidth = 0.75, alpha = .85)
        # Max/means
        mean_mcm = r"$\bf{" + str(round(float(df_daily_merge['Pct75_RH_mean_mcm_d'].mean()), 2)) + "}$"
        mean_bcm = r"$\bf{" + str(round(df_daily_merge['Pct75_RH_mean_mcm_d'].mean() * 365 / 1000, 3)) + "}$"
        sum_mcm = r"$\bf{" + str(round(df_daily_merge['Pct75_RH_mean_mcm_d'].sum(), 1)) + "}$"
        m_bcm = [max_dict['VNF_75' + field], max_dict['VAFP_75' + field], max_dict['MAFP_75' + field]]
        max_bcm = max(m_bcm)
        max_dataset = m_bcm.index(max_bcm) # which dataset max is from
        if max_dataset == 0: 
            max_dataset = 'VNF'
            max_datetime = str(max_dict['VNF_75_idx' + field])
            max_datetime = max_datetime[:max_datetime.find('.')]
        elif max_dataset == 1: 
            max_dataset = 'VIIRS AFP'
            max_datetime = max_dict['VAFP_75_idx' + field]
        elif max_dataset == 2: 
            max_dataset = 'MODIS AFP'
            max_datetime = max_dict['MAFP_75_idx' + field]
        max_datetime = str(max_datetime)
        max_bcm = r"$\bf{" + str(round(max_bcm, 1)) + "}$"
        # back to graph
        ax.set_ylim(bottom = 0)
        ax.set_xlim(left = datelim[0], right = datelim[1])
        ylimtemp = ax.get_ylim()
        ylimtemp = ylimtemp[0] + 0.89 * (ylimtemp[1] - ylimtemp[0])
        plt.text(x = datelabelx, y = ylimtemp, fontdict = font_labels, s = 'V ср. = ' + mean_mcm + ' млн. м³ в день, или ' + mean_bcm + ' млрд. м³ в год\nВсего сожжено за период наблюдения (' + str(days_obs) + ' ' + days_name + '): ' + sum_mcm + ' млн. м³\nМаксимальное сжигание за пролёт: экв. ' + max_bcm + ' млн. м³ в день (зафиксировано ' + max_dataset + ', время: ' + max_datetime + ')', ha = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp
        ax.set_xlabel('Дата', fontdict = font_labels_2)
        ax.set_ylabel('Объёмы сжигания, млн. м³', fontdict = font_labels_2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b-%Y'))
        ax.set_xticks(xt)
        ax.set_xlim(ds, de)
        plt.legend(loc = 'upper left')
        # Title
        plt.text(x = 0.5, y = 0.95, s = str(field), fontsize = 13.5, ha = "center", transform = fig.transFigure)
        plt.text(x = 0.5, y = 0.92, s = "Усреднённые за день объёмы сжигания по " + r"$\bf{" + "сумме >75" + "}$" + "% (~ экспериментальная корреляция)", fontsize = 10.5, ha = "center", transform = fig.transFigure)        
        plt.subplots_adjust(top = 0.91, hspace = 0.34)
        plt.savefig(path_save_dashboard + field + '/' + field + '_2_daily mean_Pct75_RH_graph.png', bbox_inches = 'tight')
        print(field + ' only Pct75RH graph saved successfully.')

        # MaxRH_2022
        datelim = [pd.Timestamp('2022-1-1'), max(df_daily_merge['Date Day'])]
        date_proxy = pd.date_range(datelim[0], datelim[1]).to_pydatetime() # mean temp line 
        df_daily_merge = df_daily_merge[(df_daily_merge['Date Day'] > '2022-01-01')]
        if len(df_daily_merge) != 0:
            mean_max_rh_2022 = [df_daily_merge['MaxRH_mean_mcm_d'].mean() for elem in np.zeros(len(date_proxy), )]
            fig, ax = plt.subplots(figsize = (19, 7), dpi = 400)
            plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
            if novnf == 0: ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxRH_mcm_d'],
                        label = "VNF", color = 'gold', zorder = 10, **plot_kwds1)
            ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxFRP_mcm_d_VAFP'],
                        label = "VIIRS AFP", color = 'magenta', zorder = 10, **plot_kwds1)
            ax.scatter(df_daily_merge['Date Day'], df_daily_merge['MaxFRP_mcm_d_MAFP'],
                        label = "MODIS AFP", color = 'navy', zorder = 10, **plot_kwds1)
            ax.plot(df_daily_merge['Date Day'], df_daily_merge['MaxRH_mean_mcm_d'], linestyle = 'dotted', color = 'dimgray', linewidth = 0.75,
                        alpha = .85, label = "Среднее", zorder = 11)
            ax.plot(date_proxy, mean_max_rh_2022, linestyle = 'dashed', color = 'black', linewidth = 0.75, alpha = .85)
            props = dict(facecolor = 'white', edgecolor = 'none', pad = 10)
            a = (datelim[1] - datelim[0]).total_seconds() / 2
            datelabelx = datelim[0] + datetime.timedelta(seconds = a)
            ylimtemp = ax.get_ylim()
            ylimtemp = ylimtemp[0] + 0.89 * (ylimtemp[1] - ylimtemp[0])
            ax.set_ylim(bottom = 0)
            ax.set_xlim(left = datelim[0], right = datelim[1])
            xt = ax.get_xticks() # add latest date to graph
            xt = xt[:-1] # remove the last element
            ds = mktime(datelim[0].timetuple()) / 60 / 60 / 24 + 3 / 24 # first day
            de = mktime(datelim[1].timetuple()) / 60 / 60 / 24 + 3 / 24 # last day
            xt = np.append(xt, de)
            xt = np.append(xt, ds)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b-%Y'))
            ax.set_xticks(xt)
            plt.legend(loc = 'upper left')
            # Max/means
            mean_mcm = r"$\bf{" + str(round(float(df_daily_merge['MaxRH_mean_mcm_d'].mean()), 2)) + "}$"
            mean_bcm = r"$\bf{" + str(round(df_daily_merge['MaxRH_mean_mcm_d'].mean() * 365 / 1000, 3)) + "}$"
            sum_mcm = r"$\bf{" + str(round(df_daily_merge['MaxRH_mean_mcm_d'].sum(), 1)) + "}$"
            m_bcm = [max_dict['VNF_max_2022' + field], max_dict['VAFP_max_2022' + field], max_dict['MAFP_max_2022' + field]]
            max_bcm = max(m_bcm)
            max_dataset = m_bcm.index(max_bcm) # which dataset max is from
            if max_dataset == 0: 
                max_dataset = 'VNF'
                max_datetime = str(max_dict['VNF_max_idx_2022' + field])
                max_datetime = max_datetime[:max_datetime.find('.')]
            elif max_dataset == 1:
                max_dataset = 'VIIRS AFP'
                max_datetime = max_dict['VAFP_max_idx_2022' + field]
            elif max_dataset == 2:
                max_dataset = 'MODIS AFP'
                max_datetime = max_dict['MAFP_max_idx_2022' + field]
            if max_bcm > 0: max_bcm = r"$\bf{" + str(round(max_bcm, 1)) + "}$"
            else: 
                max_bcm = r"$\bf{" + '–' + "}$"
                max_dataset = '–'
                max_datetime = '–'
            max_datetime = str(max_datetime)
            # back to graph
            days_obs = df_daily_merge['Date Day'].count()
            if days_obs == 1 or days_obs % 10 == 1: days_name = "день"
            elif days_obs in (2, 3, 4) or days_obs % 10 in (2, 3, 4): days_name = "дня"
            else: days_name = "дней"
            if days_obs in (11, 12, 13, 14, 15, 16, 17, 18, 19): days_name = "дней"
            plt.text(x = datelabelx, y = ylimtemp, fontdict = font_labels, s = 'V ср. = ' + mean_mcm + ' млн. м³ в день, или ' + mean_bcm + ' млрд. м³ в год\nВсего сожжено за период наблюдения (' + str(days_obs) + ' ' + days_name + '): ' + sum_mcm + ' млн. м³\nМаксимальное сжигание за пролёт в 2022 г.: экв. ' + max_bcm + ' млн. м³ в день (зафиксировано ' + max_dataset + ', дата: ' + max_datetime + ')', ha = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp
            ax.set_xlabel('Дата', fontdict = font_labels_2)
            ax.set_ylabel('Объёмы сжигания, млн. м³', fontdict = font_labels_2)
            # Title
            plt.text(x = 0.5, y = 0.95, s = str(field) + ' (только 2022 г.)', fontsize = 13.5, ha = "center", transform = fig.transFigure)
            plt.text(x = 0.5, y = 0.92, s = "Усреднённые за день объёмы сжигания по " + r"$\bf{" + "MaxRH" + "}$" + " (~ корреляция по отчётностям)", fontsize = 10.5, ha = "center", transform = fig.transFigure)        
            plt.subplots_adjust(top = 0.91, hspace = 0.34)
            plt.savefig(path_save_dashboard + field + '/' + field + '_3_daily mean_MaxRH_graph_2022.png', bbox_inches = 'tight')
            print(field + ' 2022 only MaxRH graph saved successfully.')
    
            # PctRH_75_2022
            mean_75_rh_2022 = [df_daily_merge['Pct75_RH_mean_mcm_d'].mean() for elem in np.zeros(len(date_proxy), )]
            fig, ax = plt.subplots(figsize = (19, 7), dpi = 400)
            plot_kwds1 = {'alpha' : 0.3, 's' : 64, 'linewidths' : 0}
            if novnf == 0: ax.scatter(df_daily_merge['Date Day'], df_daily_merge['Pct75_RH_mcm_d'],
                        label = "VNF", color = 'gold', zorder = 10, **plot_kwds1)
            ax.scatter(df_daily_merge['Date Day'], df_daily_merge['PctFRP_75_mcm_d_VAFP'],
                        label = "VIIRS AFP", color = 'magenta', zorder = 10, **plot_kwds1)
            ax.scatter(df_daily_merge['Date Day'], df_daily_merge['PctFRP_75_mcm_d_MAFP'],
                        label = "MODIS AFP", color = 'navy', zorder = 10, **plot_kwds1)
            ax.plot(df_daily_merge['Date Day'], df_daily_merge['Pct75_RH_mean_mcm_d'], linestyle = 'dotted', color = 'dimgray', linewidth = 0.75,
                        alpha = .85, label = "Среднее", zorder = 11)
            ax.plot(date_proxy, mean_75_rh_2022, linestyle = 'dashed', color = 'black', linewidth = 0.75, alpha = .85)
            # Max/means
            mean_mcm = r"$\bf{" + str(round(float(df_daily_merge['Pct75_RH_mean_mcm_d'].mean()), 2)) + "}$"
            mean_bcm = r"$\bf{" + str(round(df_daily_merge['Pct75_RH_mean_mcm_d'].mean() * 365 / 1000, 3)) + "}$"
            sum_mcm = r"$\bf{" + str(round(df_daily_merge['Pct75_RH_mean_mcm_d'].sum(), 1)) + "}$"
            m_bcm = [max_dict['VNF_75_2022' + field], max_dict['VAFP_75_2022' + field], max_dict['MAFP_75_2022' + field]]
            max_bcm = max(m_bcm)
            max_dataset = m_bcm.index(max_bcm) # which dataset max is from
            if max_dataset == 0:
                max_dataset = 'VNF'
                max_datetime = str(max_dict['VNF_75_idx_2022' + field])
                max_datetime = max_datetime[:max_datetime.find('.')]
            elif max_dataset == 1:
                max_dataset = 'VIIRS AFP'
                max_datetime = max_dict['VAFP_75_idx_2022' + field]
            elif max_dataset == 2:
                max_dataset = 'MODIS AFP'
                max_datetime = max_dict['MAFP_75_idx_2022' + field]
            if max_bcm > 0: max_bcm = r"$\bf{" + str(round(max_bcm, 1)) + "}$"
            else: 
                max_bcm = r"$\bf{" + '–' + "}$"
                max_dataset = '–'
                max_datetime = '–'
            max_datetime = str(max_datetime)
            # back to graph
            ax.set_ylim(bottom = 0)
            ax.set_xlim(left = datelim[0], right = datelim[1])
            ylimtemp = ax.get_ylim()
            ylimtemp = ylimtemp[0] + 0.89 * (ylimtemp[1] - ylimtemp[0])
            plt.text(x = datelabelx, y = ylimtemp, fontdict = font_labels, s = 'V ср. = ' + mean_mcm + ' млн. м³ в день, или ' + mean_bcm + ' млрд. м³ в год\nВсего сожжено за период наблюдения (' + str(days_obs) + ' ' + days_name + '): ' + sum_mcm + ' млн. м³\nМаксимальное сжигание за пролёт в 2022 г.: экв. ' + max_bcm + ' млн. м³ в день (зафиксировано ' + max_dataset + ', дата: ' + max_datetime + ')', ha = 'center', bbox = props, zorder = 11) # VNF['Temp_BB'].mean() + ylimtemp
            ax.set_xlabel('Дата', fontdict = font_labels_2)
            ax.set_ylabel('Объёмы сжигания, млн. м³', fontdict = font_labels_2)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b-%Y'))
            ax.set_xticks(xt)
            plt.legend(loc = 'upper left')
            # Title
            plt.text(x = 0.5, y = 0.95, s = str(field) + ' (только 2022 г.)', fontsize = 13.5, ha = "center", transform = fig.transFigure)
            plt.text(x = 0.5, y = 0.92, s = "Усреднённые за день объёмы сжигания по " + r"$\bf{" + "сумме >75" + "}$" + "% (~ экспериментальная корреляция)", fontsize = 10.5, ha = "center", transform = fig.transFigure)        
            plt.subplots_adjust(top = 0.91, hspace = 0.34)
            plt.savefig(path_save_dashboard + field + '/' + field + '_3_daily mean_Pct75_RH_graph_2022.png', bbox_inches = 'tight')
            print(field + ' 2022 only Pct75RH graph saved successfully.')
        else: print(field + ' has no 2022 detections.')
        print(field + ': all mean bcm graphs generated successfully.')
    except:
        print('\n\nError with field table and daily mean graphs for ' + field + '.\n\n')
        
# ================================= Multiprocessing pipeline ============================================
if __name__ == "__main__":
    # Time start
    start_time = time()
    print('Start time:', datetime.datetime.fromtimestamp(start_time).strftime('%H:%M:%S') + '.\n')
    
    # Share processed tables between functions
    manager = multiprocessing.Manager()
    datasets_extracted_sources = manager.dict()
    intersected_sources = manager.dict()
    return_dict = manager.dict()
    scatter_dict = manager.dict()
    frp_to_bcm_dict = manager.dict()
    csv_dict = manager.dict()
    fields = manager.dict()
    unique_objects = manager.dict()
    max_dict = manager.dict()
    
    # # Multiprocessing tasks
    # Pool 1: reading tables
    print('>> 1. Reading source tables using ' + str(len(datasets)) + ' cores. <<')
    pool1 = multiprocessing.Pool(len(datasets))
    for dataset in datasets:
        pool1.apply_async(read_source_tables, args = (datasets_extracted_sources, dataset))
    pool1.close()
    pool1.join()
    
    # Pool 2: intersecting with ROI
    print('\n>> 2. Intersecting with ROI using ' + str(len(datasets_extracted)) + ' cores. <<')
    pool2 = multiprocessing.Pool(len(datasets_extracted))
    for dataset_extracted in datasets_extracted:
        pool2.apply_async(intersect_with_ROI, args = (intersected_sources, datasets_extracted_sources, dataset_extracted))
    pool2.close()
    pool2.join()   
    
    # Pool 3: pre-process the tables
    print('\n>> 3. Summing tables using ' + str(len(datasets_extracted)) + ' cores. <<')
    pool3 = multiprocessing.Pool(len(datasets_extracted))
    for dataset_extracted in datasets_extracted:
        pool3.apply_async(process_tables, args = (scatter_dict, intersected_sources, return_dict, dataset_extracted)) # intersected_sources, dataset_extracted
    pool3.close()
    pool3.join()
    
    # Pool 4: mean/sum tables
    print('\n>> 4. Processing MODIS and VIIRS AFP & VNF datasets separately using ' + str(len(datasets_extracted)) + ' cores. <<')
    pool4 = multiprocessing.Pool(len(datasets_extracted))
    for processed_dataset in processed_datasets_names:
        pool4.apply_async(sum_tables, args = (scatter_dict, return_dict, processed_dataset))
    pool4.close()
    pool4.join()  
    
    # Pool 5: Scatterplots
    print('\n>> 5. Generating global MODIS and VIIRS AFP & VNF scatters for all fields using ' + str(len(scatter_datasets)) + ' cores. <<')
    pool5 = multiprocessing.Pool(len(scatter_datasets))
    for scatter_dataset in scatter_datasets:
        pool5.apply_async(scatterplots, args = (scatter_dict, scatter_dataset, csv_dict, frp_to_bcm_dict))
    pool5.close()
    pool5.join()  
    
    # Combined fit_paras table
    print('Generating fit_paras table.')
    fit_vars = np.array(['Dataset', 'Slope', 'Intercept', 'R_square', 'p_value', 'Std_err', 'Conf. intervals +/- 0.975', 'n_intersects'])
    for key in csv_dict:
        fit_vars = np.vstack((fit_vars, csv_dict[key]))
    df_fit = pd.DataFrame(fit_vars)
    headers = df_fit.iloc[0]
    df_fit = pd.DataFrame(df_fit.values[1:], columns = headers)
    df_fit = df_fit.sort_values(by = ['n_intersects', 'Dataset'])
    df_fit.to_csv(path_save_scatters + 'fit_paras.csv', index = False)
    
    # Appending dataset entries to pass in fields dict
    fields['VNF'] = return_dict['VNF_pct']
    fields['VIIRS AFP'] = return_dict['VIIRS AFP']
    fields['MODIS AFP'] = return_dict['MODIS AFP']
    
    # Pool 6: Calculate number of fields in question
    print('\n>> 6. Calculating number of entries per dataset ' + str(len(datasets_sources)) + ' cores. <<')
    pool6 = multiprocessing.Pool(len(datasets_sources))
    for dataset_source in datasets_sources:
        pool6.apply_async(n_entries, args = (scatter_dict, dataset_source, fields, unique_objects))
    pool6.close()
    pool6.join()   
    
    # Find max n of entries for future dashboard building
    n_fields = 1
    unique_objects_list = np.array([])
    for key in unique_objects:
        if len(unique_objects[key]) > n_fields: 
            n_fields = len(unique_objects[key])
            unique_objects_list = unique_objects[key]
    print('Total number of fields/objects: ' + str(n_fields) + '.')
    
    # Pool 7: Dashboard
    print('\n>> 7. Now, generating per field dashboard using ' + str(multiprocessing.cpu_count() - 1) + ' cores. <<')
    multiprocessing.cpu_count()
    pool7 = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    for field in unique_objects_list:
        pool7.apply_async(dashboards, args = (fields, field, frp_to_bcm_dict, max_dict))
    pool7.close()
    pool7.join()   
    
    # Pool 8: Field xlsx
    print('\n>> 8. Generating per field table using ' + str(multiprocessing.cpu_count() - 1) + ' cores. <<')
    multiprocessing.cpu_count()
    pool8 = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    for field in unique_objects_list:
        pool8.apply_async(field_tables, args = (fields, field, frp_to_bcm_dict, max_dict))
    pool8.close()
    pool8.join()  
    
    # Time elapsed
    print("\nProcessing complete. Combined time elapsed: %s sec." % round((time() - start_time), 2))
    # Hold the window on
    atexit.register(input, '\nPress any key to exit…')