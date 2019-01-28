#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 12:41:35 2018
funtion, contact the raw_data_download.py,classify_by_boat.py
check_reformat_data.py and match_tele_raw.py
finally: output the plot and statistics every week

@author: leizhao
"""

import rawdatamoudles as rdm
from datetime import datetime,timedelta
import shutil

raw_data_name_file='/home/jmanning/leizhao/data_file/raw_data_name.txt'  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
output_path='/home/jmanning/Desktop/test/test'
picture_save='/home/jmanning/Desktop/test/test1'
#telemetry='/home/jmanning/Desktop/telemetry.csv' #this is download from https://www.nefsc.noaa.gov/drifter/emolt.dat, 
start_time='2018-12-1'
end_time='2018-12-31'
interval=30   #day
telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status.csv'
num=(datetime.strptime(end_time,'%Y-%m-%d')-datetime.strptime(start_time,'%Y-%m-%d')).days
for i in range(int(num/interval)):
    start_week_time=datetime.strptime(start_time,'%Y-%m-%d')+i*timedelta(days=interval)
    end_week_time=datetime.strptime(start_time,'%Y-%m-%d')+(i+1)*timedelta(days=interval)
    
    rdm.download(output_path+'/DOWNLOADED',start_time=start_week_time,end_time=end_week_time)
    rdm.classify_file(input_dir=output_path+'/DOWNLOADED',output_dir=output_path+'/classified',telemetry_status_path_name=telemetry_status)
    rdm.check_reformat_data(input_dir=output_path+'/classified',output_dir=output_path+'/checked',telemetry_status_file=telemetry_status,raw_data_name_file=raw_data_name_file)
    rdm.match_tele_raw(output_path+'/checked',picture_save,telemetry_status=telemetry_status,start_time=start_week_time.strftime('%Y-%m-%d'),end_time=end_week_time.strftime('%Y-%m-%d'),dpi=500)
    shutil.rmtree(output_path+'/DOWNLOADED')
    shutil.rmtree(output_path+'/classified')
    shutil.rmtree(output_path+'/checked')
    
    
