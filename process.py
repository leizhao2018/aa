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
from datetime import datetime
import os
#import shutil

raw_data_name_file='/home/jmanning/leizhao/data_file/raw_data_name.txt'  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
output_path='/home/jmanning/Desktop/test/test3'  #use to save the data 
picture_save='/home/jmanning/Desktop/test/test10' #use to save the picture
telemetry='/home/jmanning/Desktop/telementry.csv' #this is download from https://www.nefsc.noaa.gov/drifter/emolt.dat, 
start_time_str='2018-7-1'
end_time_str='2018-12-31'

telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status.csv'

#way1:
#interval=30   #day
#num=(datetime.strptime(end_time_str,'%Y-%m-%d')-datetime.strptime(start_time_str,'%Y-%m-%d')).days
#for i in range(int(num/interval)):
#    start_time=datetime.strptime(start_time_str,'%Y-%m-%d')+i*timedelta(days=interval)
#    end_time=datetime.strptime(start_time_str,'%Y-%m-%d')+(i+1)*timedelta(days=interval)

#way2
start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d')


#download raw data from website
rdm.download(output_path+'/DOWNLOADED',start_time=start_time,end_time=end_time)  

#classify the file by every boat
rdm.classify_by_boat(input_dir=output_path+'/DOWNLOADED',output_dir=output_path+'/classified',telemetry_status_path_name=telemetry_status)
print('classfy finished!')
#check the reformat of every file:include header,heading,lat,lon,depth,temperature.
rdm.check_reformat_data(input_dir=output_path+'/classified',output_dir=output_path+'/checked',telemetry_status_file=telemetry_status,raw_data_name_file=raw_data_name_file)
print('check format finished!')
#match the telementry data with raw data, calculate the numbers of successful matched and the differnces of two data. finally , use the picture to show the result.
raw_dict,tele_dict,record_file_df,index,start_time_local,end_time_local,path_save=rdm.match_tele_raw(output_path+'/checked',path_save=picture_save,telemetry_path=telemetry,telemetry_status=telemetry_status,start_time=start_time.strftime('%Y-%m-%d'),end_time=end_time.strftime('%Y-%m-%d'),dpi=500)
print('match telemetered and raw data finished!')
print("start draw map")
if not os.path.exists(picture_save):
    os.makedirs(picture_save)
for i in index:
    for j in range(len(record_file_df)): #find the location of data of this boat in record file 
        if i.lower()==record_file_df['Boat'][j].lower():
            break
    if len(raw_dict[i])==0 and len(tele_dict[i])==0:   
        continue
    else:
        rdm.draw_map(raw_dict[i],tele_dict[i],i,start_time_local,end_time_local,picture_save,record_file_df.iloc[j],dpi=300)
        rdm.draw_time_series_plot(raw_dict[i],tele_dict[i],i,start_time_local,end_time_local,picture_save,record_file_df.iloc[j],dpi=300)
#    shutil.rmtree(output_path+'/DOWNLOADED')  #delete the folder of DOWNLOADED
#    shutil.rmtree(output_path+'/classified')  #DELETE THE FOLDER OF CLASSIFIED 
#    shutil.rmtree(output_path+'/checked')   #DELETE THE FOLDER OF CHECKED

