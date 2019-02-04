#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 14:05:12 2018

@author: jmanning
"""

import rawdatamoudles as rdm
import os
from datetime import datetime,timedelta
import zlconversions as zl
import pandas as pd
import sys
from pylab import mean, std
import conversions as cv
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np


# coding: utf-8
def tele_raw_location(input_dir,path_save,telemetry_status,start_time,end_time,telemetry_path='https://www.nefsc.noaa.gov/drifter/emolt.dat',accept_minutes_diff=20,acceptable_distance_diff=2,dpi=300):
    """
    caculate the avrage of location of raw data
    and 
    we can known how many file send to the satallite and output the figure
    """
    if not os.path.exists(path_save):
        os.makedirs(path_save)
    #read the file of the telementry_status
    telemetrystatus_df=rdm.read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num','max_diff_lat',\
                                      'min_diff_lat','average_diff_lat','min_lat','max_lat','min_lon','max_lon','max_diff_lon','min_diff_lon','average_diff_lon','sum_diff_lat','sum_diff_lon'],fill_value=None)
    #transfer the time format of string to datetime 
    start_time_local=datetime.strptime(start_time,'%Y-%m-%d')
    end_time_local=datetime.strptime(end_time,'%Y-%m-%d')
    allfile_lists=zl.list_all_files(input_dir)
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv':
            file_lists.append(file)
    #download the data of telementry
    tele_df=rdm.read_telemetry(telemetry_path)
    #screen out the data of telemetry in interval
    valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
    for i in range(len(tele_df)):
        tele_time=datetime.strptime(str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00','%Y-%m-%d %H:%M:%S')
        if zl.local2utc(start_time_local)<=tele_time<zl.local2utc(end_time_local):
            valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],tele_df['depth'][i],\
                                                       tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
    valuable_tele_df.index=range(len(valuable_tele_df))
    #whether the data of file and telemetry is exist
    if len(valuable_tele_df)==0 and len(file_lists)==0:
        print('please check the data website of telementry and the directory of raw_data is exist!')
        sys.exit()
    elif len(valuable_tele_df)==0:
        print('please check the data website of telementry!')
        sys.exit()
    elif len(file_lists)==0:
        print('please check the directory raw_data is exist!')
        sys.exit()
    #match the file
    index=telemetrystatus_df['Boat'] #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    for i in range(len(index)):  #loop every boat
        raw_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_lat','mean_lon'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_lat','mean_lon'])
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data

        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        for i in range(len(value_data_df['Lat'])):
            value_data_df['Lat'][i],value_data_df['Lon'][i]=cv.dm2dd(value_data_df['Lat'][i],value_data_df['Lon'][i])
        min_lat=min(value_data_df['Lat'].values)
        max_lat=max(value_data_df['Lat'].values)
        min_lon=min(value_data_df['Lon'].values)
        max_lon=max(value_data_df['Lon'].values)
        mean_lat=str(round(mean(value_data_df['Lat'].values),4))
        mean_lon=str(round(mean(value_data_df['Lon'].values),4)) #caculate the mean depth
        #get the vessel number of every file
        for i in range(len(header_df)):
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel,and min,max of lat and lon
        for i in range(len(record_file_df)):
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['min_lat'][i]=min_lat
                    record_file_df['max_lat'][i]=max_lat
                    record_file_df['min_lon'][i]=min_lon
                    record_file_df['max_lon'][i]=max_lon
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=int(record_file_df['file_number'][i]+1)
                    if record_file_df['min_lat'][i]>min_lat:
                        record_file_df['min_lat'][i]=min_lat
                    if record_file_df['max_lat'][i]<max_lat:
                        record_file_df['max_lat'][i]=max_lat
                    if record_file_df['min_lon'][i]>min_lon:
                        record_file_df['min_lon'][i]=min_lon
                    if record_file_df['max_lon'][i]<max_lon:
                        record_file_df['max_lon'][i]=max_lon
        
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]) 
        #write the data of raw file to dict
        for i in range(len(telemetrystatus_df)):
            if telemetrystatus_df['Vessel#'][i]==vessel_number:
                raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_local,\
                                    fname,float(mean_lat),float(mean_lon)]],columns=['time','filename','mean_lat','mean_lon']).iloc[0],ignore_index=True) 
        #match rawdata and telementry data
#        time_gmt=datetime.strptime(time_str,"%Y%m%d %H%M%S")
#        #transfer the format latitude and longitude
#        lat,lon=cv.dm2dd(value_data_df['Lat'][len(value_data_df)-1],value_data_df['Lon'][len(value_data_df)-1]) 
#        #caculate the numbers of successful matchs and the minimum,maximum and average different of temperature and depth, and write this data to record file
#        for i in range(len(valuable_tele_df)):
#            if valuable_tele_df['vessel_n'][i].split('_')[1]==str(vessel_number):     
#                if abs(valuable_tele_df['time'][i]-time_gmt)<=timedelta(minutes=accept_minutes_diff):  #time match
#                    if zl.dist(lat1=lat,lon1=lon,lat2=float(valuable_tele_df['lat'][i]),lon2=float(valuable_tele_df['lon'][i]))<=acceptable_distance_diff:  #distance match               
#                        for j in range(len(record_file_df)):
#                            if record_file_df['Vessel#'][j]==vessel_number:
#                                diff_lat=round((float(mean_lat)-float(valuable_tele_df['lat'][i])),4)
#                                diff_lon=round((float(mean_lon)-float(valuable_tele_df['lon'][i])),4)
#                                if record_file_df['matched_number'].isnull()[j]:
#                                    record_file_df['matched_number'][j]=1
#                                    record_file_df['sum_diff_lat'][j]=diff_lat
#                                    record_file_df['max_diff_lat'][j]=diff_lat
#                                    record_file_df['min_diff_lat'][j]=diff_lat
#                                    record_file_df['sum_diff_lon'][j]=diff_lon
#                                    record_file_df['max_diff_lon'][j]=diff_lon
#                                    record_file_df['min_diff_lon'][j]=diff_lon
#                                    break
#                                else:
#                                    record_file_df['matched_number'][j]=int(record_file_df['matched_number'][j]+1)
#                                    record_file_df['sum_diff_lat'][j]=record_file_df['sum_diff_lat'][j]+diff_lat
#                                    record_file_df['sum_diff_lon'][j]=record_file_df['sum_diff_lon'][j]+diff_lon
#                                    if record_file_df['max_diff_lat'][j]<diff_lat:
#                                        record_file_df['max_diff_lat'][j]=diff_lat
#                                    if record_file_df['min_diff_lat'][j]>diff_lat:
#                                        record_file_df['min_diff_lat'][j]=diff_lat
#                                    if record_file_df['max_diff_lon'][j]<diff_lon:
#                                        record_file_df['max_diff_lon'][j]=diff_lon
#                                    if record_file_df['min_diff_lon'][j]>diff_lon:
#                                        record_file_df['min_diff_lon'][j]=diff_lon
#                                    break
#                                    
    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict    and calculate the min,max value of lat and lon         
    
    for i in range(len(valuable_tele_df)):  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in range(len(telemetrystatus_df)):
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                  #count the numbers by boats
                if record_file_df['tele_num'].isnull()[j]:
                    record_file_df['tele_num'][j]=1
                else:
                    record_file_df['tele_num'][j]=record_file_df['tele_num'][j]+1
                if record_file_df['max_lat'].isnull()[j]:
                    record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                else:
                    if record_file_df['min_lat'][j]>valuable_tele_df['lat'][i]:
                        record_file_df['min_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['max_lat'][j]<valuable_tele_df['lat'][i]:
                        record_file_df['max_lat'][j]=valuable_tele_df['lat'][i]
                    if record_file_df['min_lon'][j]>valuable_tele_df['lon'][i]:
                        record_file_df['min_lon'][j]=valuable_tele_df['lon'][i]
                    if record_file_df['max_lon'][j]<valuable_tele_df['lon'][i]:
                        record_file_df['max_lon'][j]=valuable_tele_df['lon'][i]
                    
                #write 'time','mean_temp','mean_depth' of the telementry to tele_dict
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['lat'][i]),float(valuable_tele_df['lon'][i])]],columns=['time','mean_lat','mean_lon']).iloc[0],ignore_index=True)
    
    for i in range(len(record_file_df)):
        if not record_file_df['matched_number'].isnull()[i]:
            record_file_df['average_diff_lat'][i]=round(record_file_df['sum_diff_lat'][i]/record_file_df['matched_number'][i],4)
            record_file_df['average_diff_lon'][i]=round(record_file_df['sum_diff_lon'][i]/record_file_df['matched_number'][i],4)
        else:
            record_file_df['matched_number'][i]=0
        if record_file_df['tele_num'].isnull()[i]:
            record_file_df['tele_num'][i]=0
        if record_file_df['file_number'].isnull()[i]:
            record_file_df['file_number'][i]=0
    #draw the picture of result
    for i in index:#loop every boat,  i represent the name of boat
        for j in range(len(record_file_df)): #find the location of data of this boat in record file 
            if i.lower()==record_file_df['Boat'][j].lower():
                break
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
#        if len(raw_dict[i])==0 and len(tele_dict[i])==0:
#            continue
#        else:
#            draw_map(raw_dict[i],tele_dict[i],i,start_time_local,end_time_local,path_save,record_file_df.iloc[j],dpi=dpi)
    record_file_df=record_file_df.drop(['sum_diff_lat','sum_diff_lon'],axis=1)
    #save the record file
    record_file_df.to_csv(path_save+'/'+start_time+'_'+end_time+' statistics.csv',index=0) 
    return raw_dict,tele_dict,record_file_df,index,start_time_local,end_time_local,path_save

def to_list(lat,lon):
    "transfer to list"
    x,y=[],[]
    for i in range(len(lat)):
        x.append(lat[i])
        y.append(lon[i])
    return x,y

def draw_map(raw_dict,tele_dict,i,start_time_local,end_time_local,path_picture_save,record_file,dpi=300):
    #creat map
    #Create a blank canvas  
    fig=plt.figure(figsize=(8,10))
    fig.suptitle('F/V '+i,fontsize=24, fontweight='bold')
    ax=fig.add_axes([0.08,0.05,0.9,0.92])
    ax.set_title(start_time_local.strftime('%Y-%m-%d')+'-'+end_time_local.strftime('%Y-%m-%d'))
    ax.axes.title.set_size(16)
    min_lat=record_file['min_lat']
    max_lat=record_file['max_lat']
    max_lon=record_file['max_lon']
    min_lon=record_file['min_lon']

    if (max_lon-min_lon)>(max_lat-min_lat):
        max_lat=max_lat+((max_lon-min_lon)-(max_lat-min_lat))/2.0
        min_lat=min_lat-((max_lon-min_lon)-(max_lat-min_lat))/2.0
    else:
        max_lon=max_lon+((max_lat-min_lat)-(max_lon-min_lon))/2.0
        max_lon=max_lon-((max_lat-min_lat)-(max_lon-min_lon))/2.0
    service = 'Ocean_Basemap'
    xpixels = 5000 
    #Build a map background
    map=Basemap(projection='mill',llcrnrlat=min_lat-0.3,urcrnrlat=max_lat+0.3,llcrnrlon=min_lon-0.3,urcrnrlon=max_lon+0.3,\
                resolution='f',lat_0=(record_file['min_lat']+record_file['max_lat'])/2.0,lon_0=(record_file['max_lon']+record_file['min_lon'])/2.0,epsg = 4269)
    map.arcgisimage(service=service, xpixels = xpixels, verbose= False)
    if max_lat-min_lat>2:
        step=1
    elif max_lat-min_lat>6:
        step=int((max_lat-min_lat)/5)
    else:
        step=0.5
    # draw parallels.
    parallels = np.arange(0.,90,step)
    map.drawparallels(parallels,labels=[1,0,0,0],fontsize=10)
    # draw meridians
    meridians = np.arange(180.,360.,step)
    map.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10)
  
    if len(raw_dict)>0 and len(raw_dict)>0:
        raw_lat,raw_lon=to_list(raw_dict['mean_lat'],raw_dict['mean_lon'])
        raw_x,raw_y=map(raw_lon,raw_lat)
        ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
        tele_lat,tele_lon=to_list(tele_dict['mean_lat'],tele_dict['mean_lon'])
        tele_x,tele_y=map(tele_lon,tele_lat)
        ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
        ax.legend()
    else:
        if len(raw_dict)>0:
            raw_lat,raw_lon=to_list(raw_dict['mean_lat'],raw_dict['mean_lon'])
            raw_x,raw_y=map(raw_lon,raw_lat)
            ax.plot(raw_x,raw_y,'ro',markersize=6,alpha=0.5,label='raw_data')
            ax.legend()
        else:
            tele_lat,tele_lon=to_list(tele_dict['mean_lat'],tele_dict['mean_lon'])
            tele_x,tele_y=map(tele_lon,tele_lat)
            ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
            ax.legend()
    if not os.path.exists(path_picture_save+'/picture/'+i+'/'):
        os.makedirs(path_picture_save+'/picture/'+i+'/')
    plt.savefig(path_picture_save+'/picture/'+i+'/'+'location'+'_'+start_time_local.strftime('%Y-%m-%d')+'_'+end_time_local.strftime('%Y-%m-%d')+'.png',dpi=dpi)
###draw_map end

#main 
raw_data_name_file='/home/jmanning/leizhao/data_file/raw_data_name.txt'  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
output_path='/home/jmanning/Desktop/test/test3'
picture_save='/home/jmanning/Desktop/test/test4'
telemetry='/home/jmanning/Desktop/telementry.csv' #this is download from https://www.nefsc.noaa.gov/drifter/emolt.dat, 
start_time_str='2018-7-1'
end_time_str='2018-12-31'
telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status.csv'
start_time=datetime.strptime(start_time_str,'%Y-%m-%d')
end_time=datetime.strptime(end_time_str,'%Y-%m-%d')
#rdm.check_reformat_data(input_dir=output_path+'/classified',output_dir=output_path+'/checked',telemetry_status_file=telemetry_status,raw_data_name_file=raw_data_name_file)
raw_dict,tele_dict,record_file_df,index,start_time_local,end_time_local,path_save=tele_raw_location(output_path+'/checked',picture_save,telemetry_path=telemetry,telemetry_status=telemetry_status,start_time=start_time.strftime('%Y-%m-%d'),end_time=end_time.strftime('%Y-%m-%d'),dpi=500)
for i in index:
    for j in range(len(record_file_df)): #find the location of data of this boat in record file 
        if i.lower()==record_file_df['Boat'][j].lower():
            break
    if len(raw_dict[i])==0 and len(tele_dict[i])==0:   
        continue
    else:
        draw_map(raw_dict[i],tele_dict[i],i,start_time_local,end_time_local,path_save,record_file_df.iloc[j],dpi=300)

