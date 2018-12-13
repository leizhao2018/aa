#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 12:07:44 2018
match the file and telementy.
we can known how many file send to the satallite and output the figure

@author: leizhao
"""
import os
import datetime
import zlconversions as zl
import pandas as pd
from pylab import mean, std
import matplotlib.pyplot as plt
import conversions as cv
import rawdatamoudles as rdm

def draw(raw_data,tele_dict,i,start_time_local,end_time_local,path_picture_save,record_file):
    fig=plt.figure()
    fig.suptitle(i+'\n'+'successfully matched:'+str(record_file['matched_number'])+'   tele_num:'+\
                         str(len(tele_dict))+'   raw_num:'+str(record_file['file_number']),fontsize=8, fontweight='bold')
    ax2=fig.add_subplot(212)
    fig.autofmt_xdate(bottom=0.18)
    fig.subplots_adjust(left=0.18)
    ax1=fig.add_subplot(211)    
    if len(raw_data)>0 and len(tele_dict)>0:
        ax2.plot_date(raw_data['time'],raw_data['mean_temp'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax2.plot_date(tele_dict['time'],tele_dict['mean_temp'],linestyle='-',alpha=0.5,label='telementry',marker='^')
        ax2.set_title('temperature different of min:'+str(round(record_file['min_diff_temp'],2))+'  max:'+str(round(record_file['max_diff_temp'],2))+\
                          '  average:'+str(round(record_file['sum_diff_temp']/float(record_file['matched_number']),3)))
        ax1.plot_date(raw_data['time'],raw_data['mean_depth'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
        ax1.plot_date(tele_dict['time'],tele_dict['mean_depth'],linestyle='-',alpha=0.5,label='telementry',marker='^')
        ax1.set_title('depth different of min:'+str(round(record_file['min_diff_depth'],2))+'  max:'+str(round(record_file['max_diff_depth'],2))+\
                          '  average:'+str(round(record_file['sum_diff_depth']/float(record_file['matched_number']),3)))
    else:
        labels='raw_data'
        markers='d'
        if len(tele_dict)>0:
            raw_data=tele_dict
            labels='telementry'
            markers='^'   
        ax1.plot_date(raw_data['time'],raw_data['mean_depth'],linestyle='-',alpha=0.5,label=labels,marker=markers)
        ax2.plot_date(raw_data['time'],raw_data['mean_temp'],linestyle='-',alpha=0.5,label=labels,marker=markers)    
    ax1.legend()
    ax1.set_ylabel('depth(m)',fontsize=10)
    ax1.axes.get_xaxis().set_visible(False)
    ax1.axes.title.set_size(8)
    ax1.set_xlim(start_time_local,end_time_local)
    ax2.legend()
    ax2.set_ylabel('C')
    ax2.axes.title.set_size(8)
    ax2.set_xlim(start_time_local,end_time_local)
    if not os.path.exists(path_picture_save+'picture/'+i+'/'):
        os.makedirs(path_picture_save+'picture/'+i+'/')
    plt.savefig(path_picture_save+'picture/'+i+'/'+start_time_local.strftime('%Y-%m-%d')+'.png',dpi=300)

#HARDCODES
path_save='/home/jmanning/Desktop/testout/test/123/'#the path is the location of save picture and output file
input_directory='/home/jmanning/Desktop/testout/test/checkeddata/'
start_time='2018-12-3'   #file start time
time_interval=7    #unit: days
acceptable_time_diff=datetime.timedelta(minutes=20)  #unite:mintues
acceptable_distance_diff=2  #unit: miles
telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status - fitted .csv' # download from the telementry_status in web
raw_data='/home/jmanning/leizhao/data_file/raw_data_name.txt'  #colude"VP_NUM", "HULL_NUM", "VESSEL_NAME"
########################


#read the file of the telementry_status
telemetrystatus_df=rdm.read_telemetry(telemetry_status)
#set the output file"record_file" use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num','max_diff_depth',\
                                      'min_diff_depth','max_diff_temp','min_diff_temp','sum_diff_depth','sum_diff_temp'],fill_value=None)
#transfer the time format of string to datetime 
start_time_local=datetime.datetime.strptime(start_time,'%Y-%m-%d')
end_time_local=datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(days=time_interval)

#download the data of telementry
tele_df=pd.read_csv('https://www.nefsc.noaa.gov/drifter/emolt.dat',sep='\s+',names=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
#screen out the valuable data of telemetry in interval
valuable_tele_df=pd.DataFrame(data=None,columns=['vessel_n','esn','time','lon','lat','depth','temp'])#use to save the data during start time and end time
for i in range(len(tele_df)):
    tele_time=datetime.datetime.strptime(str(tele_df['year'].iloc[i])+'-'+str(tele_df['month'].iloc[i])+'-'+str(tele_df['day'].iloc[i])+' '+\
                                         str(tele_df['Hours'].iloc[i])+':'+str(tele_df['minates'].iloc[i])+':'+'00','%Y-%m-%d %H:%M:%S')
    if zl.local2utc(start_time_local)<=tele_time<zl.local2utc(end_time_local):
        valuable_tele_df=valuable_tele_df.append(pd.DataFrame(data=[[tele_df['vessel_n'][i],tele_df['esn'][i],tele_time,tele_df['lon'][i],tele_df['lat'][i],tele_df['depth'][i],\
                                                       tele_df['temp'][i]]],columns=['vessel_n','esn','time','lon','lat','depth','temp']))
valuable_tele_df.index=range(len(valuable_tele_df))
#get the path and name of the file that need to match
allfile_lists=zl.list_all_files(input_directory)
######################
file_lists=[]
for file in allfile_lists:
    if file[len(file)-4:]=='.csv':
        file_lists.append(file)
#whether the data of file and telemetry is exist
if len(valuable_tele_df)==0 and len(file_lists)==0: 
    print('please check the data website of telementry and raw_data!')
elif len(valuable_tele_df)==0:
    print('please check the data website of telementry!')
elif len(file_lists)==0:
    print('please check the data website of raw_data!')
else:
    #match the file
    index=telemetrystatus_df['Boat'] #set the index for dictionary
    raw_dict={}    #the dictinary about raw data, use to write the data about 'time','filename','mean_temp','mean_depth'
    tele_dict={}  #the dictionary about telementry data,use to write the data about'time','mean_temp','mean_depth'
    for i in range(len(index)):  #loop every boat
        raw_dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth'])
    for file in file_lists: # loop raw files
        fpath,fname=os.path.split(file)  #get the file's path and name
        # now, read header and data of every file  
        header_df=zl.nrows_len_to(file,2,name=['key','value']) #only header 
        data_df=zl.skip_len_to(file,2) #only data
        
        #get the vessel number of every file
        for i in range(len(header_df)):
            if header_df['key'][i].lower()=='vessel number'.lower():
                vessel_number=int(header_df['value'][i])
                break
        #caculate the number of raw files in every vessel
        for i in range(len(record_file_df)):
            if record_file_df['Vessel#'][i]==vessel_number:
                if record_file_df['file_number'].isnull()[i]:
                    record_file_df['file_number'][i]=1
                else:
                    record_file_df['file_number'][i]=record_file_df['file_number'][i]+1  
                    
 
        #caculate the mean temperature and depth of every file
        value_data_df=data_df.ix[(data_df['Depth(m)']>0.85*mean(data_df['Depth(m)']))]  #filter the data
        value_data_df=value_data_df.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        value_data_df=value_data_df.ix[(value_data_df['Temperature(C)']>mean(value_data_df['Temperature(C)'])-3*std(value_data_df['Temperature(C)'])) & \
                   (value_data_df['Temperature(C)']<mean(value_data_df['Temperature(C)'])+3*std(value_data_df['Temperature(C)']))]  #Excluding gross error
        value_data_df.index = range(len(value_data_df))  #reindex
        mean_temp=str(round(mean(value_data_df['Temperature(C)'][1:len(value_data_df)]),2))
        mean_depth=str(abs(int(round(mean(value_data_df['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        
        #match rawdata and telementry data
        time_str=fname.split('.')[0].split('_')[2]+' '+fname.split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]) 
        time_gmt=datetime.datetime.strptime(time_str,"%Y%m%d %H%M%S")
        #transfer the format latitude and longitude
        lat,lon=cv.dm2dd(value_data_df['Lat'][len(value_data_df)-1],value_data_df['Lon'][len(value_data_df)-1]) 
        #write the data of raw file to dict
        for i in range(len(telemetrystatus_df)):
            if telemetrystatus_df['Vessel#'][i]==vessel_number:
                raw_dict[telemetrystatus_df['Boat'][i]]=raw_dict[telemetrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[time_local,\
                                    fname,float(mean_temp),float(mean_depth)]],columns=['time','filename','mean_temp','mean_depth']).iloc[0],ignore_index=True) 
        #caculate the numbers of successful matchs and the minimum,maximum and average different of temperature and depth, and write this data to record file
        for i in range(len(valuable_tele_df)):
            if valuable_tele_df['vessel_n'][i].split('_')[1]==str(vessel_number):     
                if abs(valuable_tele_df['time'][i]-time_gmt)<=acceptable_time_diff:  #time match
                    if zl.dist(lat1=lat,lon1=lon,lat2=float(valuable_tele_df['lat'][i]),lon2=float(valuable_tele_df['lon'][i]))<=acceptable_distance_diff:  #distance match               
                        for j in range(len(record_file_df)):
                            if record_file_df['Vessel#'][j]==vessel_number:
                                if record_file_df['matched_number'].isnull()[j]:
                                    record_file_df['matched_number'][j]=1
                                    record_file_df['sum_diff_temp'][j]=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    record_file_df['max_diff_temp'][j]=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    record_file_df['min_diff_temp'][j]=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    record_file_df['sum_diff_depth'][j]=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    record_file_df['max_diff_depth'][j]=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    record_file_df['min_diff_depth'][j]=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    break
                                    
                                else:
                                    record_file_df['matched_number'][j]=record_file_df['matched_number'][j]+1
                                    record_file_df['sum_diff_temp'][j]=record_file_df['sum_diff_temp'][j]+abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    record_file_df['sum_diff_depth'][j]=record_file_df['sum_diff_depth'][j]+abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    if record_file_df['max_diff_temp'][j]<abs(float(mean_temp)-float(valuable_tele_df['temp'][i])):
                                        record_file_df['max_diff_temp'][j]=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    if record_file_df['min_diff_temp'][j]>abs(float(mean_temp)-float(valuable_tele_df['temp'][i])):
                                        record_file_df['min_diff_temp'][j]=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                    if record_file_df['max_diff_depth'][j]<abs(float(mean_depth)-float(valuable_tele_df['depth'][i])):
                                        record_file_df['max_diff_depth'][j]=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    if record_file_df['min_diff_depth'][j]>abs(float(mean_depth)-float(valuable_tele_df['depth'][i])):
                                        record_file_df['min_diff_depth'][j]=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                    break
                                    

    #write 'time','mean_temp','mean_depth' of the telementry to tele_dict             
    for i in range(len(valuable_tele_df)):  #valuable_tele_df is the valuable telemetry data during start time and end time 
        for j in range(len(telemetrystatus_df)):
            if int(valuable_tele_df['vessel_n'][i].split('_')[1])==telemetrystatus_df['Vessel#'][j]:
                tele_dict[telemetrystatus_df['Boat'][j]]=tele_dict[telemetrystatus_df['Boat'][j]].append(pd.DataFrame(data=[[valuable_tele_df['time'][i],\
                         float(valuable_tele_df['temp'][i]),float(valuable_tele_df['depth'][i])]],columns=['time','mean_temp','mean_depth']).iloc[0],ignore_index=True)
    #draw the picture of result
    for i in index:#loop every boat,  i represent the name of boat
        for j in range(len(record_file_df)): #find the location of data of this boat in record file 
            if i.lower()==record_file_df['Boat'][j].lower():
                break
        raw_dict[i]=raw_dict[i].sort_values(by=['time'])
        raw_dict[i].index=range(len(raw_dict[i]))
        if len(raw_dict[i])==0 and len(tele_dict[i])==0:
            continue
        else:
            draw(raw_dict[i],tele_dict[i],i,start_time_local,end_time_local,path_save,record_file_df.iloc[j])
    #save the record file
    record_file_df.to_csv(path_save+'record_file.csv',index=0) 
