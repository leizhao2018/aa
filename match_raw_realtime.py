# -*- coding: utf-8 -*-
"""
Created on Mon Oct 22 11:27:15 2018
.match the file and telementy.
we can known how many file send to the satallite
@author: leizhao
"""

import pandas as pd
import zlconvertions as zl
from pylab import mean, std
import numpy as np
from datetime import datetime,timedelta
import conversions as cv
import re
import os

#HARDCODES
input_dir='/home/jmanning/Desktop/test1/te/checked_rawdata/Virginia_Marise/'   #THis folder
output_dir='/home/jmanning/leizhao/'
vessel_number_file='/home/jmanning/leizhao/data_file/vessel_number.txt'  
satellitedata='/home/jmanning/Desktop/test/telementry.txt'  
acceptable_time_diff=timedelta(minutes=10)  #time threshold (mile)
acceptable_distance_diff=1   #Distance threshold(mile)
############################

vessel_number_df=pd.read_csv(vessel_number_file)   #get the data of vessel number
#set the dataframe of output
record_file=vessel_number_df.reindex(columns=['name','vessel','matched_number','start_time','end_time','tele_num_period','file_number','cent_tele_period','cent_match_vessel','tele_num'],fill_value=0)  
#get the data of telementry
tele_df=pd.read_csv(satellitedata,sep=' ',names=['vessel_n','esn','month','day','Hours','minates','fracyrday','lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
allfile_lists=zl.list_all_files(input_dir)  #get all files of pointed folder
#choose all files of .csv
file_lists=[]
for file in allfile_lists:
    if file[len(file)-4:]=='.csv':
        file_lists.append(file)
#exist_file_tele=pd.DataFrame(data=None,columns=['vessel_n','esn','month','day','Hours','minates','fracyrday','lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])

#start math raw file
for file in file_lists:
    file_header=zl.nrows_to(file,'Depth',['Probe Type','Lowell'])  #only get the header
    df=zl.skip_to(file,'HEADING')  #only get the data in file
    dft=df.ix[(df['Depth(m)']>0.85*mean(df['Depth(m)']))]  #filter the data
    dft=dft.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
    dft=dft.ix[(dft['Temperature(C)']>mean(dft['Temperature(C)'])-3*std(dft['Temperature(C)'])) & (dft['Temperature(C)']<mean(dft['Temperature(C)'])+3*std(dft['Temperature(C)']))]  #Excluding gross error
    dft.index = range(len(dft))  #reindex
    meantemp=str(int(round(np.mean(dft['Temperature(C)'][1:len(dft)]),2)*100)).zfill(4)  #caculate the mean temperature
    sdeviatemp=str(int(round(np.std(dft['Temperature(C)'][1:len(dft)]),2)*100)).zfill(4) #caculate the std of the temperature
    meandepth=str(abs(int(round(mean(dft['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
    rangedepth=str(abs(int(round(max(dft['Depth(m)'].values)-min(dft['Depth(m)'].values))))).zfill(3)  #caculate the range of depth
    #find the vessel number in every file
    for i in range(len(file_header)):
        if file_header['Probe Type'][i].lower()=='vessel number'.lower():
            vessel_number=int(file_header['Lowell'][i])
            break
    #find the produced time in every file
    file_time=datetime.strptime(re.split('[/_.]',file)[len(re.split('[/_.]',file))-3]+' '+re.split('[/_.]',file)[len(re.split('[/_.]',file))-2],'%Y%m%d %H%M%S')    
    #Calculate the time period during which each vessel generates data    
    for j in range(len(record_file)):
        if record_file['vessel'][j]==vessel_number:
            record_file['file_number'][j]=record_file['file_number'][j]+1  #caculate the number of files in every vessel
            if record_file['start_time'][j]==0:  #If the start time is 0, the difference between the current file time and the threshold is the start time and the end time
                record_file['start_time'][j]=(file_time-acceptable_time_diff).strftime("%Y-%m-%d %H:%M:%S")  
                record_file['end_time'][j]=(file_time+acceptable_time_diff).strftime("%Y-%m-%d %H:%M:%S")  
            else:  #If the start time is not the earliest time, the correction is the earliest time; the end time is the same.
                if file_time<datetime.strptime(record_file['start_time'][j],'%Y-%m-%d %H:%M:%S'):
                    record_file['start_time'][j]=(file_time-acceptable_time_diff).strftime("%Y-%m-%d %H:%M:%S")
                elif file_time>datetime.strptime(record_file['end_time'][j],'%Y-%m-%d %H:%M:%S'):
                    record_file['end_time'][j]=(file_time+acceptable_time_diff).strftime("%Y-%m-%d %H:%M:%S")
    lat,lon=cv.dm2dd(dft['Lat'][len(dft)-1],dft['Lon'][len(dft)-1]) #Convert latitude and longitude
    #caculate the numbers of successful matchs
    for i in range(len(tele_df)):
        if tele_df['vessel_n'][i].split('_')[1]==str(vessel_number):
            tele_time=datetime.strptime(str(tele_df['year'][i])+'-'+str(tele_df['month'][i])+'-'+str(tele_df['day'][i])+' '+str(tele_df['Hours'][i])+':'+str(tele_df['minates'][i])+':'+'00','%Y-%m-%d %H:%M:%S')
            if abs(tele_time-file_time)<=acceptable_time_diff:  #time match
                if zl.dist(lat1=lat,lon1=lon,lat2=tele_df['lat'][i],lon2=tele_df['lon'][i])<=acceptable_distance_diff:  #distance match
#                    exist_file_tele=exist_file_tele.append(tele_df.iloc[i])
                    fpath,fname=os.path.split(file)                    
                    for j in range(len(record_file)):
                        if record_file['vessel'][j]==vessel_number:
                            record_file['matched_number'][j]=record_file['matched_number'][j]+1
                            break

#calculate the number of transmition
for i in range(len(tele_df)):
    tele_time=datetime.strptime(str(tele_df['year'][i])+'-'+str(tele_df['month'][i])+'-'+str(tele_df['day'][i])+' '+str(tele_df['Hours'][i])+':'+str(tele_df['minates'][i])+':'+'00','%Y-%m-%d %H:%M:%S')
    for j in range(len(record_file)): 
        if tele_df['vessel_n'][i].split('_')[1]==str(record_file['vessel'][j]):
            record_file['tele_num'][j]=record_file['tele_num'][j]+1  #calculate the number of transmissions over all times 
            #caculate number of transmissions during time of raw folder
            if record_file['start_time'][j]!=0:
                if datetime.strptime(str(record_file['start_time'][j]),'%Y-%m-%d %H:%M:%S')<=tele_time<=datetime.strptime(str(record_file['end_time'][j]),'%Y-%m-%d %H:%M:%S'):
                    record_file['tele_num_period'][j]=record_file['tele_num_period'][j]+1

#caculate the probability of successful matching in raw folder and transmission
for i in range(len(record_file)):    
    if record_file['tele_num_period'][i]!=0:
        record_file['cent_tele_period'][i]=format(float(record_file['matched_number'][i])/float(record_file['tele_num_period'][i]),'.2%')
        record_file['cent_match_vessel'][i]=format(float(record_file['matched_number'][i])/float(record_file['file_number'][i]),'.2%')
    else:
        record_file['cent_tele_period'][i]=format(record_file['cent_tele_period'][i],'.2%')
        record_file['cent_match_vessel'][i]=format(record_file['cent_match_vessel'][i],'.2%')
record_file=pd.concat([record_file[:],pd.DataFrame(data=[['Total','',0,'','',0,0,'','',0]],columns=['name','vessel','matched_number','start_time','end_time','tele_num_period','file_number','cent_tele_period','cent_match_vessel','tele_num'])],ignore_index=True)

#caculate the totle of number of successful matched,telementry in period,folder and telementry 
for i in range(len(record_file)-1):
    record_file['matched_number'][len(record_file)-1]=record_file['matched_number'][len(record_file)-1]+record_file['matched_number'][i]
    record_file['tele_num_period'][len(record_file)-1]=record_file['tele_num_period'][len(record_file)-1]+record_file['tele_num_period'][i]
    record_file['file_number'][len(record_file)-1]=record_file['file_number'][len(record_file)-1]+record_file['file_number'][i]
    record_file['tele_num'][len(record_file)-1]=record_file['tele_num'][len(record_file)-1]+record_file['tele_num'][i]

record_file.to_csv(output_dir+'record_file.csv',index=0)


        
