# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 12:39:15 2018
this code used to know which boat get the data
and put the data file to the right folder

notice:this code is suitable for matching data after 2000
@author: jmanning
"""
import zlconversions as zl
import glob
import os
import rawdatamoudles as rdm
import sys
#HARDCODES

input_dir='/home/jmanning/Desktop/testout/test/Matdata/2018-12-3/data'  #the downloaded data from web
output_dir='/home/jmanning/Desktop/testout/raw_data5'#  th path od save data,this directory must be empty
telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status - fitted .csv'   #this file should put in main_directory
######################

#if input directory is not empty, it will exit.
if os.listdir(output_dir):
    print ('please input a empty directory!')
    sys.exit()
#read the file of the telementry_status
telemetry_status_df=rdm.read_telemetry(telemetry_status)
#fix the format of time about logger_change
for i in range(len(telemetry_status_df)):
    if not telemetry_status_df['logger_change'].isnull()[i]:
        date_logger_change=telemetry_status_df['logger_change'][i].split(',')   #get the time data of the logger_change
        for j in range(0,len(date_logger_change)):
            if len(date_logger_change[j])>4:     #keep the date have the month and year such as 1/17
                date_logger_change[j]=zl.transform_date(date_logger_change[j]) #use the transform_date(date) to fix the date
        telemetry_status_df['logger_change'][i]=date_logger_change
    else:
        continue 
#get the path and name of the file that need to match
file_lists=glob.glob(os.path.join(input_dir,'*.csv'))
#match the file   
for file in file_lists:
    #time conversion, GMT time to local time
    date=file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[2] # get the gmt date,that date we get the current file
    time=file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[3] #get the gmt_time of current file,that time we start get the data
    date_date=zl.gmt_to_eastern(date[0:4]+'-'+date[4:6]+'-'+date[6:8]+' '+time[0:2]+':'+time[2:4]+':'+time[4:6]).strftime("%Y%m%d") #get the local date that date we get the current file
    #math the SN and date
    for i in range(len(telemetry_status_df)):
        if not telemetry_status_df['Lowell-SN'].isnull()[i] and not telemetry_status_df['logger_change'].isnull()[i]: #we will enter the next line if SN or date is not exist 
            for j in range(len(telemetry_status_df['Lowell-SN'][i].split(','))):   
                fname_len_SN=len(file.split('/')[len(file.split('/'))-1:][0].split('_')[1]) #the length of SN in the file name
                len_SN=len(telemetry_status_df['Lowell-SN'][i].split(',')[j]) #the length of SN in the culumn of the Lowell-SN inthe file of the telemetry_status.csv
                if telemetry_status_df['Lowell-SN'][i].split(',')[j][len_SN-fname_len_SN:]==file.split('/')[len(file.split('/'))-1:][0].split('_')[1]:
                    fpath,fname=os.path.split(file)    #seperate the path and name of the file
                    dstfile=(fpath).replace(input_dir,output_dir+'/'+telemetry_status_df['Boat'][i]+'/'+fname) #produce the path+filename of the destination
                    #copy the file to the destination folder
                    if j<len(telemetry_status_df['logger_change'][i])-1:
                        if telemetry_status_df['logger_change'][i][j]<=date_date<=telemetry_status_df['logger_change'][i][j+1]:
                            zl.copyfile(file,dstfile)  
                    else:
                        if telemetry_status_df['logger_change'][i][j]<=date_date:
                            zl.copyfile(file,dstfile) 
