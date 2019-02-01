#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 15:50:55 2018
Download the file from web
@author: jmanning
"""
import ftplib
from datetime import datetime
import zlconversions as zl
import os

#HARDCODINGT
output_dir='/home/jmanning/leizhao/data_file/input_data/'
start_time=datetime.strptime('2000-1-1','%Y-%m-%d')
end_time=datetime.now()
############

ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
print ('Logging in.')
ftp.cwd('/Matdata')
print ('Accessing files')
filenames = ftp.nlst() # get filenames within the directory OF REMOTE MACHINE
start_time_gmt=zl.local2utc(start_time)  #time tranlate from local to UTC
end_time_gmt=zl.local2utc(end_time)
# MAKE THIS A LIST OF FILENAMES THAT WE NEED DOWNLOAD
download_files=[]
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
for file in filenames:
    if len(file.split('_'))==4:
        if start_time_gmt<=datetime.strptime(file.split('_')[2]+file.split('_')[3].split('.')[0],'%Y%m%d%H%M%S')<end_time_gmt:
            download_files.append(file)
for filename in download_files: # DOWNLOAD FILES   
    local_filename = os.path.join(output_dir, filename)
    file = open(local_filename, 'wb')
    ftp.retrbinary('RETR '+ filename, file.write)
    file.close()
ftp.quit() # This is the “polite” way to close a connection
print ('New files downloaded')