# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 11:15:59 2018
-fix the filename such as 'li_da_20180608_153220.csv' to 'li_7ada_20180608_153220.csv'
-standardize the lat and lon data format 
-Is the name_vessel right? not, correct it.
-the vessel name is exist? if not,insert the name
@author: leizhao
"""
import pandas as pd
import os
import glob

#HARDCODES
mindepth=10 #minimum depth (meters) that a file must have to be considered usable
input_dir='/home/jmanning/leizhao/data_file/input_data/'  #the directory is the boat data folder and the file that contact name with vessel folder 
input_dir2='boat_data_needs_upload/data on Lisa ann 1/towifi/' 
#input_dir='/home/jmanning/leizhao/data_file/input_data/tem/'   #the direction of the boat data
#boat_data='li_c5_20180323_202201.csv'
output_dir='/home/jmanning/leizhao/data_file/output_data/data/'  #the path of the print file. 
name_data='vessel_name.txt'      #name contact vessel number file this file has vessel_# and vessel name colums
vessel_name='Lisa_Ann_III'          # the data from whose boat
name=['Probe Type','Lowell','2','3','4','5'] #these extra are the lat, lon, temp,dep columns that come after the 
ndf_name=['name','vessel1'] #header for name_data
name1=['HEADING','datet(GMT)','lat','lon','Temperature (C)','Depth (m)']# header for csv files
ndf=pd.read_csv(input_dir+name_data,names=ndf_name) 

# First, lets standarize the file name with 4-digit serieal number
args=input_dir+input_dir2
files=os.listdir(args)
for file in files:
    file_t=file.split('_')# split fthe csv filename by underscores
    if len(file_t[1])==2:# if the serieal number is only 2 digits make it 4
        a1=file[:3]+'7a'+file[3:] # this is assumes this vessel has "7a" but we might hardcode this in the future
        os.rename(args+file,args+a1)
    elif len(file_t[1])>4:
        b=file_t[1][len(file_t[1])-4:]
        a=file_t[0]+'_'+b+'_'+file_t[2]+'_'+file_t[3]
        os.rename(args+file,args+a)
file_lists=glob.glob(os.path.join(args,'*.csv'))

for file_i in file_lists:
    ouput_path_name=file_i.replace(inputdir2,outputdir) # put file with new name in a new folder
    
    #find # header rows
    tem=pd.read_csv(file_i,names=name)
    Probe_Type=tem['Probe Type'] #assumes all csv files have "Probe Type," in the header
    # find the number of header lines in this file assuming "Depth"
    for i in range(len(Probe_Type)):
        if Probe_Type[i]=='Depth':
            m=i+1
            break
    
    # now, read header and data        
    df=pd.read_csv(file_i,sep=',',skiprows=m,names=name1) #data
    df_head=pd.read_csv(file_i,sep=',',nrows=m,names=name) # header only
    lats=df['lat']
    lons=df['lon']
    depth=df['Depth (m)']
    name_index=ndf['name']
    vessel=ndf['vessel1']
    lowell=df_head['Lowell']
    
    ##keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
    for i in range(1,len(lats)):
        if 'A'<=lats[i][(len(lats[i])-1):]<='Z':
            lats[i]=lats[i][:(len(lats[i])-1)]
            if len(lats[i])>9:          #the length of lat and lon is 9,if the length >9 we need fix it
                lats[i]=lats[i][1:]
        if 'A'<=lons[i][(len(lons[i])-1):]<='Z':
            lons[i]=lons[i][:(len(lons[i])-1)]
            if len(lons[i])>9:
                lons[i]=lons[i][1:]
    df['lat']=lats # replacing original lat and lon column with athe fixed ones
    df['lon']=lons
    
    #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING. If lack, fixed it
    if len(df.irow(0))==5: # some files didn't have the "DATA" in the first column
        df.insert(0,'HEADING','DATA')
    
    #check the data whether is the shoreside,if not,fix the vessel number
    count=0
    for i in range(len(depth)):
        count=count+(depth[i]>mindepth)# keep track of # of depths>10
    if count!=0:
        for i in range(0,len(vessel)):
            if name_index[i]==vessel_name:
                lowell[2]=vessel[i]
                m=i
    df_head['Lowell']=lowell
    ##determine the same name file is exist that will product,if exist,delete the same file 
    my_file=ouput_path_name
    my_file2=output_dir+'df_tem.csv'
    if os.path.exists(my_file):
        os.remove(my_file)
    else:
        print 'no such file:%s'%my_file
    if os.path.exists(my_file2):
        os.remove(my_file2)
    else:
        print 'no such file:%s'%my_file2
    insert_d=pd.DataFrame(data=[['vessel name',vessel_name,'','','','']],columns=['Probe Type','Lowell','2','3','4','5'])
    new_head=pd.concat([df_head[:2],insert_d,df_head[2:]],ignore_index=True)   #insert the vessel name into the head_file
    new_head.to_csv(ouput_path_name,index=0,header=0)
    df.to_csv(my_file2,index=0,header=0)
    #add the two file in one file
    os.system('cat '+my_file2+' >> '+ouput_path_name)
    os.remove(my_file2)
