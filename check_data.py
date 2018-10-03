# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 11:15:59 2018
-fix the filename such as 'li_da_20180608_153220.csv' to 'li_7ada_20180608_153220.csv'
-standardize the lat and lon data format 
-Is the name_vessel right? not, correct it.
-the vessel name is exist? if not,insert the name
NOTICE:THE FILE NAME CAN'T HAVE THIS STR SUCH AS ')','(',' '.
@author: leizhao
modefied in Oct 3,2018
"""
import pandas as pd
import os
import glob

#HARDCODES
mindepth=10 #minimum depth (meters) that a file must have to be considered usable
input_dir='/home/jmanning/leizhao/data_file/'  #the directory is the boat data folder and the file that contact name with vessel folder 
input_dir2='input_data/boat_data_needs_upload/data_on_Lisa_ann_3/towifi/' #the direction of the boat data
vessel_number_file='vessel_number.txt'      #name contact vessel number file this file has vessel_# and vessel name colums
vessel_name='Lisa_Ann_III'          # the data from whose boat
Lowell_SN_2='7a'        #the first two letters of Lowell_SN
##################################

print 'the program is running'
vessel_number_df=pd.read_csv(input_dir+vessel_number_file,names=['name','vessel_number']) 
#get the files path and name
file_lists=glob.glob(os.path.join(input_dir+input_dir2,'*.csv'))
for file in file_lists:
    
    #fix the file name
    if len(file.split('/')[len(file.split('/'))-1].split('_')[1])==2:# if the serieal number is only 2 digits make it 4
        new_file=file.split('/')[len(file.split('/'))-1][:3]+Lowell_SN_2+file.split('/')[len(file.split('/'))-1][3:]   
    else:
        new_file=file.split('/')[len(file.split('/'))-1]
    
    #find # header rows through find the number of header lines in this file assuming "Depth"
    original_file=pd.read_csv(file,nrows=12,names=['0','1','2','3','4','5'])
    for i in range(len(original_file['0'])):
        if original_file['0'][i]=='Depth':
            header_rows=i  
    # now, read header and data     
    df=pd.read_csv(file,sep=',',skiprows=header_rows+1).dropna(axis=1,how='all') #data
    name=['key','value','value1','value2','value3','value4']
    df_head=pd.read_csv(file,sep=',',nrows=header_rows,names=name).dropna(axis=1,how='all') # header only

    #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING. If lack, fixed it
    if len(df.iloc[0])==5: # some files didn't have the "DATA" in the first column
        df.insert(0,'HEADING','DATA')
    #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
    for i in range(0,len(df['lat'])):
        if len(str(df['lat'][i]).split('.')[0])>4 or 'A'<=str(df['lat'][i]).split('.')[1][len(str(df['lat'][i]).split('.')[1])-1:]<='Z':
            df['lat'][i]=str(df['lat'][i]).split('.')[0][len(str(df['lat'][i]).split('.')[0])-4:]+'.'+str(df['lat'][i]).split('.')[1][:4]
        if len(str(df['lon'][i]).split('.')[0])>4 or 'A'<=str(df['lon'][i]).split('.')[1][len(str(df['lon'][i]).split('.')[1])-1:]<='Z':
            df['lon'][i]=str(df['lon'][i]).split('.')[0][len(str(df['lon'][i]).split('.')[0])-4:]+'.'+str(df['lon'][i]).split('.')[1][:4]
    
    #check the data whether is the shoreside,if not,fix the vessel number
    count=0
    for i in range(len(df['Depth (m)'])):
        count=count+(df['Depth (m)'][i]>mindepth)# keep track of # of depths>10
    for j in range(len(df_head['value'])):
        if df_head['key'][j]=='Vessel Number' or df_head['key'][j]=='vessel number':
            LOC_V_number=j               
            if count!=0:       #if data is shoreside,the number is 99;if not through the vessel_number to repaire it 
                for i in range(len(vessel_number_df['vessel_number'])):
                    if vessel_number_df['name'][i]==vessel_name:
                        df_head['value'][j]=vessel_number_df['vessel_number'][i]
            else:
                df_head['value'][j]='99'
            break
    
    #check the vessel name whether exist or right,if not,repair it
    EXIST=0   
    #if the  vessel name s exist, find the location    
    for k in range(len(df_head['key'])):           
        if df_head['key'][k]=='Vessel Name' or df_head['key'][k]=='vessel name':
            EXIST=1
            LOC_V_NAME=k
            break
    #fix the vessel name 
    
    if EXIST==1:
        df_head['value'][LOC_V_NAME]=vessel_name
        new_head=df_head
    else:
        
        new_head=pd.concat([df_head[:LOC_V_number+1],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['Probe Type','Lowell']),df_head[LOC_V_number+1:]],ignore_index=True)
    if new_head['value'][LOC_V_number]=='99':
        new_head=new_head.replace(vessel_name,'Test')
        print file       #if the file is test file,print it
      
    #creat the path and name of the new_file and the temperature file  
    output_path_name=file.replace(input_dir2,'output_data/data/'+vessel_name+'/').replace(file.split('/')[len(file.split('/'))-1],new_file)#the output path and the print file. 
    path_tem_file=input_dir+'df_tem.csv'
    new_head.to_csv(output_path_name,header=0,index=0)
    df.to_csv(path_tem_file,index=0)
   
   #add the two file in one file
    os.system('cat '+path_tem_file+' >> '+output_path_name)
    os.remove(path_tem_file)
