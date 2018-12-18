# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 11:15:59 2018
-fix the filename such as 'li_da_20180608_153220.csv' to 'li_7ada_20180608_153220.csv'
-standardize the lat and lon data format 
-Is the vessel name right? not, correct it.
-the vessel name is exist? if not,insert the name
NOTICE:THE FILE NAME CAN'T HAVE THIS STR SUCH AS ')','(',' '.
@author: leizhao
"""
import pandas as pd
import os
import zlconversions as zl
import rawdatamoudles as rdm
#HARDCODES
input_dir='/home/jmanning/Desktop/testout/raw_data1'  #the directory is the boat data folder and this path is the same one of output in match_data.py
output_dir='/home/jmanning/leizhao/data_file/output_data/data/test' #the directory is the path we need save the checed data 
raw_data_name_file='/home/jmanning/leizhao/data_file/raw_data_name.txt'  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
telemetry_status='/home/jmanning/leizhao/data_file/telemetry_status - fitted .csv' # download from the telementry_status in web
Lowell_SN_2='7a'        #the first two letters of Lowell_SN
similarity=0.7     #The similarity of the name in raw_file and raw_data_name.txt
mindepth=10 #minimum depth (meters) that a file must have to be considered usable
out_number=5  #this file is not test file how many data out of mindepth in the file
##################################

print ('the program is running')
#read the file of the telementry_status
telemetrystatus_df=rdm.read_telemetrystatus(telemetry_status)
# produce a dataframe that use to caculate the number of items in every boat
total_df=pd.concat([telemetrystatus_df.loc[:,['Boat']][:],pd.DataFrame(data=[['Total']],columns=['Boat'])],ignore_index=True) 
total_df.insert(1,'file_total',0)
raw_data_name_df=pd.read_csv(raw_data_name_file,sep='\t') 
#get all the files under the input folder
allfile_lists=zl.list_all_files(input_dir)
#screen out the file of '.csv',and put the path+name in the fil_lists
file_lists=[]
for file in allfile_lists:
    if file[len(file)-4:]=='.csv':
        file_lists.append(file)

#start check the data and save in the output_dir
for file in file_lists:
    fpath,fname=os.path.split(file)  #get the file's path and name
    #fix the file name
    fname=file.split('/')[len(file.split('/'))-1]
    if len(fname.split('_')[1])==2:# if the serieal number is only 2 digits make it 4
        new_fname=fname[:3]+Lowell_SN_2+fname[3:]
    else:
        new_fname=fname
    df_head=zl.nrows_len_to(file,2,name=['key','value'])    #only read header
    df_data=zl.skip_len_to(file,2) #only data
    #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING.If lack, fixed it
    if len(df_data.iloc[0])==5: # some files didn't have the "DATA" in the first column
        df_data.insert(0,'HEADING','DATA')
    #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
    df_data.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data
    for i in range(0,len(df_data)):
        if len(str(df_data['Lat'][i]).split('.')[0])>4 or 'A'<=str(df_data['Lat'][i]).split('.')[1][len(str(df_data['Lat'][i]).split('.')[1])-1:]<='Z':
            df_data['Lat'][i]=str(df_data['Lat'][i]).split('.')[0][len(str(df_data['Lat'][i]).split('.')[0])-4:]+'.'+str(df_data['Lat'][i]).split('.')[1][:4]
        if len(str(df_data['Lon'][i]).split('.')[0])>4 or 'A'<=str(df_data['Lon'][i]).split('.')[1][len(str(df_data['Lon'][i]).split('.')[1])-1:]<='Z':
            df_data['Lon'][i]=str(df_data['Lon'][i]).split('.')[0][len(str(df_data['Lon'][i]).split('.')[0])-4:]+'.'+str(df_data['Lon'][i]).split('.')[1][:4]
    df_data['Depth(m)'] = df_data['Depth(m)'].map(lambda x: '{0:.2f}'.format(float(x)))  #keep two decimal fraction
    df_data['Temperature(C)'] = df_data['Temperature(C)'].map(lambda x: '{0:.2f}'.format(float(x)))
    df_data['Lon'] = df_data['Lon'].map(lambda x: '{0:.4f}'.format(float(x)))
    df_data['Lat'] = df_data['Lat'].map(lambda x: '{0:.4f}'.format(float(x)))#keep four decimal fraction
    #check the header file whether exist or right,if not,repair it
    header_file_fixed_key=['Date Format','Time Format','Temperature','Depth'] 
    header_file_fixed_value=['YYYY-MM-DD','HH24:MI:SS','C','m']
    loc_fixed_value=0
    
    for fixed_t in header_file_fixed_key:
        for k in range(len(df_head)):
            if fixed_t.lower()==df_head['key'][k].lower():
                EXIST=0
                break
            else:
                EXIST=1
        if EXIST==1:
            df_head=pd.concat([df_head[:],pd.DataFrame(data=[[fixed_t,header_file_fixed_value[loc_fixed_value]]],columns=['key','value'])],ignore_index=True)
        loc_fixed_value=loc_fixed_value+1 
    #check if the data is the test data; Is the vessel number right?(test data's vessel number is 99)
    count=0
    for i in range(len(df_data)):  #the value of count is 0 if the data is test data
        count=count+(float(df_data['Depth(m)'][i])>mindepth)# keep track of # of depths>mindepth
        if count>out_number:
            break
    vessel_name=fpath.split('/')[len(fpath.split('/'))-1:][0] #get the vessel name
    
    Serial_number_exist=0
    vessel_name_exist=0
    for j in range(len(df_head)):
        if df_head['key'][j].lower()=='Vessel Number'.lower():
            LOC_Vessel_number=j  #the location of vessel_number in df_head            
            if count!=0:      
                for i in range(len(telemetrystatus_df)):
                    if telemetrystatus_df['Boat'][i]==vessel_name:
                        df_head['value'][j]=str(telemetrystatus_df['Vessel#'][i])
                        break
                    else:
                        continue
            else:   
                df_head['value'][j]='99' #the value of the vessel number is 99 if the data is test data
        if df_head['key'][j].lower()=='Serial Number'.lower():
            Serial_number_exist=1
            if len(df_head['value'][j].split(':'))>1:
                df_head['value'][j]=df_head['value'][j].replace(':','')            
        if df_head['key'][j].lower()=='Vessel Name'.lower():
            df_head['value'][j]==vessel_name
            vessel_name_exist=1
        
    #fix the vessel name and serial number 
    if Serial_number_exist==0:
        df_head=pd.concat([df_head[:1],pd.DataFrame(data=[['Serial Number',new_fname.split('_')[1]]],columns=['key','value']),df_head[1:]],ignore_index=True)
    if vessel_name_exist==0:
        df_head=pd.concat([df_head[:2],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['key','value']),df_head[2:]],ignore_index=True)   
    if df_head['value'][LOC_Vessel_number]=='99':
        df_head=df_head.replace(vessel_name,'Test')
        
    #find where to put vp_num in df_head
    for i in range(len(df_head)):
        if df_head['key'][i].lower()=='Vessel Number'.lower():
            loc_vp_num=i+1  #put vp_num behind the vessel number
            break
    #find the location of this vessel name in raw_data_name_df 
    for i in range(len(raw_data_name_df)):
        ratio=zl.str_similarity_ratio(vessel_name.lower(),raw_data_name_df['VESSEL_NAME'][i].lower()) #caculat the similarity of two variable
        ratio_best=0
        if ratio>similarity:
            if ratio>ratio_best:
                ratio_best=ratio
                loc_vp_file=i  
    df_head=pd.concat([df_head[:loc_vp_num],pd.DataFrame(data=[['VP_NUM',raw_data_name_df['VP_NUM'][loc_vp_file]]],columns=['key','value']),df_head[loc_vp_num:]],ignore_index=True)
    
    #caculate the number of every vessel and boat file
    for i in range(len(total_df)):
        if total_df['Boat'][i].lower()==vessel_name.lower():
            total_df['file_total'][i]=total_df['file_total'][i]+1
        
    #creat the path and name of the new_file and the temperature file  
    output_path=fpath.replace(input_dir,output_dir)
    if not os.path.exists(output_path):   #check the path of the save file is exist,make it if not
        os.makedirs(output_path)

    df_head.to_csv(output_path+'/'+new_fname,index=0,header=0)
    
    df_data.to_csv(output_path+'/df_tem.csv',index=0)  #produce the temperature file
   #add the two file in one file and delet the temperature file
    os.system('cat '+output_path+'/df_tem.csv'+' >> '+output_path+'/'+new_fname)
    os.remove(output_path+'/df_tem.csv')
    
#caculate the total of all files and print save as a file.
for i in range(len(total_df)):
    total_df['file_total'][0]=total_df['file_total'][0]+total_df['file_total'][i]
total_df.to_csv(output_dir+'/items_number.txt',index=0)
