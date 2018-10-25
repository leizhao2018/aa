# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 11:15:59 2018
-fix the filename such as 'li_da_20180608_153220.csv' to 'li_7ada_20180608_153220.csv'
-standardize the lat and lon data format 
-Is the vessel name right? not, correct it.
-the vessel name is exist? if not,insert the name
-add the VP_NUM to the header
NOTICE:THE FILE NAME CAN'T HAVE THIS STR SUCH AS ')','(',' '.
@author: leizhao
"""
import pandas as pd
import os
import zlconvertions as zl

#HARDCODES
similarity=0.7
mindepth=10 #minimum depth (meters) that a file must have to be considered usable
input_dir='/home/jmanning/leizhao/data_file/input_data/check_data'  #the directory is the boat data folder
output_dir='/home/jmanning/leizhao/data_file/output_data/data/test' #the directory is the path we need save the checed data 
vessel_number_path_file='/home/jmanning/leizhao/data_file/vessel_number.txt'      #this file contact vessel number and vessel name
raw_data_name_file='/home/jmanning/leizhao/data_file/raw_data_name.txt'  
Lowell_SN_2='7a'        #the first two letters of Lowell_SN
##################################

print 'the program is running'
#read the file of the vessel_number
vessel_number_df=pd.read_csv(vessel_number_path_file,names=['name','vessel_number']) 
raw_data_name_df=pd.read_csv(raw_data_name_file,sep='\t') 
#get all the files under the input folder
allfile_lists=zl.list_all_files(input_dir)

# produce a dataframe that use to caculate the number of items
total_df=vessel_number_df
total_df.insert(2,'file_total',0)
total_df=total_df.drop(['vessel_number'],axis=1)
total_df['name'][0]='Total'

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
    
    #find # header rows through find the number of header lines in this file assuming "Depth"
    original_file=pd.read_csv(file,nrows=12,names=['0','1','2','3','4','5'])
    for i in range(len(original_file['0'])):
        if original_file['0'][i]=='HEADING':
            header_rows=i
            break   
        
    # now, read header and data     
    df=pd.read_csv(file,sep=',',skiprows=header_rows).dropna(axis=1,how='all') #data
    
    name=['key','value','value1','value2','value3','value4']
    df_head=pd.read_csv(file,sep=',',nrows=header_rows,names=name).dropna(axis=1,how='all')  # header only
    df_headerr=pd.read_csv(file,sep=',',nrows=header_rows,names=name).dropna(axis=1,how='all')  # header only
    #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING.If lack, fixed it
    if len(df.iloc[0])==5: # some files didn't have the "DATA" in the first column
        df.insert(0,'HEADING','DATA')
    #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
    df.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data
    for i in range(0,len(df['Lat'])):
        if len(str(df['Lat'][i]).split('.')[0])>4 or 'A'<=str(df['Lat'][i]).split('.')[1][len(str(df['Lat'][i]).split('.')[1])-1:]<='Z':
            df['Lat'][i]=str(df['Lat'][i]).split('.')[0][len(str(df['Lat'][i]).split('.')[0])-4:]+'.'+str(df['Lat'][i]).split('.')[1][:4]
        if len(str(df['Lon'][i]).split('.')[0])>4 or 'A'<=str(df['Lon'][i]).split('.')[1][len(str(df['Lon'][i]).split('.')[1])-1:]<='Z':
            df['Lon'][i]=str(df['Lon'][i]).split('.')[0][len(str(df['Lon'][i]).split('.')[0])-4:]+'.'+str(df['Lon'][i]).split('.')[1][:4]
    
#    check the header file whether exist or right,if not,repair it
    header_file_fixed_type=['Date Format','Time Format','Temperature','Depth'] 
    header_file_fixed_lowell=['YYYY-MM-DD','HH24:MI:SS','C','m']
    loc=0
    EXIST=0
    for fixed_t in header_file_fixed_type:
        for k in range(len(df_head['key'])):
            if fixed_t.lower()==df_head['key'][k].lower():
                break
            else:
                EXIST=1
                count=k+1
        if EXIST==1:
            df_head=pd.concat([df_head[:count],pd.DataFrame(data=[[fixed_t,header_file_fixed_lowell[loc]]],columns=['key','value'])],ignore_index=True)
        loc=loc+1 
    #check if the data is the test data; Is the vessel number right?(test data's vessel number is 99)
    count=0
    for i in range(len(df['Depth(m)'])):  #the value of count is 0 if the data is test data
        count=count+(df['Depth(m)'][i]>mindepth)# keep track of # of depths>mindepth
    vessel_name=fpath.split('/')[len(fpath.split('/'))-1:][0] #get the vessel name
    for j in range(len(df_head['value'])):
        if df_head['key'][j].lower()=='Vessel Number'.lower():
            LOC_V_number=j
             #check and fix the vessel number              
            if count!=0:      
                for i in range(len(vessel_number_df['vessel_number'])):
                    if vessel_number_df['name'][i]==vessel_name:
                        df_head['value'][j]=str(vessel_number_df['vessel_number'][i])
                    else:
                        df_head['value'][j]=str(df_head['value'][j])
            else:   
                df_head['value'][j]='99' #the value of the vessel number is 99 if the data is test data
            break
        
    #caculate the number of every vessel and boat file
    for i in range(len(total_df['name'])):
        if total_df['name'][i].lower()==vessel_name.lower():
            total_df['file_total'][i]=total_df['file_total'][i]+1

    #if the vessel name and serial number are exist, find the location of them 
    EXIST=0 
    S_number=0  
    for k in range(len(df_head['key'])):           
        if df_head['key'][k].lower()=='Vessel Name'.lower():
            EXIST=1
            LOC_V_NAME=k
        if df_head['key'][k].lower()=='Serial Number'.lower():
            S_number=1
            loc_S_number=k
    #check and fix the vessel name and serial number 
    if S_number==0:
        df_head=pd.concat([df_head[:1],pd.DataFrame(data=[['Serial Number',new_fname.split('_')[1]]],columns=['key','value']),df_head[1:]],ignore_index=True)
        LOC_V_NAME=LOC_V_NAME+1
    if EXIST==0:
        df_head=pd.concat([df_head[:2],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['key','value']),df_head[2:]],ignore_index=True)
    if EXIST==1:
        df_head['value'][LOC_V_NAME]=vessel_name
    if S_number==1:
        if len(df_head['value'][loc_S_number].split(':'))>1:
            df_head['value'][loc_S_number]=df_head['value'][loc_S_number].replace(':','')
    
    for i in range(len(df_head['key'])):
        if df_head['key'][i].lower()=='Vessel Number'.lower():
            loc_vp_header=i+1
            break
    for i in range(len(raw_data_name_df['VESSEL_NAME'])):
        ratio=zl.str_similarity_ratio(vessel_name.lower(),raw_data_name_df['VESSEL_NAME'][i].lower())
        ratio_best=0
        if ratio>similarity:
            print ratio
            if ratio>ratio_best:
                ratio_best=ratio
                loc_vp_file=i
    new_head=pd.concat([df_head[:loc_vp_header],pd.DataFrame(data=[['VP_NUM',raw_data_name_df['VP_NUM'][loc_vp_file]]],columns=['key','value']),df_head[loc_vp_header:]],ignore_index=True)
#    new_head=df_head
    if new_head['value'][LOC_V_number]=='99':
        new_head=new_head.replace(vessel_name,'Test')
        print file     #if the file is test file,print it
        
    #creat the path and name of the new_file and the temperature file  
    output_path=fpath.replace(input_dir,output_dir)
    if not os.path.exists(output_path):   #check the path of the save file is exist,make it if not
        os.makedirs(output_path)

    path_tem_file=input_dir+'/df_tem.csv'
    new_head.to_csv(output_path+'/'+new_fname,index=0,header=0)
    df['Depth(m)'] = df['Depth(m)'].map(lambda x: '{0:.2f}'.format(float(x)))  #keep two decimal fraction
    df['Temperature(C)'] = df['Temperature(C)'].map(lambda x: '{0:.2f}'.format(float(x)))
    df['Lon'] = df['Lon'].map(lambda x: '{0:.4f}'.format(float(x)))
    df['Lat'] = df['Lat'].map(lambda x: '{0:.4f}'.format(float(x)))#keep four decimal fraction
    df.to_csv(output_path+'/df_tem.csv',index=0)  #produce the temperature file
    
   #add the two file in one file and delet the temperature file
    os.system('cat '+output_path+'/df_tem.csv'+' >> '+output_path+'/'+new_fname)
    os.remove(output_path+'/df_tem.csv')
    
#caculate the total of all files and print save as a file.
for i in range(len(total_df['file_total'])):
    total_df['file_total'][0]=total_df['file_total'][0]+total_df['file_total'][i]
total_df.to_csv(output_dir+'/items_number.txt',index=0)
