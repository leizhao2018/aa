#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 12:07:44 2018

@author: jmanning
"""
import ftplib
import os
import glob
import datetime
import zlconversions as zl
import pandas as pd
from pylab import mean, std
import matplotlib.pyplot as plt
import conversions as cv

#Hardcods
path_picture_save='/home/jmanning/Desktop/'
download_path='/home/jmanning/Desktop/testout/raw_data/' # the path that we will need save the download files
main_directory='/home/jmanning/leizhao/data_file/'
start_time='2018-11-1'   #file start time
time_interval=168    #unit: hours
mindepth=10 #minimum depth (meters) that a file must have to be considered usable
acceptable_time_diff=datetime.timedelta(minutes=20)  #unite:mintues
acceptable_distance_diff=2  #unit: miles
############
vessel_number_path_file=main_directory+'vessel_number.txt'      #this file contact vessel number and vessel name
telemetry_status=main_directory+'telemetry_status.csv'   
raw_data_name_file=main_directory+'raw_data_name.txt'
##########

vessel_number_df=pd.read_csv(vessel_number_path_file) #read the file of the vessel_number
raw_data_name_df=pd.read_csv(raw_data_name_file,sep='\t') #read the file of the raw_data_name_file
record_file=vessel_number_df.reindex(columns=['name','vessel','matched_number','file_number','tele_num',\
                                              'average_diff_depth','average_diff_temp','max_diff_depth','min_diff_depth',\
                                              'max_diff_temp','min_diff_temp','sum_diff_depth','sum_diff_temp'],fill_value=None)

##download the raw_data
ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
print ('Logging in.')
ftp.cwd('/Matdata')
print ('Accessing files')
filenames = ftp.nlst() # get filenames within the directory OF REMOTE MACHINE
start_time_gmt=zl.local2utc(datetime.datetime.strptime(start_time,'%Y-%m-%d'))  #time tranlate from local to UTC
# MAKE THIS A LIST OF FILENAMES THAT WE NEED DOWNLOAD
download_files=[]
for file in filenames:
    if len(file.split('_'))==4:
        if start_time_gmt<=datetime.datetime.strptime(file.split('_')[2]+file.split('_')[3].split('.')[0],\
                                                      '%Y%m%d%H%M%S')<start_time_gmt+datetime.timedelta(\
                                                                    hours=time_interval):
            download_files.append(file)
for filename in download_files: # DOWNLOAD FILES   
    local_filename = os.path.join(download_path, filename)
    file = open(local_filename, 'wb')
    ftp.retrbinary('RETR '+ filename, file.write)
    file.close()
ftp.quit() # This is the “polite” way to close a connection
print ('New files downloaded')
file_lists=glob.glob(os.path.join(download_path,'*.csv')) #get the path and name of the file that need to match

#download the data of telementry
tele_df=pd.read_csv('https://www.nefsc.noaa.gov/drifter/emolt.dat',names=['a'])
tele_data=pd.DataFrame(data=None,columns=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])

#fix the format of telementry data that downloaded just now
for i in range(len(tele_df)):
    #downloaded data have many space, we didn't need this, so we need dele
    v_line=tele_df['a'][i].replace('       ',' ').replace('      ',' ').replace('     ',' ').replace('    ',\
                  ' ').replace('   ',' ').replace('  ',' ').replace(' ',' ').split(' ')      
    if v_line[0]=='':
        tele_time=datetime.datetime.strptime(str(v_line[17])+'-'+str(v_line[3])+'-'+str(v_line[4])+' '+str(v_line[5])+':'+str(v_line[6])+':'+'00','%Y-%m-%d %H:%M:%S')
        if start_time_gmt<=tele_time<start_time_gmt+datetime.timedelta(hours=time_interval):
            exist_file_tele=pd.DataFrame(data=[[v_line[1],v_line[2],v_line[3],v_line[4],v_line[5],v_line[6],v_line[7],v_line[8],v_line[9],v_line[10],\
                                                v_line[11],v_line[12],v_line[13],v_line[14],v_line[15],v_line[16],v_line[17]]],columns=['vessel_n',\
    'esn','month','day','Hours','minates','fracyrday','lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
            tele_data=tele_data.append(exist_file_tele)
    else:
        tele_time=datetime.datetime.strptime(str(v_line[16])+'-'+str(v_line[2])+'-'+str(v_line[3])+' '+str(v_line[4])+':'+str(v_line[5])+':'+'00','%Y-%m-%d %H:%M:%S')
        if start_time_gmt<=tele_time<start_time_gmt+datetime.timedelta(hours=time_interval):
            exist_file_tele=pd.DataFrame(data=[[v_line[0],v_line[1],v_line[2],v_line[3],v_line[4],v_line[5],v_line[6],v_line[7],v_line[8],v_line[9],\
                                                v_line[10],v_line[11],v_line[12],v_line[13],v_line[14],v_line[15],v_line[16]]],columns=['vessel_n',\
    'esn','month','day','Hours','minates','fracyrday','lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
            tele_data=tele_data.append(exist_file_tele)
tele_data.index=range(len(tele_data))

if len(tele_data)==0 and len(file_lists):
    print('please check the data website of telementry and raw_data!')
elif len(tele_data)==0:
    print('please check the data website of telementry!')
elif len(file_lists)==0:
    print('please check the data website of raw_data!')
else:
    #read the file of the telementry_status
    data=pd.read_csv(telemetry_status)
    #find the data lines number in the file('telemetry_status.csv')
    for i in range(len(data['Boat'])):
        if data['Boat'][i]=='Status Codes:':
            data_line_number=i     
    #read the data about "telemetry_status.csv"
    telementrystatus_df=pd.read_csv(telemetry_status,nrows=data_line_number)

    #fix the format of time about logger_change
    for i in range(data_line_number):
        if telementrystatus_df['logger_change'][i]==telementrystatus_df['logger_change'][i]:
            date_logger_change=telementrystatus_df['logger_change'][i].split(',')   #get the time data of the logger_change
            for j in range(0,len(date_logger_change)):
                if len(date_logger_change[j])>4:     #keep the date have the month and year such as 1/17
                    date_logger_change[j]=zl.transform_date(date_logger_change[j]) #use the transform_date(date) to fix the date
            telementrystatus_df['logger_change'][i]=date_logger_change
        else:
            continue

    #check the column of Lowell-SN, if the data is not exist, give a value('0') 
    for i in range(len(telementrystatus_df['Lowell-SN'])):
        if telementrystatus_df['Lowell-SN'][i]==telementrystatus_df['Lowell-SN'][i]:
            telementrystatus_df['Lowell-SN'][i]=telementrystatus_df['Lowell-SN'][i].replace(' ','') # delete all spaces in every line
        else:
            telementrystatus_df['Lowell-SN'][i]='0'  
    for i in range(len(telementrystatus_df['logger_change'])):
        if telementrystatus_df['logger_change'][i]==telementrystatus_df['logger_change'][i]:
            telementrystatus_df['logger_change'][i]=telementrystatus_df['logger_change'][i] # delete all spaces in every line
        else:
            telementrystatus_df['logger_change'][i]='0' 

    #match the file
    index=vessel_number_df['name']
    dict={}    #the dictinary about raw data
    tele_dict={}  #the dictionary about telementry data
    for i in range(len(index)):
        dict[index[i]]=pd.DataFrame(data=None,columns=['time','filename','mean_temp','mean_depth'])
        tele_dict[index[i]]=pd.DataFrame(data=None,columns=['time','mean_temp','mean_depth'])
    for file in file_lists:
        fpath,fname=os.path.split(file)  #get the file's path and name
        #find # header rows through find the number of header lines in this file assuming "Depth"
        original_file=pd.read_csv(file,nrows=12,names=['0','1','2','3','4','5'])
        for i in range(len(original_file['0'])):
            if original_file['0'][i]=='HEADING':
                header_rows=i
                break   
        # now, read header and data of every file  
        file_header=zl.nrows_to(file,'Depth',['Probe Type','Lowell'])  #only get the header
        df=pd.read_csv(file,sep=',',skiprows=header_rows).dropna(axis=1,how='all') #data
        #get the vessel number of every file
        for i in range(len(file_header)):
            if file_header['Probe Type'][i].lower()=='vessel number'.lower():
                vessel_number=int(file_header['Lowell'][i])
                break
        if len(df.iloc[0])==5: # some files didn't have the "DATA" in the first column
            df.insert(0,'HEADING','DATA') 
        df.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data  
        count_large_mindepth=0
        for i in range(len(df['Depth(m)'])):
            if df['Depth(m)'][i]<mindepth:
                count_large_mindepth=0
            else:
                count_large_mindepth=1
                break
        if count_large_mindepth==0:
            continue
            
        #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000  
        for i in range(0,len(df['Lat'])):
            if len(str(df['Lat'][i]).split('.')[0])>4 or 'A'<=str(df['Lat'][i]).split('.')[1][len(str(df['Lat'][i]).split('.')[1])-1:]<='Z':
                df['Lat'][i]=str(df['Lat'][i]).split('.')[0][len(str(df['Lat'][i]).split('.')[0])-4:]+'.'+str(df['Lat'][i]).split('.')[1][:4]
            if len(str(df['Lon'][i]).split('.')[0])>4 or 'A'<=str(df['Lon'][i]).split('.')[1][len(str(df['Lon'][i]).split('.')[1])-1:]<='Z':
                df['Lon'][i]=str(df['Lon'][i]).split('.')[0][len(str(df['Lon'][i]).split('.')[0])-4:]+'.'+str(df['Lon'][i]).split('.')[1][:4]
        for i in range(len(record_file)):
            if record_file['vessel'][i]==vessel_number:
                if record_file['file_number'][i]==record_file['file_number'][i]:
                    record_file['file_number'][i]=record_file['file_number'][i]+1  #caculate the number of files in every vessel
                else:
                    record_file['file_number'][i]=1
        #caculate the mean temperature and depth of every file
        dft=df.ix[(df['Depth(m)']>0.85*mean(df['Depth(m)']))]  #filter the data
        dft=dft.ix[2:]   #delay several minutes to let temperature sensor record the real bottom temp
        dft=dft.ix[(dft['Temperature(C)']>mean(dft['Temperature(C)'])-3*std(dft['Temperature(C)'])) & \
                   (dft['Temperature(C)']<mean(dft['Temperature(C)'])+3*std(dft['Temperature(C)']))]  #Excluding gross error
        dft.index = range(len(dft))  #reindex
        mean_temp=str(int(round(mean(dft['Temperature(C)'][1:len(dft)]),2)*100)).zfill(4)
        mean_depth=str(abs(int(round(mean(dft['Depth(m)'].values))))).zfill(3)   #caculate the mean depth
        
        #match rawdata and telementry data
        #time conversion, GMT time to local time
        date=fname.split('.')[0].split('_')[2] # get the gmt date,that date we get the current file
        time=fname.split('.')[0].split('_')[3] #get the gmt_time of current file,that time we start get the data
        date_date=zl.gmt_to_eastern(date[0:4]+'-'+date[4:6]+'-'+date[6:8]+' '+time[0:2]+':'+time[2:4]+':'+time[4:6]) #get the local date that date we get the current file
        file_time=datetime.datetime.strptime(date[0:4]+'-'+date[4:6]+'-'+date[6:8]+' '+time[0:2]+':'+time[2:4]+':'+time[4:6],"%Y-%m-%d %H:%M:%S")
        lat,lon=cv.dm2dd(dft['Lat'][len(dft)-1],dft['Lon'][len(dft)-1]) #Convert latitude and longitude
        #caculate the numbers of successful matchs
        #caculate the minimum,maximum and average different of temperature and depth
        for i in range(len(tele_data)):
            if tele_data['vessel_n'][i].split('_')[1]==str(vessel_number):     
                tele_time=datetime.datetime.strptime(str(tele_data['year'][i])+'-'+str(tele_data['month'][i])+'-'+str(tele_data['day'][i])+\
                                                     ' '+str(tele_data['Hours'][i])+':'+str(tele_data['minates'][i])+':'+'00','%Y-%m-%d %H:%M:%S')
                if abs(tele_time-file_time)<=acceptable_time_diff:  #time match
                    if zl.dist(lat1=lat,lon1=lon,lat2=float(tele_data['lat'][i]),lon2=float(tele_data['lon'][i]))<=acceptable_distance_diff:  #distance match               
                        for j in range(len(record_file)):
                            if record_file['vessel'][j]==vessel_number:
                                if record_file['matched_number'][j]==record_file['matched_number'][j]:
                                    record_file['matched_number'][j]=record_file['matched_number'][j]+1
                                    record_file['sum_diff_temp'][j]=record_file['sum_diff_temp'][j]+abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    record_file['sum_diff_depth'][j]=record_file['sum_diff_depth'][j]+abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    if record_file['max_diff_temp'][j]<abs(float(mean_temp)/100-float(tele_data['temp'][i])):
                                        record_file['max_diff_temp'][j]=abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    if record_file['min_diff_temp'][j]>abs(float(mean_temp)/100-float(tele_data['temp'][i])):
                                        record_file['min_diff_temp'][j]=abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    if record_file['max_diff_depth'][j]<abs(float(mean_depth)-float(tele_data['depth'][i])):
                                        record_file['max_diff_depth'][j]=abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    if record_file['min_diff_depth'][j]>abs(float(mean_depth)-float(tele_data['depth'][i])):
                                        record_file['min_diff_depth'][j]=abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    break
                                else:
                                    record_file['matched_number'][j]=1
                                    record_file['sum_diff_temp'][j]=abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    record_file['max_diff_temp'][j]=abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    record_file['min_diff_temp'][j]=abs(float(mean_temp)/100-float(tele_data['temp'][i]))
                                    record_file['sum_diff_depth'][j]=abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    record_file['max_diff_depth'][j]=abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    record_file['min_diff_depth'][j]=abs(float(mean_depth)-float(tele_data['depth'][i]))
                                    break
        #math the SN and date
        for i in range(len(telementrystatus_df['Lowell-SN'])):
            #we will enter the next line if SN or date is not exist
            if telementrystatus_df['Lowell-SN'][i]==telementrystatus_df['Lowell-SN'][i] and telementrystatus_df['logger_change'][i]==telementrystatus_df['logger_change'][i]:  
                for j in range(len(telementrystatus_df['Lowell-SN'][i].split(','))):   
                    fname_len_SN=len(fname.split('_')[1]) #the length of SN in the file name
                    len_SN=len(telementrystatus_df['Lowell-SN'][i].split(',')[j]) #the length of SN in the culumn of the Lowell-SN inthe file of the telemetry_status.csv
                    if telementrystatus_df['Lowell-SN'][i].split(',')[j][len_SN-fname_len_SN:]==fname.split('_')[1]:

                        if j<len(telementrystatus_df['logger_change'][i])-1:
                            if telementrystatus_df['logger_change'][i][j]<=date_date.strftime("%Y%m%d")<=telementrystatus_df['logger_change'][i][j+1]:
                                dict[telementrystatus_df['Boat'][i]]=dict[telementrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[date_date,\
                                    fname,float(mean_temp)/100.0,float(mean_depth)]],columns=['time','filename','mean_temp','mean_depth']).iloc[0])

                        else:
                            if telementrystatus_df['logger_change'][i][j]<=date_date.strftime("%Y%m%d"):
                                dict[telementrystatus_df['Boat'][i]]=dict[telementrystatus_df['Boat'][i]].append(pd.DataFrame(data=[[date_date,\
                                    fname,float(mean_temp)/100.0,float(mean_depth)]],columns=['time','filename','mean_temp','mean_depth']).iloc[0])   

    #clasify the telementry data to every raw             
    for i in range(len(tele_data)):
        for j in range(len(vessel_number_df)):
            if int(tele_data['vessel_n'][i].split('_')[1])==vessel_number_df['vessel'][j]:
                date_time=datetime.datetime.strptime(tele_data['year'][i]+'-'+tele_data['month'][i]+'-'+tele_data['day'][i]+' '+\
                                                     tele_data['Hours'][i]+':'+tele_data['minates'][i],'%Y-%m-%d %H:%M')
                tele_dict[vessel_number_df['name'][j]]=tele_dict[vessel_number_df['name'][j]].append(pd.DataFrame(data=[[date_time,\
                         float(tele_data['temp'][i]),float(tele_data['depth'][i])]],columns=['time','mean_temp','mean_depth']).iloc[0])
    #draw the picture
    for i in index:
        for j in range(len(record_file)):
            if i.lower()==record_file['name'][j].lower():
                break
        dict[i]=dict[i].sort_values(by=['time'])
        dict[i].index=range(len(dict[i]))
        tele_dict[i]=tele_dict[i].sort_values(by=['time'])
        tele_dict[i].index=range(len(tele_dict[i]))
        if len(dict[i])==0 and len(tele_dict[i])==0:
            continue
        elif len(tele_dict[i])>0 and len(dict[i])>0:
            fig=plt.figure()
            fig.suptitle(i+'\n'+'successfully matched:'+str(record_file['matched_number'][j])+'   tele_num:'+\
                         str(len(tele_dict[i]))+'   raw_num:'+str(record_file['file_number'][j]),fontsize=8, fontweight='bold')
            ax2=fig.add_subplot(212)
            ax2.plot_date(dict[i]['time'],dict[i]['mean_temp'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
            ax2.plot_date(tele_dict[i]['time'],tele_dict[i]['mean_temp'],linestyle='-',alpha=0.5,label='telementry',marker='^')
            ax2.set_title('temperature different of min:'+str(round(record_file['min_diff_temp'][j],2))+'  max:'+str(round(record_file['max_diff_temp'][j],2))+\
                          '  average:'+str(round(record_file['sum_diff_temp'][j]/float(record_file['matched_number'][j]),3)))
            ax2.legend()
            ax2.set_ylabel('C')
            ax2.axes.title.set_size(8)
            ax2.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            fig.autofmt_xdate(bottom=0.18)
            fig.subplots_adjust(left=0.18)
            ax1=fig.add_subplot(211)
            ax1.plot_date(dict[i]['time'],dict[i]['mean_depth'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
            ax1.plot_date(tele_dict[i]['time'],tele_dict[i]['mean_depth'],linestyle='-',alpha=0.5,label='telementry',marker='^')
            ax1.set_title('depth different of min:'+str(round(record_file['min_diff_depth'][j],2))+'  max:'+str(round(record_file['max_diff_depth'][j],2))+\
                          '  average:'+str(round(record_file['sum_diff_depth'][j]/float(record_file['matched_number'][j]),3)))
            ax1.legend()
            ax1.set_ylabel('depth(m)',fontsize=10)
            ax1.axes.get_xaxis().set_visible(False)
            ax1.axes.title.set_size(8)
            ax1.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            plt.savefig(path_picture_save+i+start_time+'.png',dpi=300)
        elif len(dict[i])==0:
            fig=plt.figure()
            fig.suptitle(i+'\n'+'successfully matched:'+str(record_file['matched_number'][j])+'   tele_num:'+\
                         str(len(tele_dict[i]))+'   raw_num:'+str(record_file['file_number'][j]),fontsize=8, fontweight='bold')
            ax2=fig.add_subplot(212)
            ax2.plot_date(tele_dict[i]['time'],tele_dict[i]['mean_temp'],linestyle='-',alpha=0.5,label='telementry',marker='^')
            ax2.legend()
            ax2.set_ylabel('C')
            ax2.axes.title.set_size(8)
            ax2.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            fig.autofmt_xdate(bottom=0.18)
            fig.subplots_adjust(left=0.18)
            ax1=fig.add_subplot(211)
            ax1.plot_date(tele_dict[i]['time'],tele_dict[i]['mean_depth'],linestyle='-',alpha=0.5,label='telementry',marker='^')
            ax1.legend()
            ax1.set_ylabel('depth(m)',fontsize=10)
            ax1.axes.get_xaxis().set_visible(False)
            ax1.axes.title.set_size(8)
            ax1.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            plt.savefig(path_picture_save+i+start_time+'.png',dpi=300)
        else:
            fig=plt.figure()
            fig.suptitle(i+'\n'+'successfully matched:'+str(record_file['matched_number'][j])+'   tele_num:'+\
                         str(len(tele_dict[i]))+'   raw_num:'+str(record_file['file_number'][j]),fontsize=8, fontweight='bold')
            ax2=fig.add_subplot(212)
            ax2.plot_date(dict[i]['time'],dict[i]['mean_temp'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
            ax2.legend()
            ax2.set_ylabel('C')
            ax2.axes.title.set_size(8)
            ax2.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            fig.autofmt_xdate(bottom=0.18)
            fig.subplots_adjust(left=0.18)
            ax1=fig.add_subplot(211)
            ax1.plot_date(dict[i]['time'],dict[i]['mean_depth'],linestyle='-',alpha=0.5,label='raw_data',marker='d')
            ax1.legend()
            ax1.set_ylabel('depth(m)',fontsize=10)
            ax1.axes.get_xaxis().set_visible(False)
            ax1.axes.title.set_size(8)
            ax1.set_xlim(datetime.datetime.strptime(start_time,'%Y-%m-%d'),datetime.datetime.strptime(start_time,'%Y-%m-%d')+datetime.timedelta(hours=time_interval))
            plt.savefig(path_picture_save+i+start_time+'.png',dpi=300)   
    record_file.to_csv('/home/jmanning/leizhao/'+'record_file1.csv',index=0)
