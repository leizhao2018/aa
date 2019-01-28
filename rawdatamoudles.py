
"""
Created on Wed Oct  3 12:39:15 2018

@author: leizhao
"""
"""
Created on Wed Oct  3 12:39:15 2018

@author: leizhao
"""
import conversions as cv
import ftplib
import glob
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys
import zlconversions as zl
from datetime import datetime,timedelta
from pylab import mean, std
#HARDCODES

    
def check_reformat_data(input_dir,output_dir,telemetry_status_file,raw_data_name_file,Lowell_SN_2='7a',similarity=0.7,mindepth=10):
    #read the file of the vessel_number
    telemetrystatus_df=read_telemetrystatus(telemetry_status_file)
    raw_data_name_df=pd.read_csv(raw_data_name_file,sep='\t') 

    #produce a dataframe that use to caculate the number of items
    total_df=pd.concat([telemetrystatus_df.loc[:,['Boat']][:],pd.DataFrame(data=[['Total']],columns=['Boat'])],ignore_index=True)
    total_df.insert(1,'file_total',0)
    #get all the files under the input folder
    #screen out the file of '.csv',and put the path+name in the fil_lists
    allfile_lists=zl.list_all_files(input_dir)
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
        # now, read header and data
        df_head=zl.nrows_len_to(file,2,name=['key','value'])
        df=zl.skip_len_to(file,2) #data
        #the standard data have 6 columns, sometimes the data possible lack of the column of the HEADING.If lack, fixed it
        if len(df.iloc[0])==5: # some files didn't have the "DATA" in the first column
            df.insert(0,'HEADING','DATA')
        df.columns = ['HEADING','Datet(GMT)','Lat','Lon','Temperature(C)','Depth(m)']  #rename the name of conlum of data
        #keep the lat and lon data format is right,such as 00000.0000w to 0000.0000
        for i in range(0,len(df['Lat'])):
            df['Lat'][i]=format_lat_lon(df['Lat'][i])
            df['Lon'][i]=format_lat_lon(df['Lon'][i])
        #check if the data is the test data; Is the vessel number right?(test data's vessel number is 99)
        df['Depth(m)'] = df['Depth(m)'].map(lambda x: '{0:.2f}'.format(float(x)))  #keep two decimal fraction
        df['Temperature(C)'] = df['Temperature(C)'].map(lambda x: '{0:.2f}'.format(float(x)))
        df['Lon'] = df['Lon'].map(lambda x: '{0:.4f}'.format(float(x)))
        df['Lat'] = df['Lat'].map(lambda x: '{0:.4f}'.format(float(x)))#keep four decimal fraction

        count=0
        for i in range(len(df['Depth(m)'])):  #the value of count is 0 if the data is test data
            count=count+(float(df['Depth(m)'][i])>mindepth)# keep track of # of depths>mindepth
            if count>5:
                break
        vessel_name=fpath.split('/')[len(fpath.split('/'))-1:][0] #get the vessel name
        for j in range(len(df_head)):
            if df_head['key'][j].lower()=='Vessel Number'.lower():
                LOC_V_number=j
                #check and fix the vessel number              
                if count!=0:      
                    for i in range(len(telemetrystatus_df)):
                        if telemetrystatus_df['Boat','Vessel#'][i]==vessel_name:
                            df_head['value'][j]=str(telemetrystatus_df['Vessel#'][i])
                            break
                        else:
                            continue
                else:   
                    df_head['value'][j]='99' #the value of the vessel number is 99 if the data is test data
                break
        if df_head['value'][LOC_V_number]=='99':
            df_head=df_head.replace(vessel_name,'Test')
            print (file)     #if the file is test file,print it
        #check the header file whether exist or right,if not,repair it
        header_file_fixed_key=['Date Format','Time Format','Temperature','Depth'] 
        header_file_fixed_value=['YYYY-MM-DD','HH24:MI:SS','C','m']
        loc=0
        EXIST=0
        for fixed_t in header_file_fixed_key:
            for k in range(len(df_head['key'])):
                if fixed_t.lower()==df_head['key'][k].lower():
                    break
                else:
                    EXIST=1
                    count=k+1
            if EXIST==1:
                df_head=pd.concat([df_head[:count],pd.DataFrame(data=[[fixed_t,header_file_fixed_value[loc]]],columns=['key','value'])],ignore_index=True)
            loc=loc+1 
        #caculate the number of every vessel and boat file
        for i in range(len(total_df['Boat'])):
            if total_df['Boat'][i].lower()==vessel_name.lower():
                total_df['file_total'][i]=total_df['file_total'][i]+1



        #if the vessel name and serial number are exist, find the location of them 
        vessel_name_EXIST=0 
        S_number_EXIST=0  
        for k in range(len(df_head['key'])):           
            if df_head['key'][k].lower()=='Vessel Name'.lower():
                vessel_name_EXIST=1
                df_head['value'][k]=vessel_name
            if df_head['key'][k].lower()=='Serial Number'.lower():
                if len(df_head['value'][k].split(':'))>1:
                    df_head['value'][k]=df_head['value'][k].replace(':','')
                S_number_EXIST=1
        #check and fix the vessel name and serial number 
        if S_number_EXIST==0:
            df_head=pd.concat([df_head[:1],pd.DataFrame(data=[['Serial Number',new_fname.split('_')[1]]],columns=['key','value']),df_head[1:]],ignore_index=True)
        if vessel_name_EXIST==0:#
            df_head=pd.concat([df_head[:2],pd.DataFrame(data=[['Vessel Name',vessel_name]],columns=['key','value']),df_head[2:]],ignore_index=True)


        for i in range(len(df_head['key'])):
            if df_head['key'][i].lower()=='Vessel Number'.lower():
                loc_vp_header=i+1
                break
        for i in range(len(raw_data_name_df['VESSEL_NAME'])):
            ratio=zl.str_similarity_ratio(vessel_name.lower(),raw_data_name_df['VESSEL_NAME'][i].lower())
            ratio_best=0
            if ratio>similarity:
                if ratio>ratio_best:
                    ratio_best=ratio
                    loc_vp_file=i
        df_head=pd.concat([df_head[:loc_vp_header],pd.DataFrame(data=[['VP_NUM',raw_data_name_df['VP_NUM'][loc_vp_file]]],columns=['key','value']),df_head[loc_vp_header:]],ignore_index=True)
        #creat the path and name of the new_file and the temperature file  
        output_path=fpath.replace(input_dir,output_dir)
        if not os.path.exists(output_path):   #check the path of the save file is exist,make it if not
            os.makedirs(output_path)
        df_head.to_csv(output_path+'/'+new_fname,index=0,header=0)
        df.to_csv(output_path+'/df_tem.csv',index=0)  #produce the temperature file  
        #add the two file in one file and delet the temperature file
        os.system('cat '+output_path+'/df_tem.csv'+' >> '+output_path+'/'+new_fname)
        os.remove(output_path+'/df_tem.csv')
    #caculate the total of all files and print save as a file.
    for i in range(len(total_df['file_total'])):
        total_df['file_total'][0]=total_df['file_total'][0]+total_df['file_total'][i]
    total_df.to_csv(output_dir+'/items_number.txt',index=0)
    
    
def classify_file(input_dir,output_dir,telemetry_status_path_name):
    """
    this code used to know which boat get the data
    and put the data file to the right folder
    notice:this code is suitable for matching data after 2000
    """
    if os.listdir(output_dir):
        print ('please input a empty directory!')
        sys.exit()
    #read the file of the telementry_status
    df=read_telemetrystatus(telemetry_status_path_name)

    #fix the format of time about logger_change
    for i in range(len(df)):
        if df['logger_change'].isnull()[i]:
            continue
        else:
            date_logger_change=df['logger_change'][i].split(',')   #get the time data of the logger_change
            for j in range(0,len(date_logger_change)):
                if len(date_logger_change[j])>4:     #keep the date have the month and year such as 1/17
                    date_logger_change[j]=zl.transform_date(date_logger_change[j]) #use the transform_date(date) to fix the date
            df['logger_change'][i]=date_logger_change
            
    #get the path and name of the file that need to match
    file_lists=glob.glob(os.path.join(input_dir,'*.csv'))
    #match the file        
    for file in file_lists:
        #time conversion, GMT time to local time
        time_str=file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[2]+' '+file.split('/')[len(file.split('/'))-1:][0].split('.')[0].split('_')[3]
        #GMT time to local time of file
        time_local=zl.gmt_to_eastern(time_str[0:4]+'-'+time_str[4:6]+'-'+time_str[6:8]+' '+time_str[9:11]+':'+time_str[11:13]+':'+time_str[13:15]).strftime("%Y%m%d")
        #math the SN and date
        for i in range(len(df['Lowell-SN'])):
            if df['Lowell-SN'].isnull()[i] or df['logger_change'].isnull()[i]:  #we will enter the next line if SN or date is not exist 
                continue
            else:
                for j in range(len(df['Lowell-SN'][i].split(','))):   
                    fname_len_SN=len(file.split('/')[len(file.split('/'))-1:][0].split('_')[1]) #the length of SN in the file name
                    len_SN=len(df['Lowell-SN'][i].split(',')[j]) #the length of SN in the culumn of the Lowell-SN inthe file of the telemetry_status.csv
                    if df['Lowell-SN'][i].split(',')[j][len_SN-fname_len_SN:]==file.split('/')[len(file.split('/'))-1:][0].split('_')[1]:
                        fpath,fname=os.path.split(file)    #seperate the path and name of the file
                        dstfile=(fpath).replace(input_dir,output_dir+'/'+df['Boat'][i]+'/'+fname) #produce the path+filename of the destination
                        dstfile=dstfile.replace('//','/')
                        #copy the file to the destination folder
                        if j<len(df['logger_change'][i])-1:
                            if df['logger_change'][i][j]<=time_local<=df['logger_change'][i][j+1]:
                                zl.copyfile(file,dstfile)  
                        else:
                            if df['logger_change'][i][j]<=time_local:
                                zl.copyfile(file,dstfile) 

def download(save_path,start_time=datetime.strptime('2000-1-1','%Y-%m-%d'),end_time=datetime.now()):
    ftp=ftplib.FTP('66.114.154.52','huanxin','123321')
    print ('Logging in.')
    ftp.cwd('/Matdata')
    print ('Accessing files')
    filenames = ftp.nlst() # get filenames within the directory OF REMOTE MACHINE
    start_time_gmt=zl.local2utc(start_time)  #time tranlate from local to UTC
    end_time_gmt=zl.local2utc(end_time)
    # MAKE THIS A LIST OF FILENAMES THAT WE NEED DOWNLOAD
    download_files=[]
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    for file in filenames:
        if len(file.split('_'))==4:
            if start_time_gmt<=datetime.strptime(file.split('_')[2]+file.split('_')[3].split('.')[0],'%Y%m%d%H%M%S')<end_time_gmt:
                download_files.append(file)
    for filename in download_files: # DOWNLOAD FILES   
        local_filename = os.path.join(save_path, filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)
        file.close()
    ftp.quit() # This is the “polite” way to close a connection
    print ('New files downloaded')
    
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
    
    
def format_lat_lon(data):
    if len(str(data).split('.')[0])>4 or 'A'<=str(data).split('.')[1][len(str(data).split('.')[1])-1:]<='Z':
        data=str(data).split('.')[0][len(str(data).split('.')[0])-4:]+'.'+str(data).split('.')[1][:4]
    return data
def match_tele_raw(input_dir,path_save,telemetry_status,start_time,time_interval,accept_minutes_diff,acceptable_distance_diff):
    """
    match the file and telementy.
    we can known how many file send to the satallite and output the figure
    """
    #read the file of the telementry_status
    telemetrystatus_df=read_telemetrystatus(telemetry_status)
    #st the record file use to write minmum maxmum and average of depth and temperature,the numbers of file, telemetry and successfully matched
    record_file_df=telemetrystatus_df.loc[:,['Boat','Vessel#']].reindex(columns=['Boat','Vessel#','matched_number','file_number','tele_num',\
                                              'average_diff_depth','average_diff_temp','max_diff_depth','min_diff_depth',\
                                              'max_diff_temp','min_diff_temp','sum_diff_depth','sum_diff_temp'],fill_value=None)
    #transfer the time format of string to datetime 
    start_time_local=datetime.strptime(start_time,'%Y-%m-%d')
    end_time_local=datetime.strptime(start_time,'%Y-%m-%d')+timedelta(days=time_interval)
    allfile_lists=zl.list_all_files(input_dir)
    ######################
    file_lists=[]
    for file in allfile_lists:
        if file[len(file)-4:]=='.csv':
            file_lists.append(file)
    #download the data of telementry
    tele_df=pd.read_csv('https://www.nefsc.noaa.gov/drifter/emolt.dat',sep='\s+',names=['vessel_n','esn','month','day','Hours','minates','fracyrday',\
                                          'lon','lat','dum1','dum2','depth','rangedepth','timerange','temp','stdtemp','year'])
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
        time_gmt=datetime.strptime(time_str,"%Y%m%d %H%M%S")
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
                if abs(valuable_tele_df['time'][i]-time_gmt)<=accept_minutes_diff:  #time match
                    if zl.dist(lat1=lat,lon1=lon,lat2=float(valuable_tele_df['lat'][i]),lon2=float(valuable_tele_df['lon'][i]))<=acceptable_distance_diff:  #distance match               
                        for j in range(len(record_file_df)):
                            if record_file_df['Vessel#'][j]==vessel_number:
                                diff_temp=abs(float(mean_temp)-float(valuable_tele_df['temp'][i]))
                                diff_depth=abs(float(mean_depth)-float(valuable_tele_df['depth'][i]))
                                if record_file_df['matched_number'].isnull()[j]:
                                    record_file_df['matched_number'][j]=1
                                    record_file_df['sum_diff_temp'][j]=diff_temp
                                    record_file_df['max_diff_temp'][j]=diff_temp
                                    record_file_df['min_diff_temp'][j]=diff_temp
                                    record_file_df['sum_diff_depth'][j]=diff_depth
                                    record_file_df['max_diff_depth'][j]=diff_depth
                                    record_file_df['min_diff_depth'][j]=diff_depth
                                    break
                                else:
                                    record_file_df['matched_number'][j]=record_file_df['matched_number'][j]+1
                                    record_file_df['sum_diff_temp'][j]=record_file_df['sum_diff_temp'][j]+diff_temp
                                    record_file_df['sum_diff_depth'][j]=record_file_df['sum_diff_depth'][j]+diff_depth
                                    if record_file_df['max_diff_temp'][j]<diff_temp:
                                        record_file_df['max_diff_temp'][j]=diff_temp
                                    if record_file_df['min_diff_temp'][j]>diff_temp:
                                        record_file_df['min_diff_temp'][j]=diff_temp
                                    if record_file_df['max_diff_depth'][j]<diff_depth:
                                        record_file_df['max_diff_depth'][j]=diff_depth
                                    if record_file_df['min_diff_depth'][j]>diff_depth:
                                        record_file_df['min_diff_depth'][j]=diff_depth
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

    
    
def read_telemetrystatus(path_name):
    data=pd.read_csv(path_name)
    #find the data lines number in the file('telemetry_status.csv')
    for i in range(len(data['Unnamed: 0'])):
        if data['Unnamed: 0'].isnull()[i]:
            data_line_number=i
            break
    #read the data about "telemetry_status.csv"
    telemetrystatus_df=pd.read_csv(path_name,nrows=data_line_number)
    telemetrystatus_df.columns=['Boat', 'Status (as 13 Nov 2018)','Vessel#', 'Funding', 'Program', 'Captain',
       'email address', 'phone', 'Port', 'Techs', 'Visit_Dates for telemetry',
       'Aquatec-SN', 'Lowell-SN', 'logger_change',
       'ESN','Other ESNs used', 'wifi?', 'wants weather?',
       'Notes (see individual tabs for historical notes)', 'add mail address!',
       'LI Firmware', 'image_file}', 'Fixed vs. Mobile', 'AP3 Batch',
       'weather_code']
    for i in range(len(telemetrystatus_df)):
        telemetrystatus_df['Boat'][i]=telemetrystatus_df['Boat'][i].replace("'","")
        if not telemetrystatus_df['Lowell-SN'].isnull()[i]:
            telemetrystatus_df['Lowell-SN'][i]=telemetrystatus_df['Lowell-SN'][i].replace('，',',')
        if not telemetrystatus_df['logger_change'].isnull()[i]:
            telemetrystatus_df['logger_change'][i]=telemetrystatus_df['logger_change'][i].replace('，',',')
    return telemetrystatus_df
