# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 15:08:40 2018

@author: leizhao

directory list in the end

"""
#from __future__ import unicode_literals
#import platform
#import warnings



import re
import pandas as pd
import pytz
import datetime
import os,shutil
import numpy as np
import math


#try:
#    from .StringMatcher import StringMatcher as SequenceMatcher
#except ImportError:
#    if platform.python_implementation() != "PyPy":
#        warnings.warn('Using slow pure-python SequenceMatcher. Install python-Levenshtein to remove this warning')
#    from difflib import SequenceMatcher
#
#from . import utils


    
def sd2uv(s,d):
    """transform the speed and direction data to the x,y components of the arrow vectors(u,v)""" 
    u_t=math.sin(math.radians(d))
    v_t=math.cos(math.radians(d))
    if abs(u_t)==1:
        v=0
        u=float(s)*u_t
    elif abs(v_t)==1:
        u=0
        v=float(s)*v_t
    else:
        u=float(s)*u_t
        v=float(s)*v_t
    return u,v

def uv2sd(u,v):
    """transform the x,y components of the arrow vectors(u,v) to the speed and direction data"""
#    s=math.sqrt(u**2+v**2)
    s=math.sqrt(np.square(u)+np.square(v))
    if s==0:
        d=0
    else:
        if abs(v/s)==1:
            d=180/np.pi*math.acos(float(v/s))
        elif abs(u/s)==1:
            d=180/np.pi*math.asin(float(u/s))
        else:
            dt=180/np.pi*math.atan(float(u/v))
            if u>0 and v>0:
                d=dt
            elif v<0:
                d=180+dt
            else:
                d=360+dt
    return s,d
    
    
def list_sd2uv(s,d):
    """aim at the list transform the speed and direction data to the x,y components of the arrow vectors(u,v)"""
    u,v=np.zeros(len(s)),np.zeros(len(s))
    for i in range(len(s)):
        u[i],v[i]=sd2uv(s[i],d[i])
    return u,v
        
    
    
def list_uv2sd(u,v):
    """aim at the list transform the x,y components of the arrow vectors(u,v) to the speed and direction data"""
    s,d=np.zeros(len(u)),np.zeros(len(u))
    for i in range(len(u)):
        s[i],d[i]=uv2sd(u[i],v[i])
    return s,d
    
    
def sd_list_mean(speeds,directions):
    """aim at the list about average of speed and direction"""
    u_total,v_total=0,0
    for a in range(len(speeds)):
        u,v=sd2uv(speeds[a],directions[a])
        u_total=u_total+u
        v_total=v_total+v
    u_mean=u_total/len(speeds)
    v_mean=v_total/len(speeds)
    WS,WD=uv2sd(u_mean,v_mean)
    return WS,WD
def transform_date(date):
    date=date.replace(' ','')
    if len(date.split('/'))!=3:
        date=date.split('/')[0]+'/'+'01'+'/'+date.split('/')[1]
    if len(date.split('/')[0])==1:
        date='0'+date.split('/')[0]+'/'+date.split('/')[1]+'/'+date.split('/')[2]
    if len(date.split('/')[1])==1:
        date=date.split('/')[0]+'/'+'0'+date.split('/')[1]+'/'+date.split('/')[2]
    if len(date.split('/')[2])==2:
        date=date.split('/')[0]+'/'+date.split('/')[1]+'/'+'20'+date.split('/')[2]
    date_data=date.split('/')[2]+date.split('/')[0]+date.split('/')[1]
    return date_data
def gmt_to_eastern(times_gmt):
    eastern = pytz.timezone('US/Eastern')
    gmt = pytz.timezone('Etc/GMT')
    date = datetime.datetime.strptime(str(times_gmt),'%Y-%m-%d %H:%M:%S')
    date_gmt=gmt.localize(date)
    easterndate=date_gmt.astimezone(eastern)
    return easterndate
    
def copyfile(srcfile,dstfile):
    if not os.path.isfile(srcfile):
        print "%s not exist!"%(srcfile)
    else:
        fpath,fname=os.path.split(dstfile) 
        if not os.path.exists(fpath):
            os.makedirs(fpath)
        shutil.copyfile(srcfile,dstfile)

#the number of heading.
def skip_to(fle, line,**kwargs):
    if os.stat(fle).st_size <= 5:
        raise ValueError("File is empty")
    with open(fle) as f:
        pos = 0
        cur_line = f.readline()
        while not cur_line.startswith(line):
            pos = f.tell()
            cur_line = f.readline()
        f.seek(pos)
        return pd.read_csv(f, **kwargs)
def keep_number(value,integer_num,decimal_digits):
    #ouput data type is str
    data=str(value)
    if len(data.split('.'))==2:
        integer=data.split('.')[0]
        decimal=data.split('.')[1]
    else:
        integer=data
        decimal=[]
    if integer_num==all:
        integer=integer
    elif len(integer)>integer_num:
        integer=integer[len(integer)-integer_num:]
    elif len(integer)<integer_num:
        for i in range(integer_num-len(integer)):
            integer='0'+integer[:]
    if decimal_digits==all:
        decimal=decimal
    elif len(decimal)>decimal_digits:
        decimal=decimal[:decimal_digits]
    elif len(decimal)<decimal_digits:
        if decimal==[]:
            decimal='0'
            for i in range(decimal_digits-len(decimal)):
                decimal=decimal[:]+'0'
        else:
            for i in range(decimal_digits-len(decimal)):
                decimal=decimal[:]+'0'
    return str(integer+'.'+decimal)
    

#df=skip_to(fn,'HEADING',sep=',',parse_dates={'datet':[1]},index_col='datet',date_parser=parse2)  
def fuzzyfinder(user_input, collection):
    suggestions = []
    pattern = '.*?'.join(user_input)    # Converts 'djm' to 'd.*?j.*?m'
    regex = re.compile(pattern)         # Compiles a regex.
    for item in collection:
        match = regex.search(item)      # Checks if the current item matches the regex.
        if match:
            suggestions.append((len(match.group()), match.start(), item))
    return [x for _, _, x in sorted(suggestions)] 




def list_all_files(rootdir):
    _files = []
    list = os.listdir(rootdir) #列出文件夹下所有的目录与文件
    for i in range(0,len(list)):
           path = os.path.join(rootdir,list[i])
           if os.path.isdir(path):
              _files.extend(list_all_files(path))
           if os.path.isfile(path):
              _files.append(path)
    return _files
    
"""
sd2uv:    21
uv2sd:    36
list_sd2uv(s,d):  58
list_uv2sd:   67
sd_list_mean:    75
"""
