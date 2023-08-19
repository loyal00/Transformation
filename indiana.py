
import os
from lib.utils import *
from lib.functions import *
import pandas as pd
import json
import re
import hashlib
import string
import time
from simple_chalk import chalk, green
import numpy as np
from collections import OrderedDict
import random
import glob
import urllib.request
import warnings
warnings.filterwarnings("ignore")
import sys
import io
import boto3
from botocore.exceptions import NoCredentialsError
import zipfile


import glob, os

df = pd.read_excel('docs/INDIANA.xlsx', header=0)
df["Medicare_Number"] = df["Medicare_Number"].astype(str).apply(lambda x: "0"+x if len(x)==5 else x)
medicare = df['Medicare_Number'].astype(str).str.strip()
#print(df)

objects = CMSValidations.objects(medicare_number__in = medicare)

list_hs_df = []

for ob in objects:
    master_id_json=ob.to_json()
    stud_obj = json.loads(master_id_json)
    drg_dict = dict( {
                        '_id' : stud_obj['provider']["$oid"] ,
                        'name' : ob['name'] ,
                        'city' : ob['city'] ,
                        'state' : ob['state'] ,
                        'medicare_number' : ob['medicare_number'],
                        'location1' : ob['location']['coordinates'][0],
                        'location2' : ob['location']['coordinates'][1],
                        'link' : ob['mr_mr_location']})
    list_hs_df.append(drg_dict) 
                                

HS_DF = pd.DataFrame.from_records(list_hs_df)
HS_DF.to_csv('input_detail/INDIANA_CM.csv', index=False)


# In[3]:


inputs=[]

df_ID = pd.read_csv( 'input_detail/INDIANA_CM.csv', header=0)
files =[]


    
for file in glob.glob("input/INDIANA/*.zip"):
    files.append(file.split("\\")[-1])
    
# for file in glob.glob("input/BAYLOR_SCOTT/*.json"):
#     files.append(file.split("\\")[-1])
    
df_ID['medicare_number'] = df_ID['medicare_number'].astype(str)
df_ID["medicare_number"] = df_ID["medicare_number"].apply(lambda x: "00"+x if len(x)==4 else x)
df_ID["medicare_number"] = df_ID["medicare_number"].apply(lambda x: "0"+x if len(x)==5 else x)


notfound = []

df_ID['filename'] = '0'
for i in range(len(df_ID)):
    flag = 0
    for j in range(len(files)):
        if df_ID.loc[i,'medicare_number'] in files[j]:
            df_ID.loc[i,'filename'] = files[j]
            flag = 1
    if flag == 0:
        notfound.append(df_ID.loc[i,'medicare_number'])
notfound = set(notfound)

for i in range(0,len(df_ID)):
    if df_ID.loc[i,'filename']!= '0':
        file_type = df_ID.loc[i,'filename'].split(".")[1].lower()
        if file_type=='zip':
            temp = {
            "File_Name": df_ID.loc[i,'filename'].replace('~$',''),
            "Provider_Id": df_ID.loc[i,'_id'],
            "lng": df_ID.loc[i,'location1'],
            "lat": df_ID.loc[i,'location2'],
            "State":df_ID.loc[i,'state'],
            "Provider_Name": df_ID.loc[i,'name'],
            "Location": df_ID.loc[i,'city'],
            "input_dir": 'input/INDIANA/',
            "medicare_number": df_ID.loc[i,'medicare_number']
          }
            inputs.append(temp)
print(notfound)




inputs = inputs[0:1]
len(inputs)




for file_inputs in inputs:
    
    start_time = time.time()
    providerId= file_inputs["Provider_Id"]#database mongob=db
    provider_name= file_inputs["Provider_Name"]
    State_Code = file_inputs["State"]
    lng = file_inputs["lng"]
    lat = file_inputs["lat"]
    input_dir = file_inputs["input_dir"]
    File_Name= file_inputs["File_Name"]
    has_chandra_approval= True
    location = file_inputs["Location"]
    medi_number =  file_inputs['medicare_number']


    file_type = File_Name.split(".")[1].lower()
    delimiter = "," if file_type == "txt" else ","
    
    #Load the MR File into Data Frame
    print("Opening File : {} , file Type : {} , delimiter : {}".format(File_Name,file_type,delimiter))

    chunk_size=200000
    if((file_type=="zip") ):
        chunks = pd.read_csv(os.path.join(File_Name),encoding = "ISO-8859-1",chunksize=chunk_size,compression='zip',header=0)#ISO-8859-1
    

    batch_no = 1
    for df in chunks:
        df.reset_index(drop=True, inplace=True)

        df = df.loc[:, ~df.columns.str.match("Unnamed")]

        needed_columns = df.columns.tolist()
        needed_columns = [str(x).strip().upper().replace("\n","_").replace(" ","_").strip().replace('_-_','-').replace('-ALL_PLANS','').replace('-OPTUM','_OPTUM') for x in needed_columns ]
        df.columns = needed_columns
        
        print(df.columns)
        
        
        df.rename(columns={"CPT/HCPCS/DRG":"cpt_code","DESCRIPTION":"test_name"}, inplace=True)
        
        df = df[df['test_name'].notna()]

        df['test_name'] = df['test_name'].astype(str)

        df['test_name'] = df['test_name'].str.upper().str.strip().astype(str)

        df = df[df['test_name'] != 'NONE']

        df = df[df['test_name'] != 'NAN']

        df = df[df['test_name'] != 'NA']

        df = df[df['test_name'] != 'N/A']
        df = df[df['test_name'] != 'N\A']
        
        df = df.replace('N/A',np.nan)
        
        df.reset_index(drop=True, inplace=True)
        
        df.loc[df['cpt_code'].astype(str).str.contains('-'),'cpt_code'] = 'nan'
        df.loc[df['NDC'].astype(str).str.contains('-'),'NDC'] = 'nan'
        df.loc[df['GROSS_CHARGE'].astype(str).str.contains('-'),'GROSS_CHARGE'] = '0'
        df.loc[df['DISCOUNTED_CASH_PRICE'].astype(str).str.contains('-'),'DISCOUNTED_CASH_PRICE'] = '0'
        df.loc[df['REVENUE_CODE'].astype(str).str.contains('-'),'REVENUE_CODE'] = 'nan'
        
        
        grouped = df.groupby(['cpt_code','test_name'], sort=False)

        count = 0
        df_new = []

        Insurance_cols = []

        for key, value in grouped:
            value.reset_index(drop=True, inplace=True)
            value['CONTRACT'] = value['CONTRACT'].str.upper()
            temp={}
            col_need = df.loc[:, ~df.columns.str.match("CONTRACT")].columns.to_list()
            for cols in col_need:
                temp[cols] = value.loc[0,cols]

            for i in range(len(value)):
                payer = str(value.loc[i,'CONTRACT'])

                temp[payer] = value.loc[i, 'PAYER-SPECIFIC_NEGOTIATED_RATE']


            df_new.append(temp)


        df = df_new.copy()
        del df_new; del grouped ;del temp  

        df = pd.DataFrame(df)
        print(df.columns)
        
        
        
        if 'NDC' in df.columns:
            df['NDC'] = df.loc[df['NDC'].notna(),'NDC'].astype(str).replace('N/A','').str.replace('-','')

            
        df['cpt_code'] = df['cpt_code'].astype(str)
        
        df['NDC'] = df['NDC'].astype(str)
        
        df['cpt_code'] = df.apply(lambda x: x['cpt_code'] + ',' + x['NDC'] if x['cpt_code'] != '' and x['cpt_code'] != 'nan' and x['NDC'] != 'nan' else x['cpt_code'], axis=1)
        
        df.loc[df["cpt_code"]== 'nan', "cpt_code"] = df['NDC']
        
        df.cpt_code = df.cpt_code.str.split(',')
        df = df.explode('cpt_code').reset_index(drop=True)

        df['cpt_code'] = df['cpt_code'].astype(str)
        
        df['cpt_code'] = df.apply(lambda x: 'nan'  if len(x['cpt_code']) == 14   else x['cpt_code'], axis=1)

        df['cpt_code'] = df['cpt_code'].replace('', 'nan', regex=True)

        df['cpt_code'] = df['cpt_code'].astype(str).str.strip().str.upper().str.replace(',','').str.replace("'",'')

        df["cpt_code"] = df["cpt_code"].apply(lambda x: 'nan' if x.isalpha() else x)

        df['cpt_code'] = df['cpt_code'].apply(lambda x: my_func(x) if x=='nan' or x=='NAN' else x)#custome code
        df['cpt_code'] = [x.replace('.0','') for x in df['cpt_code'].astype(str).tolist()]
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==2 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==4 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '00'+x if len(x)==1 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '00000'+x if len(x)==6 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '0000'+x if len(x)==7 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '000'+x if len(x)==8 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '00'+x if len(x)==9 else x)
        df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==10 and x.isnumeric()==True else x)
        
        
        Insurance_cols = df.columns.tolist()[14:]
        print(Insurance_cols)
        
        df['year_of_insertion'] = '2023'
        
        df['code_type'] = df['cpt_code'].apply(apply_code_type).apply(get_code_type).astype(str) 
        df['code_type'] = df.apply(lambda x: '5'  if len(x['cpt_code']) >= 12   else x['code_type'], axis=1)


        df["short_description"] = df["test_name"]

        df['REVENUE_CODE']= df['REVENUE_CODE'].fillna(df['cpt_code'])
        df.loc[df['REVENUE_CODE']=='nan','REVENUE_CODE'] = df['cpt_code']
        df["billingId"] = df["REVENUE_CODE"]
        df["quantity"]= 0 
        df['PATIENT_TYPE'] = df['PATIENT_TYPE'].str.replace('Inpatient','1').replace('Outpatient','0')
        df['inpatient'] = df['PATIENT_TYPE'].fillna('1')
    
        
        
        if 'GROSS_CHARGE' in df.columns:
            df["GROSS_CHARGE"] = df["GROSS_CHARGE"].replace(r"^\s*$", "0", regex=True)
            df["std_price_ip"]= df["GROSS_CHARGE"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().str.replace('115.00% of fee sched','0').replace('24.55% of Gross Charge','0').replace(np.nan,0).replace("nan",0).astype(float)
            df["std_price_op"]= df["GROSS_CHARGE"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().str.replace('115.00% of fee sched','0').replace('24.55% of Gross Charge','0').replace(np.nan,0).replace("nan",0).astype(float)

        if 'DISCOUNTED_CASH_PRICE' in df.columns:
            df["DISCOUNTED_CASH_PRICE"] = df["DISCOUNTED_CASH_PRICE"].replace('-','0').replace(r"^\s*$", "0", regex=True)
            df["cash_price_ip"] = df["DISCOUNTED_CASH_PRICE"].astype(str).str.replace(r'(^.*Gross.*$)', '0').str.replace("[\$,-]","", regex=True).str.strip().str.replace('115.00% of fee sched','0').replace('24.55% of Gross Charge','0').replace(np.nan,0).replace("nan",0).astype(float)
            df["cash_price_op"]  = df["DISCOUNTED_CASH_PRICE"].astype(str).str.replace(r'(^.*Gross.*$)', '0').str.replace("[\$,-]","", regex=True).str.strip().replace(np.nan,0).str.replace('115.00% of fee sched','0').replace('24.55% of Gross Charge','0').replace("nan",0).astype(float)
        else:
            df["cash_price_ip"] = 0.0
            df["cash_price_op"]  = 0.0

        df['discount'] = ((df['std_price_ip'] - df['cash_price_ip'])/df['std_price_ip'])*100
        df['discount'] = df['discount'].fillna(0)
        df.loc[df['cash_price_ip'] ==0, 'discount'] = 0
        df.loc[df['std_price_ip'] ==0, 'discount'] = 0.0
        df.loc[df['discount'] <0, 'discount'] = 0.0

        df.loc[df['code_type'].str.contains('4'), 'std_price_op'] = 0
        df.loc[df['code_type'].str.contains('4'), 'cash_price_op'] = 0
        df.loc[df['code_type'].str.contains('3'), 'std_price_op'] = 0
        df.loc[df['code_type'].str.contains('3'), 'cash_price_op'] = 0

        df.rename(columns={'DE-IDENTIFIED_MINIMUM_NEGOTIATED_CHARGE':'MIN','DE-IDENTIFIED_MAXIMUM_NEGOTIATED_CHARGE':'MAX'}, inplace=True)
        
        needed_columns = ["billingId",'year_of_insertion',"cpt_code","code_type","test_name","inpatient",
            "quantity","std_price_ip","std_price_op","cash_price_ip","cash_price_op",'discount',
                "short_description","MIN","MAX"] + Insurance_cols

        df = pd.DataFrame(df, columns = needed_columns)
        columns = df.columns
        
        unique_insurances  = extract_unique_cols(columns,from_col=15)
        print("unique insurance : ", len(unique_insurances))   
        print("unique insurance : ", unique_insurances)    

        columns_ = df.columns.tolist()
        a = [x.replace('_',' ').strip() for x in columns_[15:]]
        columns_ = columns_[:15]+a
        df.columns= columns_
        Insurance_cols = df.columns.tolist()[15:]  
        
        ip_list = []
        op_list = []

        for col in Insurance_cols:
            df[col] = df[col].replace(r"^\s*$", np.nan, regex=True).replace(' N/A ',np.nan)
            df[col] = df[col].fillna(0)
            df.loc[df[col].astype(str).str.contains('-'), col] = "0"
            

            
            ip = col.upper() + '-Ip'
            df[ip] = df[col].astype(str).str.strip().replace("[\$,-]","", regex=True).str.replace(r'(^.*Gross.*$)', '0').replace("NA",0).replace("N/A",0).replace(np.nan,0).replace("nan", "0").astype(float)
            ip_list.append(ip)

            op = col.upper() + '-Op'
            df[op] = df[col].astype(str).str.strip().replace("[\$,-]","", regex=True).str.replace(r'(^.*Gross.*$)', '0').replace("NA",0).replace("N/A",0).replace(np.nan,0).replace("nan", "0").astype(float)

            df.loc[df["code_type"].str.contains("4"), op] = 0
            df.loc[df["code_type"].str.contains("3"), op] = 0
            op_list.append(op)       


        #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   
        minmax_cols = ['MIN','MAX']
        for mm in minmax_cols:
            df[mm] = df[mm].astype(str)
            df.loc[df[mm] =='0.0', mm] = 'nan'
            df.loc[df[mm].astype(str).str.contains('-'), mm] = 'nan'
            
            df.loc[df[mm]==" $-   ", mm] = np.nan
            df[mm] = df[mm].replace(r"^\s*$", np.nan, regex=True).astype(str).str.replace("(","").str.replace(")","").replace('**',np.nan).str.replace(',','')

        

        df["MAX"] = df["MAX"].replace(r"^\s*$", np.nan, regex=True).astype(str).str.replace("(","").str.replace(")","")
        df["MIN"] = df["MIN"].replace(r"^\s*$", np.nan, regex=True).astype(str).str.replace("(","").str.replace(")","")

        df["max_price_ip1"]  = df[ip_list][~(df[ip_list]==0)].astype(float).max(axis=1,skipna = True)
        df["max_price_op1"] = df[op_list][~(df[op_list]==0)].astype(float).max(axis=1,skipna = True)
        df["min_price_ip1"] = df[ip_list][~(df[ip_list]==0)].astype(float).min(axis=1,skipna = True)
        df["min_price_op1"] = df[op_list][~(df[op_list]==0)].astype(float).min(axis=1,skipna = True)

        df["max_price_ip"] = df["MAX"].astype(str).str.replace("[\$,-]","", regex=True).replace("nan",np.nan).replace("NAN",np.nan)
        df["max_price_op"] = df["MAX"].astype(str).str.replace("[\$,-]","", regex=True).replace("nan",np.nan).replace("NAN",np.nan)
        df["min_price_ip"] = df["MIN"].astype(str).str.replace("[\$,-]","", regex=True).replace("nan",np.nan).replace("NAN",np.nan)
        df["min_price_op"] = df["MIN"].astype(str).str.replace("[\$,-]","", regex=True).replace("nan",np.nan).replace("NAN",np.nan)


        df.max_price_ip.fillna(df.max_price_ip1, inplace=True)
        df.max_price_op.fillna(df.max_price_op1, inplace=True)
        df.min_price_ip.fillna(df.min_price_ip1, inplace=True)
        df.min_price_op.fillna(df.min_price_op1, inplace=True)


        df["max_price_op"] = df["max_price_op"].fillna(0).astype(float)
        df["min_price_op"] = df["min_price_op"].fillna(0).astype(float)
        df["max_price_ip"] = df["max_price_ip"].fillna(0).astype(float)
        df["min_price_ip"] = df["min_price_ip"].fillna(0).astype(float)

        df.loc[df['code_type'].str.contains('4'), 'min_price_op'] = 0
        df.loc[df['code_type'].str.contains('4'), 'max_price_op'] = 0
        df.loc[df['code_type'].str.contains('3'), 'min_price_op'] = 0
        df.loc[df['code_type'].str.contains('3'), 'max_price_op'] = 0

        df.drop(Insurance_cols , axis="columns", inplace=True)

        df["location"] = location
        df["isServicable"] = True
        df['medicare_number'] = medi_number
        df['createdBy'] = 'SHUBHAM'

        df["isShoppable"] = True

        if provider_name:
            df["provider_name"] = provider_name

        if State_Code:
            df["state_code"] = State_Code

        df["provider"] = providerId
        df["is_active"] = True
        df['is_bundled'] = False

        df["master_key"] = df["cpt_code"].astype(str) + "-" + df["code_type"].astype(str)



        needed_columns =['billingId','year_of_insertion','provider','provider_name','medicare_number',
                        'code_type','cpt_code','test_name','inpatient','test_category','state_code','createdBy',
                        'location','isShoppable','isServicable','short_description',
                        'quantity','is_active','is_bundled',
                        'master_key','std_price_ip','std_price_op',
                        'cash_price_ip','cash_price_op','discount','max_price_ip',
                            'max_price_op','min_price_ip','min_price_op'] + ip_list + op_list 

        df = pd.DataFrame(df, columns = needed_columns)


        print(df.shape)


        df = df.loc[~((df['std_price_ip'] == 0) & (df['std_price_op'] == 0)&(df['cash_price_ip'] == 0) & (df['cash_price_op'] == 0)&(df['max_price_ip'] == 0) & (df['min_price_op'] == 0)&(df['min_price_ip'] == 0) & (df['max_price_op'] == 0))]
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True) 
        print("shape : ",df.shape)
        #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@


        group = 'INDIANA_UNIVERSITY_HEALTH'
        print("shape After Droppping duplicates & 0 prices : ",df.shape)
        df1  = df.copy()
        df.to_csv(providerId+ '_' + str(batch_no) + '.csv')

        ACCESS_KEY = ''
        SECRET_KEY = ''
        

        def upload_to_aws(local_file, bucket, s3_file):
            s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
            try:
                s3.upload_file(local_file, bucket, s3_file)
                print("Uploaded Successful")
                return True
            except FileNotFoundError:
                print("The file was not found")
                return False
            except NoCredentialsError:
                print("Credentials not available")
                return False


        uploaded = upload_to_aws(providerId + '_' + str(batch_no) + '.csv', 'data-stage-1', 'Insertion_2023/'+ group + '/'+providerId + '_' + str(batch_no) + '.csv')
        print('--- Took %s seconds ---' % (time.time() - start_time))

        #     ##@@@@@@@@@@@@------------------Uploading Files------@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

        ACCESS_KEY = ''
        SECRET_KEY = ''


        def upload_to_aws(local_file, bucket, s3_file):
            s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
            try:
                s3.upload_file(local_file, bucket, s3_file)
                print("Uploaded Successful")
                return True
            except FileNotFoundError:
                print("The file was not found")
                return False
            except NoCredentialsError:
                print("Credentials not available")
                return False


        uploaded = upload_to_aws(providerId + '_' + str(batch_no)  +'.csv', 'data-stage-2', 'Final_Data_2023/'+ group + '/'+providerId + '_' + str(batch_no)  + '.csv')
        print('--- Took %s seconds ---' % (time.time() - start_time))
        
        

        
        batch_no+=1                





    

        
        






