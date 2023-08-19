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

#####################################################

import glob, os

df = pd.read_excel('docs/PROVIDENCE.xlsx', header=0)
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
HS_DF.to_csv('input_detail/PROVIDENCE_CM.csv', index=False)

#######################################################################

inputs=[]

df_ID = pd.read_csv( 'input_detail/PROVIDENCE_CM.csv', header=0)
files =[]

for file in glob.glob("input/PROVIDENCE/*.xlsx"):
    files.append(file.split("\\")[-1])

for file in glob.glob("input/PROVIDENCE/*.json"):
    files.append(file.split("\\")[-1])
    
for file in glob.glob("input/PROVIDENCE/*.csv"):
    files.append(file.split("\\")[-1])

    
df_ID['medicare_number'] = df_ID['medicare_number'].astype(str)
df_ID["medicare_number"] = df_ID["medicare_number"].apply(lambda x: "00"+x if len(x)==4 else x)
df_ID["medicare_number"] = df_ID["medicare_number"].apply(lambda x: "0"+x if len(x)==5 else x)

# df_ID = df_ID[df_ID['in']!='Yes']
# df_ID.reset_index(drop=True, inplace=True)
# print(df_ID.shape)

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
        if file_type=='json':
            temp = {
            "File_Name": df_ID.loc[i,'filename'].replace('~$',''),
            "Provider_Id": df_ID.loc[i,'_id'],
            "lng": df_ID.loc[i,'location1'],
            "lat": df_ID.loc[i,'location2'],
            "State":df_ID.loc[i,'state'],
            "Provider_Name": df_ID.loc[i,'name'],
            "Location": df_ID.loc[i,'city'],
            "input_dir": 'input/PROVIDENCE/',
            "medicare_number": df_ID.loc[i,'medicare_number']
          }
            inputs.append(temp)
print(notfound)

#########################################################################

inputs = inputs[41:42]
len(inputs)
########################################################################

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
    medicare = file_inputs["medicare_number"]
    

    file_type = File_Name.split(".")[1].lower()
    delimiter = "," if file_type == "txt" else ","
    
    #Load the MR File into Data Frame
    print("Opening File : {} , file Type : {} , delimiter : {}".format(File_Name,file_type,delimiter))

    dfs = []
    flag=0
    with open(os.path.join(File_Name), encoding='utf-8', errors='ignore') as json_data:
            data = json.load(json_data, strict=False)
            dfs = []
            for key,value in data.items():
                print(key)
                if 'Gross Charges' in key or  'Discount Cash Price - Gross' in key:
                    if 'Gross Charges' in key:
                        df = pd.json_normalize(data,key)
                        df.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                        drop_cols = ['Note','HOSPITAL SYSTEM CHARGE CODE']
                        for dc in drop_cols:
                            if dc in df.columns:
                                df.drop(dc , axis='columns', inplace=True)
                    else:              
                        if 'Discount Cash Price - Gross' in key :
                            df_dis1 = pd.json_normalize(data,key)
                            print(df_dis1.columns)
                            needed_columns = df_dis1.columns.tolist()
                            needed_columns = [str(x).strip().replace('[','').replace(']','').replace('(','').replace(')','')for x in needed_columns]
                            df_dis1.columns = needed_columns
                            print(df_dis1.columns)
                            ccv = df_dis1.loc[:, df_dis1.columns.str.contains("CSJ LOCATION Unit Price IP/OP")]
                            dd = ccv.columns.tolist()
                            
                            print(dd)
                            df_dis1.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)

                            for i in range(len(df)):
                                df.loc[df['test_name']==df_dis1.loc[i,'test_name'], 'CASH'] = df_dis1.loc[i,'SHS LOCATION Unit Price IP/OP Discount Cash Price'] #Discount Cash Price
                                
                        
                            print(df.columns)
                            df.to_csv('cc.csv')
                
                if 'Pharmacy Charges' in key or 'Discount Cash Price - Pharmacy' in key:
                    if 'Pharmacy Charges' in key:
                        df_p = pd.json_normalize(data,key)
                        df_p.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)

                    else:
                        if 'Discount Cash Price - Pharmacy' in key:
                            df_dis2 = pd.json_normalize(data,key)
                            df_dis2.reset_index(drop=True, inplace=True)
                            df_dis2.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            for i in range(len(df_p)):
                                df_p.loc[df_p['test_name']==df_dis2.loc[i,'test_name'], 'UNIT_CASH'] = df_dis2.loc[i,'UNIT PRICE Discount Cash Price']
                        
                           
                if 'Supply Charges' in key or 'Discount Cash Price - Supply' in key:
                    if 'Supply Charges' in key:
                        df_s = pd.json_normalize(data,key)
                        df_s.to_csv('cv.csv')

                        df_s.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)

                    else:
                        if 'Discount Cash Price - Supply' in key:
                            df_dis3 = pd.json_normalize(data,key)
                            df_dis3.reset_index(drop=True, inplace=True)
                            df_dis3.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            for i in range(len(df_s)):
                                    df_s.loc[df_s['test_name']==df_dis3.loc[i,'test_name'], 'UNIT_CASH'] = df_dis3.loc[i,'UNIT PRICE Discount Cash Price']
                
                
                if 'Outpatient' in key or 'OP Payer'in key:
                    flag_op = 0
                    if 'Payer' in key:
                        df_out_temp = pd.json_normalize(data,key)
                        df_out_temp.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)

                        temp_payer = df_out_temp['Payer'].unique().tolist()
                        print(temp_payer)
                        df_out_temp.reset_index(drop=True, inplace=True)
                        df_out_temp["APC"] = [x.replace(".0","") for x in df_out_temp["APC"].astype(str).tolist()]
                        df_out_temp["APC"] = df_out_temp["APC"].apply(lambda x: "0"+x if len(x)==3 else x)
                        df_out_temp["APC"] = df_out_temp["APC"].apply(lambda x: "00"+x if len(x)==2 else x)
                        df_out_temp["APC"] = df_out_temp["APC"].apply(lambda x: "000"+x if len(x)==1 else x)
                        df_out[temp_payer] = 0
                        for i in range(len(df_out_temp)):
                            df_out.loc[df_out['APC']==df_out_temp.loc[i,'APC'], temp_payer] = df_out_temp.loc[i,'Payer Specific Negotiated Charge']
                    else:
                        if 'Minimum' in key:
                            df_out = pd.json_normalize(data,key)
                            df_out.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            df_out["APC"] = [x.replace(".0","") for x in df_out["APC"].astype(str).tolist()]
                            df_out["APC"] = df_out["APC"].apply(lambda x: "0"+x if len(x)==3 else x)
                            df_out["APC"] = df_out["APC"].apply(lambda x: "00"+x if len(x)==2 else x)
                            df_out["APC"] = df_out["APC"].apply(lambda x: "000"+x if len(x)==1 else x)
                        else:
                            df_out_temp = pd.json_normalize(data,key)
                            df_out_temp.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            temp_payer = 'De-Identified Maximum Negotiated Charge'
                            df_out[temp_payer] = 0
                            for i in range(len(df_out_temp)):
                                df_out.loc[df_out['APC']==df_out_temp.loc[i,'APC'], temp_payer] = df_out_temp.loc[i,'De-Identified Maximum Negotiated Charge']
                            df_out["APC"] = [x.replace(".0","") for x in df_out["APC"].astype(str).tolist()]
                            df_out["APC"] = df_out["APC"].apply(lambda x: "0"+x if len(x)==3 else x)
                            df_out["APC"] = df_out["APC"].apply(lambda x: "00"+x if len(x)==2 else x)
                            df_out["APC"] = df_out["APC"].apply(lambda x: "000"+x if len(x)==1 else x)

                if 'Inpatient' in key or 'IP Payer'in key:
                    if 'Payer' in key:
                        df_in_temp = pd.json_normalize(data,key)
                        df_in_temp.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                        temp_payer = df_in_temp['Payer'].unique().tolist()
                        df_in_temp.reset_index(drop=True, inplace=True)
                        df_in_temp["MS-DRG"] = [x.replace(".0","") for x in df_in_temp["MS-DRG"].astype(str).tolist()]
                        df_in_temp["MS-DRG"] = df_in_temp["MS-DRG"].apply(lambda x: "0"+x if len(x)==2 else x)
                        df_in_temp["MS-DRG"] = df_in_temp["MS-DRG"].apply(lambda x: "00"+x if len(x)==1 else x)
                        df_in[temp_payer] = 0
                        for i in range(len(df_in_temp)):
                            df_in.loc[df_in['MS-DRG']==df_in_temp.loc[i,'MS-DRG'], temp_payer] = df_in_temp.loc[i,'Payer Specific Negotiated Charge']
                    else:
                        if 'Minimum' in key:
                            df_in = pd.json_normalize(data,key)
                            df_in.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            df_in["MS-DRG"] = [x.replace(".0","") for x in df_in["MS-DRG"].astype(str).tolist()]
                            df_in["MS-DRG"] = df_in["MS-DRG"].apply(lambda x: "0"+x if len(x)==2 else x)
                            df_in["MS-DRG"] = df_in["MS-DRG"].apply(lambda x: "00"+x if len(x)==1 else x)
                        else:
                            df_in_temp = pd.json_normalize(data,key)
                            temp_payer = 'De-Identified Maximum Negotiated Charge'
                            df_in_temp.rename(columns={"CHARGE DESCRIPTION":"test_name","Description":"test_name","DRUG GENERIC NAME":"test_name","SUPPLY ITEM DESCRIPTION":'test_name'}, inplace=True)
                            df_in[temp_payer] = 0
                            for i in range(len(df_in_temp)):
                                df_in.loc[df_in['MS-DRG']==df_in_temp.loc[i,'MS-DRG'], temp_payer] = df_in_temp.loc[i,'De-Identified Maximum Negotiated Charge']
                            df_in["MS-DRG"] = [x.replace(".0","") for x in df_in["MS-DRG"].astype(str).tolist()]
                            df_in["MS-DRG"] = df_in["MS-DRG"].apply(lambda x: "0"+x if len(x)==2 else x)
                            df_in["MS-DRG"] = df_in["MS-DRG"].apply(lambda x: "00"+x if len(x)==1 else x)

    df  = pd.concat([df,df_p,df_s],axis= 0)#,df_s
    if flag_op ==0:
        df_out['code_type_'] = 'APC'
        df_out['inpatient'] = 0
        needed_columns = df_out.columns.tolist()
        needed_columns = [str(x).strip().replace('\n','_').replace('-','_').replace(' ','_').replace('_-_','_').replace('__','_').replace('__','_').upper().strip().replace('APC','cpt_code') for x in needed_columns]
        df_out.columns = needed_columns

    df_in['code_type_'] = 'DRG'
    df_in['inpatient'] = 1

    df['inpatient'] = 1
    df['code_type_'] = '0'


    needed_columns = df.columns.tolist()
    needed_columns = [str(x).strip().replace('\n','_').replace('-','_').replace(' ','_').replace('_-_','_').replace('__','_').replace('__','_').upper().strip().replace('FSC_103','GROSS_PRICE').replace('CDM_HCPCS','cpt_code').replace('FSC 103','GROSS_PRICE').replace('DISCOUNT_PRICING','discount') for x in needed_columns]
    df.columns = needed_columns

    needed_columns = df_in.columns.tolist()
    needed_columns = [str(x).strip().replace('\n','_').replace('-','_').replace(' ','_').replace('_-_','_').replace('__','_').replace('__','_').upper().strip().replace('MS_DRG','cpt_code') for x in needed_columns]
    df_in.columns = needed_columns

    dcols = ['HOSPITAL_SYSTEM_SUPPLY_CODE','HOSPITAL_SYSTEM_DRUG_CODE','CHARGE_CODE_NAME','HOSPITAL_SYSTEM_SUPPLY_IDENTIFIER','LAWSON_ID']
    for cols_d in dcols:
        if cols_d in df.columns:
            df.drop(cols_d , axis='columns', inplace=True)

    if flag_op==0:
        df = pd.concat([df, df_out, df_in], axis=0)
        del df_out
    else:
        df = pd.concat([df, df_in], axis=0)

    del df_in   

    df.reset_index(drop=True, inplace=True)
    df.to_csv(medicare+'_source.csv')
    
    
    Insurance_cols = df.columns.tolist()[15:]
    print(df.columns)

    
    df['year_of_insertion'] = '2023'
    
    df.rename(columns={"TEST_NAME":"test_name","NATIONAL_DRUG_CODE_(NDC)":"NDC"}, inplace=True)
    
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
    df['cpt_code'] = df['cpt_code'].fillna(df['CPT/HCPCS_CODE'])
    
    if 'NDC' in df.columns:
        df['NDC'] = df.loc[df['NDC'].notna(),'NDC'].astype(str).replace('N/A','').str.replace('-','').str.zfill(11)
#         df.loc[df['NDC'].notna(),'cpt_code'] = df['NDC']
        
   
        
    df['cpt_code'] = df['cpt_code'].astype(str)
    
    df['NDC'] = df['NDC'].astype(str)
    
    df['cpt_code'] = df.apply(lambda x: x['cpt_code'] + ',' + x['NDC'] if x['cpt_code'] != '' and x['cpt_code'] != 'nan' and x['NDC'] != 'nan' else x['cpt_code'], axis=1)
    
    df.loc[df["cpt_code"]== 'nan', "cpt_code"] = df['NDC']
    
    df.cpt_code = df.cpt_code.str.split(',')
    df = df.explode('cpt_code').reset_index(drop=True)
    
    
    
    df['cpt_code'] = df['cpt_code'].replace('', 'nan', regex=True)
    
    df['cpt_code'] = df['cpt_code'].astype(str).str.strip().str.upper().str.replace('-','').str.replace(',','')
    
    df["cpt_code"] = df["cpt_code"].apply(lambda x: 'nan' if x.isalpha() else x)
    
    df['cpt_code'] = df['cpt_code'].apply(lambda x: my_func(x) if x=='nan' or x=='NAN' else x)#custome code
    df['cpt_code'] = [x.replace('.0','') for x in df['cpt_code'].astype(str).tolist()]
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==2 else x)
#     df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==4 else x)
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '00'+x if len(x)==1 else x)
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '0000'+x if len(x)==7 else x)
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '000'+x if len(x)==8 else x)
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '00'+x if len(x)==9 else x)
    df['cpt_code'] = df['cpt_code'].apply(lambda x: '0'+x if len(x)==10 and x.isnumeric()==True else x)
    
    df['code_type'] = df['cpt_code'].apply(apply_code_type).apply(get_code_type).astype(str) 
    
    df["short_description"] = df["test_name"]
        
    df["billingId"] = df['cpt_code']
    
    df['billingId'] = [x.replace('.0','') for x in df['billingId'].astype(str).tolist()]
    
    df["quantity"]=0
    
    df['inpatient'] = df['INPATIENT']
    
    df['SHS_LOCATION_(UNIT_PRICE)_[IP/OP]'] = df['SHS_LOCATION_(UNIT_PRICE)_[IP/OP]'].fillna(df['UNIT_PRICE'])
    
    if 'SHS_LOCATION_(UNIT_PRICE)_[IP/OP]' in df.columns:
        df["SHS_LOCATION_(UNIT_PRICE)_[IP/OP]"] = df["SHS_LOCATION_(UNIT_PRICE)_[IP/OP]"].replace(r"^\s*$", "0", regex=True)
        df["std_price_ip"]= df["SHS_LOCATION_(UNIT_PRICE)_[IP/OP]"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().replace(np.nan,0).replace("nan",0).astype(float)
        df["std_price_op"]= df["SHS_LOCATION_(UNIT_PRICE)_[IP/OP]"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().replace(np.nan,0).replace("nan",0).astype(float)
   
    df['CASH']= df['CASH'].fillna(df['UNIT_CASH'])

    if 'CASH' in df.columns:
        df["CASH"] = df["CASH"].replace(r"^\s*$", "0", regex=True)
        df["cash_price_ip"]= df["CASH"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().str.replace('#VALUE!','0').replace(np.nan,0).replace("nan",0).astype(float)
        df["cash_price_op"]= df["CASH"].astype(str).str.replace("[\$,-]","", regex=True).str.strip().str.replace('#VALUE!','0').replace(np.nan,0).replace("nan",0).astype(float)
   
    else:
        df["cash_price_ip"] = 0.0
        df["cash_price_op"] = 0.0
        

    df['discount'] = ((df['std_price_ip'] - df['cash_price_ip'])/df['std_price_ip'])*100
    df['discount'] = df['discount'].fillna(0)
    df.loc[df['cash_price_ip'] ==0, 'discount'] = 0.0
    df.loc[df['std_price_ip'] ==0, 'discount'] = 0.0
    df.loc[df['discount'] <0, 'discount'] = 0.0
    
    df.loc[df['code_type'].str.contains('4'), 'std_price_op'] = 0
    df.loc[df['code_type'].str.contains('4'), 'cash_price_op'] = 0
    df.loc[df['code_type'].str.contains('3'), 'std_price_op'] = 0
    df.loc[df['code_type'].str.contains('3'), 'cash_price_op'] = 0
    
    df.rename(columns={'DE_IDENTIFIED_MINIMUM_NEGOTIATED_CHARGE':'MIN','DE_IDENTIFIED_MAXIMUM_NEGOTIATED_CHARGE':'MAX'}, inplace=True)

    needed_columns = ["billingId",'year_of_insertion',"cpt_code","code_type","inpatient","test_name",
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
        df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)
        df[col] = df[col].fillna(0)
       
        ip = col.upper() + "-Ip"
        df[ip] = df[col].astype(str).str.strip().replace("[\$,-]","", regex=True).replace("NA",0).replace("N/A",0).replace(np.nan,0).replace("nan", "0").astype(float)
        ip_list.append(ip)

        op = col.upper() + "-Op"
        df[op] = df[col].astype(str).str.strip().replace("[\$,-]","", regex=True).replace("NA",0).replace("N/A",0).replace(np.nan,0).replace("nan", "0").astype(float)

        df.loc[df["code_type"].str.contains("4"), op] = 0
        df.loc[df["code_type"].str.contains("3"), op] = 0
        op_list.append(op) 
        
    
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
    df['createdBy'] = 'SHUBHAM'
        
    df["isShoppable"] = True
    df['medicare_number'] = medicare
    
    if provider_name:
        df["provider_name"] = provider_name

    if State_Code:
        df["state_code"] = State_Code
        
    df["provider"] = providerId
    df["is_active"] = True
    df['is_bundled'] = False
    
    df["master_key"] = df["cpt_code"].astype(str) + "-" + df["code_type"].astype(str)
    
    
    needed_columns =['billingId','year_of_insertion','provider','provider_name','medicare_number',
                      'code_type','cpt_code','test_name',"inpatient",'test_category','state_code','createdBy',
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
    ###########################################################

    drg_dict = insert_drglook(df)


    carrier_dict = insert_insurance_carriers(unique_insurances)

    map_insert_provider(carrier_dict,providerId,State_Code)

    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    df = opt_master_key(df)    
    #     df['master_key'] = df['master_key'].str.replace('-0.0','-0')
    print('--- Took %s seconds ---' % (time.time() - start_time))
    #     print(done)
    #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    needed_columns = ['id','billingId','year_of_insertion','provider','provider_name','medicare_number',
                      'code_type','cpt_code','test_name','state_code','createdBy',
                      'location','isShoppable','isServicable','is_bundled',
                      'short_description','quantity','is_active',
                      'master_key','master_id','cptcode_type',
                      'isMandatory','discount','std_price_ip','std_price_op',
                      'cash_price_ip','cash_price_op','max_price_ip',
                       'max_price_op','min_price_ip','min_price_op'] + ip_list + op_list

    df = pd.DataFrame(df, columns = needed_columns)
    #df = df.drop_duplicates(subset='master_key', keep='first')


    needed_columns_noINS = ['id','billingId','year_of_insertion','provider','provider_name','medicare_number',
                      'code_type','cpt_code','test_name','state_code','createdBy',
                      'location','isShoppable','isServicable','is_bundled',
                      'short_description','quantity','is_active',
                      'master_key','master_id','cptcode_type',
                      'isMandatory','discount','std_price_ip','std_price_op',
                      'cash_price_ip','cash_price_op','max_price_ip',
                       'max_price_op','min_price_ip','min_price_op']


    CM_df =  pd.DataFrame(df, columns = needed_columns_noINS)

    cmins_df = batch_pre_process_cm_ins(df,carrier_dict,unique_insurances)
    #     cmins_df= pd.concat(cmins_df)

    insert_cm(CM_df,[lng,lat])
    #     print(done)
    con_insert_cmins(cmins_df,[lng,lat])

    print('--- Took %s seconds ---' % (time.time() - start_time))


    print('\n\n')
    
