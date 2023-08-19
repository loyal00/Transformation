import os
import pandas as pd
import json
import re
import hashlib
import string
from bson.objectid import ObjectId
from model.schema import *
import time
from simple_chalk import chalk, green
import numpy as np
from collections import OrderedDict
import random
from lib.utils import *
import concurrent.futures


def Update_isService(providerID,collection='charge'):
    
    if collection =='provider':
        print("Provider  :  ",providerID)
        TestSchema.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
        Provider_Insurance.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
    elif collection=='charge':
        print("Charge  :  ",providerID)
        CM_Insurances.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
        Charge_Master.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
    else:
        print("Both  :  ",providerID)
        TestSchema.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
        Provider_Insurance.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
        CM_Insurances.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)
        Charge_Master.objects(provider__in = [providerID], year_of_insertion = '').update(set__isServicable = False)


def opt_master_key(main_df):
    main_df['cpt_code_type'] = main_df['cpt_code'].astype(str) + '-' + main_df['code_type'].astype(str) 

    main_df = main_df.drop_duplicates(subset='cpt_code_type', keep='first')

    master_keys = main_df[main_df.code_type != '6']['master_key'].unique()
    
    objects = DRG_lookup_new.objects(master_key__in = main_df['master_key'])
    
    drg_dict = []
    drg_dict_man = []
    drg_dict_test_name = []
    

    for ob in objects:
        drg_dict.append({ob['master_key'] : str(ob['id'])}) 
        drg_dict_man.append({ob['master_key'] : ob['isMandatory']}) 
        drg_dict_test_name.append({ob['master_key'] : ob['test_name']}) 

#     drg_dict_ = defaultdict(list)
#     drg_dict_man_ = defaultdict(list)
#     drg_dict_test_name_ = defaultdict(list)
    
#     {drg_dict_[key].append(sub[key]) for sub in drg_dict for key in sub} 
#     {drg_dict_man_[key].append(sub[key]) for sub in drg_dict_man for key in sub} 
#     {drg_dict_test_name_[key].append(sub[key]) for sub in drg_dict_test_name for key in sub} 
#     del drg_dict,drg_dict_man,drg_dict_test_name


    drg_dict_ = {}
    drg_dict_man_ = {}
    drg_dict_test_name_ = {}
    
    for d in drg_dict:
        drg_dict_.update(d)
    for d in drg_dict_man:
        drg_dict_man_.update(d)
    for d in drg_dict_test_name:
        drg_dict_test_name_.update(d)
    
    del drg_dict,drg_dict_man,drg_dict_test_name
    
    main_df['master_id'] = main_df['cpt_code_type']
    main_df['isMandatory'] = main_df['cpt_code_type']
    main_df['test_name_main'] = main_df['cpt_code_type']
                                     
    print(main_df.shape)
                                     
    main_df['master_id'] = main_df['master_id'].map(drg_dict_)
    main_df['isMandatory'] = main_df['isMandatory'].map(drg_dict_man_)
    main_df['test_name_main'] = main_df['test_name_main'].map(drg_dict_test_name_)
                                     
    del drg_dict_,drg_dict_man_,drg_dict_test_name_
    
    
    main_df.loc[main_df['isMandatory']==True, 'test_name'] = main_df['test_name_main']
    
    main_df["encoded_message"] = main_df['test_name'].apply(hash_md5).astype(str) 
    main_df["id"] = main_df['test_name'].apply(objectID_assign)
    main_df['cptcode_type'] = main_df['provider'].astype(str) + "-" + main_df['cpt_code'].astype(str) + "-" + main_df['code_type'].astype(str)
    
    main_df['master_id2'] = main_df['master_id'] 
    main_df.loc[~(main_df['master_id'].notna()), 'master_id2'] = main_df['id']
    
    main_df['master_key'] = main_df['provider'].astype(str) + "-"+main_df['cpt_code'].astype(str) + "-"+main_df['code_type'].astype(str) + "-"+main_df['encoded_message'].astype(str) + "-"+main_df['master_id2'].astype(str) + "-"+main_df['min_price_op'].astype(str) + "-"+main_df['min_price_ip'].astype(str) + "-"+main_df['max_price_op'].astype(str) + "-"+main_df['max_price_ip'].astype(str) + "-"+'0' + "-"+'0' + "-"+main_df['cash_price_op'].astype(str) + "-"+main_df['cash_price_ip'].astype(str) + "-"+main_df['std_price_op'].astype(str) + "-"+main_df['std_price_ip'].astype(str) + "-"+ main_df['year_of_insertion'].astype(str)
        
    main_df.drop_duplicates(subset='master_key', keep="first", inplace=True)
    main_df.reset_index(drop=True, inplace=True)
    main_df['master_id'] = main_df['master_id'].fillna('')
    print(main_df.shape)
    
    return main_df


def set_master_key(main_df,providerId):
    
    main_df['cpt_code_type'] = main_df['cpt_code'].astype(str) + '-' + main_df['code_type'].astype(str)

    main_df['id'] = 0 ; main_df['master_id'] = 0

    main_df['isMandatory'] = False ; master_key_set = set()    

    main_df['cptcode_type'] = 0 ; main_df['encoded_message']=0

    drg_dict = DRG_lookup_new.objects(master_key__in = main_df['master_key'])

    print(main_df.shape)

    for i, row in main_df.iterrows():

        temp_ID = ObjectId() 

        row['id'] = temp_ID

        drg_list = list( filter( lambda x : x['master_key'] == row['cpt_code_type'] , drg_dict ) )

        if len(drg_list) > 0:
            row['master_id'] = drg_list[0]['id']
            row['isMandatory'] = drg_list[0]['isMandatory']
            if row['isMandatory'] == True:
                row['test_name'] = drg_list[0]['test_name']

        row['cptcode_type'] = providerId + "-" + row['cpt_code'] + "-" + str(row['code_type'])

        if 'test_name' in row:
            row['encoded_message'] = hash_md5( row['test_name'] )

            concat_list = [

                providerId ,
                row['cpt_code'] ,
                str(row['code_type'] ),
                row['encoded_message'] ,
                str( row['master_id'] ) if 'master_id' in row else temp_ID ,#code type 6 - _tempId (internal code)#cptnumber smallletter
                str( row['min_price_op'] ) if 'min_price_op' in row else '0' ,
                str( row['min_price_ip'] ) if 'min_price_ip' in row else '0' ,
                str( row['max_price_op'] ) if 'max_price_op' in row else '0' ,
                str( row['max_price_ip'] ) if 'max_price_ip' in row else '0' ,
                '0',
                '0',
                str( row['cash_price_op'] ) if 'cash_price_op' in row else '0' ,
                str( row['cash_price_ip'] ) if 'cash_price_ip' in row else '0' ,
                str( row['std_price_op'] ) if 'std_price_op' in row else '0' ,
                str( row['std_price_ip'] ) if 'std_price_ip' in row else '0' ,

                ]

            row['master_key'] = master_key = "-".join(concat_list)

            main_df.loc[i] = row

            if master_key in master_key_set:
                #print("not inserted ",master_key)
                main_df.drop(i, inplace=True)
                continue
            else:
                master_key_set.add(master_key)

    print(main_df.shape)
    return main_df

def extract_unique_cols(columns,from_col=14):
    col_list = columns[from_col:].to_list()
    col_list = [ x.replace('_',' ').strip().replace('-INPATIENT','').replace('-OUTPATIENT','').replace('-Ip','').replace('-Op','') for x in col_list]
    unique_set = set( string.upper() for string in col_list )

    return unique_set
    
def my_func(x):
    rand_cpt =  '{:010}'.format( random.randrange( 3**3 , 10**3 ) )
    rand_cpt =  ''.join( random.choices( string.ascii_uppercase + string.digits, k = 10 ) )
    return rand_cpt

def batch_pre_process_cm_ins(df,carrier_list,unique_insurances):#column wise INS to row-wise INS traspose

    if 'inpatient' in df.columns:
        needed_columns_noINS = ['billingId','inpatient','year_of_insertion','provider','provider_name','medicare_number',
                                  'code_type','cpt_code','test_name','state_code','discount',
                                  'location','isShoppable','isServicable','is_bundled',
                                  'short_description','quantity','is_active',
                                  'master_key','master_id','cptcode_type',
                                  'isMandatory','std_price_ip','std_price_op',
                                  'cash_price_ip','cash_price_op','max_price_ip',
                                   'max_price_op','min_price_ip','min_price_op']
    else:
        needed_columns_noINS = ['billingId','year_of_insertion','provider','provider_name','medicare_number',
                                  'code_type','cpt_code','test_name','state_code','discount',
                                  'location','isShoppable','isServicable','is_bundled',
                                  'short_description','quantity','is_active',
                                  'master_key','master_id','cptcode_type',
                                  'isMandatory','std_price_ip','std_price_op',
                                  'cash_price_ip','cash_price_op','max_price_ip',
                                   'max_price_op','min_price_ip','min_price_op']

    df_cm = df[needed_columns_noINS]
    
    df_ins_final = []
        
    for i,carrier in enumerate(unique_insurances):

        df_ins = df_cm[needed_columns_noINS].copy()

        carrier_object = list(filter( lambda x : x['carrier_name'] == carrier ,carrier_list) )

        # print('carrier_object',carrier_object)

        if len(carrier_object) > 0 :
            print('found Carrier => ',carrier_object[0])
            df_ins['carrier_id'] = carrier_object[0]['carrier_id']

        df_ins['carrier_name'] = carrier
        
        # cm_ins , providerTests_ins -> unique
        df_ins['master_carrier'] = df_ins['master_key'] + '-' + df_ins['carrier_name']
        
        df = df.sort_values( by=['master_key'] )
        df_ins = df_ins.sort_values( by=['master_key'] )

        # max values considerd as ip op

        
        df_ins['ip_price'] = df[ carrier + '-Ip' ].astype(float)
        df_ins['op_price'] = df[ carrier + '-Op' ].astype(float)

        # df_ins['outofnetwork_ip'] = df[ carrier + '-outofnetwork_ip' ].astype(float)
        # df_ins['outofnetwork_op'] = df[ carrier + '-outofnetwork_op' ].astype(float)

        # df_ins['emergency_outofnetwork_ip'] = df[ carrier + '-Emr_outofnetwork_ip' ].astype(float)
        # df_ins['emergency_outofnetwork_op'] = df[ carrier + '-Emr_outofnetwork_op' ].astype(float)

        
        df_ins = df_ins.loc[~((df_ins['ip_price'] == 0) & (df_ins['op_price'] == 0) )]
        


        # df_ins['Min_INS'] = df[ carrier + '-Min' ].astype(float)
        # df_ins['Max_INS'] = df[ carrier + '-Max' ].astype(float)

        # df_ins['Professional_fee_ip'] = df[ carrier + '-Pro_ip' ].astype(float)
        # df_ins['Professional_fee_op'] = df[ carrier + '-Pro_op' ].astype(float)
        # df_ins = df_ins.loc[~((df_ins['Professional_fee_op'] == 0) & (df_ins['Professional_fee_ip'] == 0))]

        # print(df_ins)
        
        df_ins_final.append(df_ins)
        
    return df_ins_final

def modify_test_code( test, drg_dict, Insurance_cols ,providerId , carrier_dict ,
                is_child=False , parent_id=None,master_key_set=set(), is_bundled=False,  temp_ID=None):   #parentChild
    
    if 'inpatient' in test:
        needed_columns = ['billingId','inpatient','year_of_insertion','provider','provider_name','medicare_number',
                                  'code_type','cpt_code','test_name','state_code','discount',
                                  'location','isShoppable','isServicable','is_bundled',
                                  'short_description','quantity','is_active',
                                  'master_key','master_id','cptcode_type',
                                  'isMandatory','discount','std_price_ip','std_price_op',
                                  'cash_price_ip','cash_price_op','max_price_ip',
                                   'max_price_op','min_price_ip','min_price_op']
    else:
        needed_columns = ['billingId','year_of_insertion','provider','provider_name','medicare_number',
                                  'code_type','cpt_code','test_name','state_code','discount',
                                  'location','isShoppable','isServicable','is_bundled',
                                  'short_description','quantity','is_active',
                                  'master_key','master_id','cptcode_type',
                                  'isMandatory','discount','std_price_ip','std_price_op','er_price_ip','er_price_op',
                                  'cash_price_ip','cash_price_op','max_price_ip',
                                   'max_price_op','min_price_ip','min_price_op']


    obj = OrderedDict({})

    for old in needed_columns:
        if old in test:
            obj[ old ] = test[ old ]
    
    temp_MK_ID = ''
    
    if is_child == True:
        obj['parentId'] = parent_id
        obj['superKey'] = parent_id            
        obj['test_name'] = test['test_name'].upper().strip()       
        temp_MK_ID = str(parent_id)
        
    else:
        obj['id'] = temp_ID
        obj['test_name'] = test['test_name'].strip().upper()
        temp_MK_ID = str(temp_ID)
            

    obj['is_bundled'] = is_bundled
        
    obj['quantity'] = 0
        
       
    obj['cpt_code_type'] = obj['master_key']
    obj['cpt_code'] = obj['cpt_code']
    
    if 'cpt_code' in obj and bool( obj['cpt_code'] ) :

        drg_list = list( filter( lambda x : x['master_key'] == obj['cpt_code_type'] , drg_dict ) )

        if len(drg_list) > 0:
            obj['master_id'] = drg_list[0]['id']
            obj['isMandatory'] = drg_list[0]['isMandatory']
            if obj['isMandatory'] == True and is_child==False:
                obj['test_name'] = drg_list[0]['test_name']

        obj['cptcode_type'] = providerId + "-" + obj['cpt_code'] + "-" + obj['code_type']

        if 'test_name' in obj:
            obj['encoded_message'] = hash_md5(  obj['test_name'] )

            concat_list = [

                str(parent_id) + '-' + providerId if parent_id else providerId ,
                obj['cpt_code'] ,
                obj['code_type'] ,
                obj['encoded_message'] ,
                str( obj['master_id'] ) if 'master_id' in obj else temp_MK_ID ,#code type 6 - _tempId (internal code)#cptnumber smallletter
                str( obj['min_price_op'] ) if 'min_price_op' in obj else '0' ,
                str( obj['min_price_ip'] ) if 'min_price_ip' in obj else '0' ,
                str( obj['max_price_op'] ) if 'max_price_op' in obj else '0' ,
                str( obj['max_price_ip'] ) if 'max_price_ip' in obj else '0' ,
                '0',
                '0',
                str( obj['cash_price_op'] ) if 'cash_price_op' in obj else '0' ,
                str( obj['cash_price_ip'] ) if 'cash_price_ip' in obj else '0' ,
                str( obj['std_price_op'] ) if 'std_price_op' in obj else '0' ,
                str( obj['std_price_ip'] ) if 'std_price_ip' in obj else '0' ,

                ]

            obj['master_key'] = master_key = "-".join(concat_list)
                
            if master_key in master_key_set:
                print("not inserted ",master_key)
                return (None,None)
            else:
                master_key_set.add(master_key)

    else:
        print('no cpt_code' , obj)
    
    keys_to_remove = { 'child_cpt_code','child_test_name', 'cpt_code_type' , 'cpt_code_', 'encoded_message','test_name_'}
    for col in keys_to_remove:
        if col in obj:
            del obj[col]
            

    ins_list = []
    
    for carrier in Insurance_cols:
        
        obj_ins = obj.copy()

        if 'id' in obj_ins:
            del obj_ins['id']

        carrier_object = list( filter( lambda x : x['carrier_name'] == carrier.upper() , carrier_dict) )

        if len(carrier_object) > 0 :
            # print('found Carrier => ',carrier_object[0])
            obj_ins['carrier_id'] = carrier_object[0]['carrier_id']

        obj_ins['carrier_name'] = carrier.upper()

        # cm_ins , providerTests_ins -> unique
        if 'master_key' in obj_ins:
            obj_ins['master_carrier'] = obj_ins['master_key'] + '-' + obj_ins['carrier_name']

        # filling the in-patient values for that carrier

        obj_ins['ip_price'] = test[carrier +  '-Ip' ]
        obj_ins['op_price'] = test[carrier +  '-Op' ]
            
    
        ins_list.append( obj_ins )

    return (obj, ins_list)





def insert_cm(main,location=None):
    if isinstance(main, pd.DataFrame):
        sample_main = main.to_dict('records')
    else:
        sample_main = main
    #sample_main['location']=[-75.611502, 40.243941]

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in sample_main]
        

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in sample_main]
    

    list_main = [ Charge_Master(**main) for main in new_list ]

    print("These many Charge Master Data to Insert : {} ".format(len(list_main)))
    insert = Charge_Master.objects.insert( list_main ,load_bulk=True)

    print("Inserted Charge Master Data : ",len(insert))
    
    return
	
def CM_Ins_insert(main,location=None):

    if isinstance(main, pd.DataFrame):
        sample_insurance = main.to_dict('records')
    else:
        sample_insurance = main
       

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in sample_insurance]

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in sample_insurance]

    list_provider_tests = [ CM_Insurances(**ins) for ins in new_list ]

#     print('These many Charge master Insurances to Insert : {}'.format( len( list_provider_tests ) ) )

    insert = CM_Insurances.objects.insert( list_provider_tests ,load_bulk=False)

#     print('Inserted many Charge master Insurances : {}'.format( len( insert ) ) )

    return



##########################################################################################

def insert_cm_1(main,location=None):
    if isinstance(main, pd.DataFrame):
        sample_main = main.to_dict('records')
    else:
        sample_main = main
    #sample_main['location']=[-75.611502, 40.243941]

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in sample_main]
        

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in sample_main]
    

    list_main = [ TestSchema(**main) for main in new_list ]

    print("These many Charge Master Data to Insert : {} ".format(len(list_main)))
    insert = TestSchema.objects.insert( list_main ,load_bulk=True)

    print("Inserted Charge Master Data : ",len(insert))
    
    return
	
def CM_Ins_insert_1(main,location=None):

    if isinstance(main, pd.DataFrame):
        sample_insurance = main.to_dict('records')
    else:
        sample_insurance = main
       

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in sample_insurance]

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in sample_insurance]

    list_provider_tests = [ Provider_Insurance(**ins) for ins in new_list ]

#     print('These many Charge master Insurances to Insert : {}'.format( len( list_provider_tests ) ) )

    insert = Provider_Insurance.objects.insert( list_provider_tests ,load_bulk=False)

#     print('Inserted many Charge master Insurances : {}'.format( len( insert ) ) )

    return



def con_insert_cmins_1(main_list,location=None):
    try:
        print('Inserting Charge Master Insurance' )
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map( lambda x : CM_Ins_insert_1( x , location) , main_list)
        print('Inserted Charge Master Insurance' )
    except Exception as error:
        print('DuplicateKeyError',error)
	   
	
#########################################################################################



    
def con_insert_cmins(main_list,location=None):
    try:
        print('Inserting Charge Master Insurance' )
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map( lambda x : CM_Ins_insert( x , location) , main_list)
        print('Inserted Charge Master Insurance' )
    except Exception as error:
        print('DuplicateKeyError',error)
	   





def insert_drglook(df,has_chandra_approval=True):
    print("________________________________________________________________________________________________")
    
    df = df[df.code_type != '6']

    master_keys = df['master_key'].unique()

    # Item_Service_Name , Item_Service_Description , Item_Service_Code
    if len(master_keys) == df.shape[0]:
        print('Found no duplicates in drg data')
    else:
        df = df.drop_duplicates(subset=['master_key'])

    if 'inpatient' in df.columns:
        columns_needed = [
                            'master_key',
                            'test_name',
                            'short_description',
                            'cpt_code',
                            'code_type',
                            'inpatient'
                        ]
    else:
        columns_needed = [
                            'master_key',
                            'test_name',
                            'short_description',
                            'cpt_code',
                            'code_type',
                        ]

    print('DRG_lookup => ',df[columns_needed].shape)


    main_df = df[columns_needed]

    objects = DRG_lookup_new.objects(master_key__in=master_keys)

    drg_dict = [ dict( {
            '_id' : ob['id'] ,
            'cpt_code' : ob['cpt_code'] ,
            'code_type' : ob['code_type'] ,
            'master_key' : ob['master_key'],
            'isMandatory' : ob['isMandatory'],
            'isShoppable' : ob['isShoppable'] if 'isShoppable' in ob else False
    } ) for ob in objects ]

    found = len(drg_dict)

    print("Found {} records out of {} , Supposed to Insert : {}".format( found , len(master_keys) , len(master_keys) - found ))

    drg_df = pd.DataFrame.from_records(drg_dict)
    # main_df = pd.DataFrame.from_records(main_list)

    main_df['master_key'] = main_df['master_key'].astype(str)

    if ( len(drg_df) > 0 ):
            found_drg = main_df['master_key'].isin(drg_df['master_key'])
            found_ones = main_df[found_drg]
            not_found_ones = main_df[~found_drg]
            not_found_ones_list = not_found_ones.to_dict('records')
    else:
            not_found_ones = main_df
            not_found_ones_list = not_found_ones.to_dict('records')


    if 'inpatient' in df.columns:
        allow_keys = {'test_name', 'cpt_code', 'code_type' , 'master_key','inpatient'}
    else:
        allow_keys = {'test_name', 'cpt_code', 'code_type' , 'master_key'}

    print("Found these many to Insert : {}".format( len(not_found_ones) ) )
    # print(not_found_ones)
    filter_not_found = [ update_class_allow(ob,allow_keys) for ob in not_found_ones_list ]

    Drg_list = [ DRG_lookup_new(**ob) for ob in filter_not_found ]
    
    if(has_chandra_approval and len(Drg_list)>0):
            inserted = DRG_lookup_new.objects.insert(Drg_list , load_bulk=True)

            inserted_dict = [ dict( {
                    '_id' : ob['id'] ,
                    'cpt_code' : ob['cpt_code'] ,
                    'code_type' : ob['code_type'] ,
                    'master_key' : ob['master_key'],
                    'isMandatory' : ob['isMandatory'],
                    'isShoppable' : ob['isShoppable'] if 'isShoppable' in ob else False ,
                    'inpatient' : ob['inpatient'] if 'inpatient' in ob else 1
                    
            } ) for ob in inserted ]

            drg_dict.extend(inserted_dict)        
            print("This many {} Inserted in this Format : {}".format( len(inserted), filter_not_found[0]))


    elif(has_chandra_approval!=True and len(Drg_list)>0) :
            print("Please get chandra approval for the above file save as needed_cpts.csv")
            # not_found_ones.to_csv(os.path.join('output','needed_cpts.csv'),index=False)
    else:
            print("Already all are in Database total:",len(drg_dict))

    return (drg_dict)


def insert_OptionalItems(df,has_chandra_approval=True):
    
    print("________________________________________________________________________________________________")
    
    df = df[df.code_type != '6']

    master_keys = df['master_key'].unique()

    # Item_Service_Name , Item_Service_Description , Item_Service_Code
    if len(master_keys) == df.shape[0]:
        print('Found no duplicates in optional data')
    else:
        df = df.drop_duplicates(subset=['master_key'])

    columns_needed = [
        'master_key',
        'test_name',
        'short_description',
        'cpt_code',
        'code_type',
        'provider'
    ]

    print('Optional_lookup => ',df[columns_needed].shape)


    main_df = df[columns_needed]
    
    objects = OptionalItems.objects(master_key__in=master_keys)

    optional_dict = [ dict( {
            '_id' : ob['id'] ,
            'cpt_code' : ob['cpt_code'] ,
            'code_type' : ob['code_type'] ,
            'master_key' : ob['master_key'],
            'provider' : ob['provider']
    } ) for ob in objects ]

    found = len(optional_dict)

    print("Found {} records out of {} , Supposed to Insert : {}".format( found , len(master_keys) , len(master_keys) - found ))

    optional_df = pd.DataFrame.from_records(optional_dict)
    # main_df = pd.DataFrame.from_records(main_list)

    main_df['master_key'] = main_df['master_key'].astype(str)

    if ( len(optional_df) > 0 ):
            found_drg = main_df['master_key'].isin(optional_df['master_key'])
            found_ones = main_df[found_drg]
            not_found_ones = main_df[~found_drg]
            not_found_ones_list = not_found_ones.to_dict('records')
    else:
            not_found_ones = main_df
            not_found_ones_list = not_found_ones.to_dict('records')


    allow_keys = {'test_name', 'cpt_code', 'code_type' , 'master_key','provider'}

    print("Found these many to Insert : {}".format( len(not_found_ones) ) )
    # print(not_found_ones)
    filter_not_found = [ update_class_allow(ob,allow_keys) for ob in not_found_ones_list ]

    Optional_list = [ OptionalItems(**ob) for ob in filter_not_found ]
    
    if(has_chandra_approval and len(Optional_list)>0):
            inserted = OptionalItems.objects.insert(Optional_list , load_bulk=True)

            inserted_dict = [ dict( {
                    '_id' : ob['id'] ,
                    'cpt_code' : ob['cpt_code'] ,
                    'code_type' : ob['code_type'] ,
                    'master_key' : ob['master_key'],
                    'provider' : ob['provider']
            } ) for ob in inserted ]

            optional_dict.extend(inserted_dict)        
            print("This many {} Inserted in this Format : {}".format( len(inserted), filter_not_found[0]))


    elif(has_chandra_approval!=True and len(Optional_list)>0) :
            print("Please get chandra approval for the above file save as needed_cpts.csv")
            # not_found_ones.to_csv(os.path.join('output','needed_cpts.csv'),index=False)
    else:
            print("Already all are in Database total:",len(optional_dict))

    return 


def insert_insurance_carriers(carrier_key_set, has_chandra_approval=True):

    print("________________________________________________________________________________________________")

    main_list = list([])
    carrier_keys=list([])

    for value in carrier_key_set:

        obj = dict({})

        obj['Name'] = value
        carrier_keys.insert(0,value)
        main_list.append(obj)

    # print('carrier_keys : ',carrier_keys)

    insurances_available=Insurence_lookup.objects(Name__in=carrier_keys)

    carrier_dict = [ dict( {
            'carrier_id' : ob['id'] ,
            'carrier_name' : ob['Name']
    } ) for ob in insurances_available ]

    print('carrier_dict : ',len(carrier_dict))

    if len(carrier_dict) > 0:

        carrier_df = pd.DataFrame.from_records(carrier_dict)
        main_df = pd.DataFrame.from_records(main_list)
        found_carriers = main_df['Name'].isin(carrier_df['carrier_name'])
        found_ones = main_df[found_carriers]
        not_found_ones = main_df[~found_carriers]
        print("Found {} records out of {} , Supposed to Insert : {}".format( len(found_carriers) , len(carrier_keys) , len(carrier_keys) - len(found_carriers) ))
    else :
        main_df = pd.DataFrame.from_records(main_list)
        not_found_ones = main_df
        print("Found {} records out of {} , Supposed to Insert : {}".format( len(carrier_dict) , len(carrier_keys) , len(carrier_keys) - len(carrier_dict) ))



    if(len(not_found_ones)>0):
            not_found_ones_list = not_found_ones.to_dict('records')
    else:
            not_found_ones_list=[]



    # print(not_found_ones_list)

    allow_keys = {'Name'}

    filter_not_found = [ update_class_allow(ob,allow_keys) for ob in not_found_ones_list ]

    Carry_list = [ Insurence_lookup(**ob) for ob in filter_not_found ]
    if(has_chandra_approval and len(Carry_list)>0):
            inserted = Insurence_lookup.objects.insert(Carry_list , load_bulk=True)

            inserted_dict = [ dict( {
            'carrier_id' : ob['id'] ,
            'carrier_name' : ob['Name']
            } ) for ob in inserted ]

            print("This many {} Inserted in this Format : {}".format( len(inserted), filter_not_found[0]))
            carrier_dict.extend(inserted_dict)
            
    elif(has_chandra_approval!=True and len(Carry_list)>0) :
            print("Please get chandra approval for the above file save as needed_carriers.csv")
            # not_found_ones.to_csv(os.path.join('output','needed_carriers.csv'),index=False)
    else:
            print("Carriers Are already inserted!:",len(carrier_dict))

    return (carrier_dict)


def map_insert_provider(carrier_dict,Provider_Id,State_code):
    
    print("________________________________________________________________________________________________")

    provider_dict = { 'providerId' : Provider_Id 
    , 'state_code' : State_code 
    }

    existing_ones = ProviderInsurance.objects( providerId = Provider_Id , carrier_id__in = [ ob['carrier_id'] for ob in carrier_dict ])

    print('Existing ProviderInsurance : {} out of These many carriers {}'.format( len(existing_ones) , len(carrier_dict) ) )

    '''
    This takes your O(N*M) algorithm and turns it into an O(max(N,M)) algorithm.
    '''
    if ( len(existing_ones) == len(carrier_dict)    ):
        # all the already existing
        provider_dict = []

    elif ( len(existing_ones) > 0 ):
        # someone are already existing so we have ignore the existing ones
        existing_list = list(dict({

                    'name' : ob['name'],
                    'carrier_id' : ob['carrier_id']['id']
        }) for ob in existing_ones)

        # print('Format : ',existing_list[0])

        non_existing_ones = [ ob for ob in carrier_dict if dict({
            
                'name' : ob['carrier_name'],
                'carrier_id' : ob['carrier_id']
        }) not in existing_list ]


        print('These many {} ProviderInsurances to insert'.format(len(non_existing_ones) ) )
        print('Format : ',non_existing_ones[0])

        provider_dict = [ dict({
                        'providerId' : Provider_Id ,
                        'state_code' : State_code ,
                        'name' : ob['carrier_name'],
                        'carrier_id' : ob['carrier_id']
                    }) for ob in non_existing_ones ]

    else:
        # none are existing , we have to insert all of them (carrier_dict)
        provider_dict = [ dict({
                'providerId' : Provider_Id ,
                'state_code' : State_code ,
                'name' : ob['carrier_name'],
                'carrier_id' : ob['carrier_id']
            }) for ob in carrier_dict ]



    provider_insurance_list = [ ProviderInsurance(**ob) for ob in provider_dict]

    if len(provider_insurance_list) > 0:

        provider_insurance_list

        inserted_provider_ins = ProviderInsurance.objects.insert(provider_insurance_list,load_bulk=True)
        
        print('Inserted these {}    many in this format {}'.format(len(inserted_provider_ins),provider_dict[0]))

    else:

        print('Got No Data to Insert')

    return []


def insert_providerTests(main,location=None):

    if isinstance(main, pd.DataFrame):
        provider_tests = main.to_dict('records')
    else:
        provider_tests = main

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in provider_tests]

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in provider_tests]
    
    list_provider_tests = [ TestSchema(**ins) for ins in new_list ]

    print("These many Provider Tests Data to Insert : {} ".format(len(list_provider_tests)))
   
    insert = TestSchema.objects.insert( list_provider_tests ,load_bulk=True)

    print('Inserted many providerTests : {}'.format( len( insert ) ) )
        
        
    return 

def insert_providerTests_myChart(main,location=None):#TestSchemaMyChart

    if isinstance(main, pd.DataFrame):
        provider_tests = main.to_dict('records')
    else:
        provider_tests = main

    if location:
        location = {"location":location}
        [ ob.update(location) for ob in provider_tests]

    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in provider_tests]
    
    list_provider_tests = [ TestSchemaMyChart(**ins) for ins in new_list ]

    print("These many Provider Tests Data to Insert : {} ".format(len(list_provider_tests)))
   
    insert = TestSchemaMyChart.objects.insert( list_provider_tests ,load_bulk=True)

        
    return 
	
def proIns_insert(tests,location):
        
    if isinstance(tests, pd.DataFrame):
        provider_ins = tests.to_dict('records')
    else:
        provider_ins = tests
    
    if location:
        location = {"location":location}
        [ ob.update(location) for ob in provider_ins]
        
    new_list = [{k: v for k, v in d.items() if k != 'inpatient'} for d in provider_ins]
    
    list_provider_tests = [ Provider_Insurance(**ins) for ins in new_list ]

    print('These many providerTest Insurances to Insert : {}'.format( len( list_provider_tests ) ) )

    insert = Provider_Insurance.objects.insert( list_provider_tests ,load_bulk=True)

    print('Inserted many providerTest Insurances : {}'.format( len( insert ) ) )
    
    return
