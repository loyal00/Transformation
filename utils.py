
code_type=["HCPCS","CPT","APC","APR_Drg","DRG","NDC","Custom"]
import hashlib
from bson.objectid import ObjectId

#Re-Usable Functions
def trim(x):
    return x.strip()

# Find dictionary matching value in list
def getMatchingValuesFromList(list_vals,key,value):

    for sub in list_vals:
        if(sub):
          #print(sub)
            if (sub[key]== value):
                  return sub

#Data Clean-Up Functions
def name(x):
  return x.split('[')[0].strip()

def code(x):
  try :
      return x.split('[')[1].replace(']','')
  except IndexError as i:
      # print('exception',x)
      return ''

def apply_code(x):
  if x == 'ERX':
      return 'Drug_Fee'
  elif x == 'SUP':
      return 'Supply_Fee'
  elif x == 'EAP':
      return 'Primary Procedure'
  elif x=='DRG':
      return 'Primary Procedure'
  return x

def transform_code(x):

  if "HCPCS " in x:
      return x.replace("HCPCS ","")
  elif "CPT" in x:
      return x.replace("CPT ","")
  elif "MS-DRG V38 (FY 2021) " in x:
      return x.replace("MS-DRG V38 (FY 2021) ","")
  elif "Custom " in x:
      return x.replace("Custom ","")
  else:
      return x


# get codetype number
def getCodeType(x):

  return code_type.index(x)

def get_code_type(x):
  # 0->HCPCS ,1->CPT , 2->APC, 3->APR_Drg, 4->MS_DRG, 5->NDC, 6->OTHER
  if(x==None):
    return 6
  else:
    code_types = ["HCPCS" ,"CPT" , "APC", "APR_Drg", "DRG", "NDC", "OTHER"]
    if(x not in code_types):
      return 6
    else:
      return code_types.index(x)

def update_class_allow(inc_obj=dict({}),allow_class={}):
  obj = dict({})

  for key in allow_class:
      obj[key] = inc_obj[key]

  return obj

def hash_md5(x):
  # print(str(x))
  if(str(x)=="nan"):
    return ""
  else:
    return hashlib.md5(x.encode('utf-8')).hexdigest()

def objectID_assign(x):
        return ObjectId()

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def apply_code_type(x):
    if len(x) == 5 and "-" not in x:

        if x.isnumeric() == True:
            return 'CPT'
        elif x.isalnum() == True:
            return 'HCPCS'
        else:
            return None

    elif len(x.replace('MS-','')) == 3:

        if x.replace('MS-','').isnumeric() == True:
            return 'DRG'
        else:
            return None
            
    elif len(x.replace('-','')) == 4 and "-" in x or len(x.replace('APR-','')) == 3:

        if x.replace('APR-','').replace('-','').isnumeric() == True:
            return 'APR_Drg'
        else:
            return None
            
    elif len(x.replace("-","")) == 11:

        if x.replace("-","").isnumeric() == True:
            return 'NDC'
        else:
            return None
    
    elif len(x) == 4 and '-' not in x:
        return 'APC'


    elif len(x) == 10 and "-" not in x:

        return 'OTHER'

    else:
        # print(x)
        return None
