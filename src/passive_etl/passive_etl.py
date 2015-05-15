from datetime import datetime
import savReaderWriter as spss
import pandas as pd
import sqlalchemy
import pyodbc
import ConfigParser
import os
import glob
import operator

ops = {"==": operator.eq,
        "!=": operator.ne,
        "<": operator.lt,
        ">": operator.gt,
        "<=": operator.le,
        ">=": operator.ge}

def get_file(path='',dir=''):
    #determine a file to load to sql, must be .sav at this point

    if os.path.isfile(path):
        #use specific file
        file = path
    elif os.path.isdir(dir):
        #find most recently updated .sav file
        file = max(glob.iglob(os.path.join(dir,'*.sav')),key=os.path.getctime)
    else:
        raise ValueError('No .sav has been specified or can be found in the config directory. Check the config.conf file and enusre that a valid path or dir has been specified')

    return file


def read_spss(cf):

    #determine file to open
    spss_file = get_file(cf['path'],cf['dir'])
   
    print '[INFO] Starting SPSS import on file:\n%s' %spss_file
    
    if len(cf['kill_cols']) == 0:
        select_vars = cf['keep_cols'] if len(cf['keep_cols']) > 0 else None
    else:
        check_vars = spss.SavHeaderReader(spss_file).varNames
        select_vars = [x for x in check_vars if not(x in cf['kill_cols'])]
    
    load_start = datetime.now()
    data = spss.SavReader(spss_file, returnHeader=False,selectVars=select_vars)
    df = pd.DataFrame(list(data),columns=select_vars)
    load_finish = datetime.now()
    
    timer = load_finish-load_start
    initial_rows = len(df)

    print '\n[INFO] Total records import from spss: %i' %initial_rows
    print '[INFO] In %s seconds at %s records/second' %(timer,initial_rows/timer.seconds)
    print '[INFO] Creating value dictionaries'

    #create value dictionaries
    my_dict = data.valueLabels
    punch_definitions = pd.DataFrame(data.valueLabels)

    #punch_definitions is not responding to the selectVars input so manually cleaning out unwanted mapping labels
    for col in punch_definitions.columns:
        if not(col  in select_vars):
            punch_definitions.drop(col,inplace=True,axis=1)
    
    print '[INFO] All data loaded in memory successfully'

    return df,punch_definitions


def get_condition(str_logic):
    if len(str_logic[0])==0:
        return None

    col = []
    op = []
    val = []

    for logic in str_logic:
        col.append(logic[0])
        op.append(ops[logic[1]])
        try:
            val.append(int(logic[2]))
        except:
            val.append(logic[2])

    return col,op,val


def clean_data(cf,df):

    col,op,val = get_condition(cf['row_logic'])
    #Config scripted logic
    for i in range(len(col)):
        df = df[op[i](df[col[i]],val[i])]

    #OLD: manually scripted
    #df = df[df.APP_DOWNLOAD == 1]
    cleaned_rows = len(df)
    print '[INFO] Number of records left after cleaning: %d' %cleaned_rows
    
    #restack data into 3 columns
    #col1 = respondent_id, col2 = question_id, col3 = response_id
    if cf['stack']:
        print 'stacking logic here'


    return df


def to_sql(cf,spss_df,punch_data):
    #take in data fram and push to sql
    engine = sqlalchemy.create_engine(cf['type']+'+pyodbc://',creator=connect)
    
    if len(cf['table_name']) == 0:
        table_name = 'P3PassiveUpload'
        punch_name = 'P3PassiveUpload_punch'
    else:
        table_name = cf['table_name']
        punch_name = cf['table_name']+'_punch'
    
    print '[INFO] Starting sql load process for spss data at %s' %datetime.now()
    spss_df.to_sql(table_name,engine, index = False, if_exists='replace', schema='dbo')
    print '[INFO] SQL load finished spss data at %s' %datetime.now()

    print '[INFO] Starting sql load process for variable definitions as %s' %datetime.now()
    punch_data.to_sql(punch_name,engine,index = True,if_exists='replace',schema='reference')
    print '[INFO] SQL load finished spss data at %s' %datetime.now()


def connect():
    new_config = ConfigParser.ConfigParser()
    new_config.readfp(open("config.conf"))
    
    con = {}

    con['type'] = new_config.get('Database','type')
    con['host'] = new_config.get('Database','host')
    con['db_user'] = new_config.get('Database','db_user')
    con['db_name'] = new_config.get('Database','db_name')
    con['driver'] = new_config.get('Database','driver')

    connection_string = 'DRIVER={'+con['driver']+'};SERVER='+con['host']+';DATABASE='+con['db_name']
    #"DRIVER={SQL Server};SERVER=mstelms.extranet.iext\\mstelms;DATABASE=PassiveTest"
    return pyodbc.connect(connection_string)

def unpack_config(cf_file):
    new_config = ConfigParser.ConfigParser()
    new_config.readfp(open(cf_file))
    
    cf_dict = {}

    cf_dict['path'] = new_config.get('Script_Arguments','path')
    cf_dict['dir'] = new_config.get('Script_Arguments','dir')
    cf_dict['keep_cols'] = new_config.get('Script_Arguments','keep_cols').split(',') if len(new_config.get('Script_Arguments','keep_cols'))> 0 else None
    cf_dict['kill_cols'] = new_config.get('Script_Arguments','kill_cols').split(',') if len(new_config.get('Script_Arguments','kill_cols'))> 0 else None
    cf_dict['row_logic'] = [x.split(',') for x in new_config.get('Script_Arguments','row_logic').split(';')] if len(new_config.get('Script_Arguments','row_logic'))> 0 else None
    cf_dict['stack'] = True if new_config.get('Script_Arguments','stack').lower() == 'true' else False
    cf_dict['table_name'] = new_config.get('Script_Arguments','table_name')
    cf_dict['type'] = new_config.get('Database','type')
    cf_dict['host'] = new_config.get('Database','host')
    cf_dict['db_user'] = new_config.get('Database','db_user')
    cf_dict['db_name'] = new_config.get('Database','db_name')
    cf_dict['driver'] = new_config.get('Database','driver')

    return cf_dict

def main():
    #quarterback module
    cf = unpack_config("config.conf")
   
    #read in spss and return datafile and dictionary of dictionaries of punches
    spss_data,punch_dict = read_spss(cf)

    #clean the data,adding in different parameters to stack or keep rows or kill rows
    data = clean_data(cf,spss_data)

    #push data to sql
    #to_sql(cf,data,punch_dict)


if __name__ == '__main__':

    start_time = datetime.now()

    try:
        main()
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)
    except Exception, e:
        print 'Error:'
        print e.message
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)