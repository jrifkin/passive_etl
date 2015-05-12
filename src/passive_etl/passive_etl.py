from datetime import datetime

def read_spss(path = ''):
    import savReaderWriter as spss
    import pandas as pd

    if len(path)==0:
        path = r"O:\MSI\PERSONAL FOLDERS\Files for Jake\Passive\Passive Panel\Data\Dimension+P3 data_06-May-2015.sav"

    print '[INFO] Starting SPSS import on file:\n%s' %path
    
    load_start = datetime.now()
    data = spss.SavReader(path, returnHeader=True)
    df = pd.DataFrame(list(data),columns=data.varNames)
    load_finish = datetime.now()
    
    timer = load_finish-load_start
    initial_rows = len(df)

    print '\n[INFO] Total records import from spss: %i' %initial_rows
    print '[INFO] In %s seconds at %s records/second' %(timer,initial_rows/timer.seconds)
    
    #keep only necessary columns
    keep_cols =  ["Respondent_ID","Respondent_Serial","NETWORK","SMARTPHONE_BRAND","SMARTPHONE_MODEL","APP_DOWNLOAD","EMAIL_CAPTURE","EMAIL_CONFIRM","resp_gender","resp_age","EMPLOYMENT","PERSONAL_USE01","PERSONAL_USE02","PERSONAL_USE03","PERSONAL_USE04","PERSONAL_USE05","PERSONAL_USE06","PERSONAL_USE07","PERSONAL_USE08","PERSONAL_USE09","PERSONAL_USE10","PERSONAL_USE11","PERSONAL_USE12","PERSONAL_USE13","PERSONAL_USE14","PERSONAL_USE15","PERSONAL_USE16","DEVICE_TYPE","CARRIER","DATA_PLAN","DATA_LIMIT"]
    kill_cols = [col for col in data.varNames if not(col in keep_cols)]
            
    df.drop(kill_cols,inplace=True,axis=1)

    #only keep records of respondents that answered 1 to APP_DOWNLOAD
    df = df[df.APP_DOWNLOAD == 1]
    
    cleaned_rows = len(df)

    print '[INFO] Number of records cleaned: %d' %(initial_rows - cleaned_rows)
    print '[INFO] Number of records left after cleaning: %d' %cleaned_rows
    
    return df


def to_sql(df, connection = None):
    #take in data fram and push to sql
    import sqlalchemy
    import pyodbc

    Driver = 'pyodbc'
    Server = 'mstelms.extranet.iext\\mstelms'
    User = 'ipsosgroup\\jake.rifkin'
    Password = 'Darklord18'
    # If the database you want to connect to is the default
    # for the SQL Server login, omit this attribute     
    Database = 'PassiveTest'


    if connection == None:
        #if no connection, default to local mysql
        connection = 'mysql+mysqldb://root:morpheus@localhost/test'
        engine = sqlalchemy.create_engine(connection)
    else:
        engine = sqlalchemy.create_engine('mssql://',creator=connect)

    df.to_sql('passive_data',engine, index = False, chunksize = 100, if_exists='replace')

def connect():
    return pyodbc.connect("DRIVER={SQL Server};SERVER=mstelms.extranet.iext\\mstelms;DATABASE=PassiveTest")

def main():
    #quarterback module
    data = read_spss()

    my_connec = "DRIVER={SQL Server};SERVER=mstelms.extranet.iext\\mstelms;DATABASE=PassiveTest"

    #push data to sql
    to_sql(data,my_connec)


if __name__ == '__main__':

    start_time = datetime.now()

    try:
        main()
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)
    except Exception, e:
        print 'Error:'
        print e.message
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)