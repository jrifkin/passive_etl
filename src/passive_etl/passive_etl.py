
def read_spss(path = ''):
    import savReaderWriter as spss
    import pandas as pd

    if len(path)==0:
        path = r"O:\MSI\PERSONAL FOLDERS\Files for Jake\Passive\passive_etl\Data\Dimension + P3 Data.sav"
    
    data = spss.SavReader(path, returnHeader=True)

    df = pd.DataFrame(list(data),columns=data.varNames)
        
    return df



def main():
    #quarterback module
    data = read_spss()
    print data.tail()

if __name__ == '__main__':
    from datetime import datetime

    start_time = datetime.now()

    try:
        main()
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)
    except Exception, e:
        print 'Error:'
        print e.message
        print 'Run-Time Elapsed: %s' %(datetime.now()-start_time)