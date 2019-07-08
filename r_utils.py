from datetime import datetime, timedelta

def getDateFormat(dte = None, delta = 0):
    '''
    Returns the current date and time in the format required for Datetime. 
    delta : the days before the current date, you want the date
    '''
    
    if dte is not None:
        return str(dte.timestamp()*1000)
    else:
        return str((datetime.now() - timedelta(days = delta )).timestamp()*1000)
    
    
def plotByHist(data, n, x_label, plt):
    cdata = {}
    for result in data:
        cdata[result['key']] = result['doc_count']
    
    labels = list(cdata.keys())
    values = list(cdata.values())
    plt.figure(figsize=(16,9), dpi=512)
    plt.bar(labels[:n], values[:n])
    plt.xticks(rotation=90,fontsize=8)
    plt.xlabel(x_label)
    plt.ylabel('Frequency')
    plt.yscale('log')
