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
    
    
def plotByHist(data, x_label, plt):
    cdata = {}
    for result in data:
        cdata[result['key']] = result['doc_count']
    
    labels = list(cdata.keys())
    values = list(cdata.values())
    plt.figure(figsize=(8,4.5), dpi=128)
    plt.bar(labels, values)
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False
    )
    plt.xlabel(x_label)
    plt.ylabel('Frequency')
    plt.yscale('log')
