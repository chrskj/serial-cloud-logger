import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import csv
import matplotlib.dates as mdates
import os


def add_header(source):
    temp_file = source[0:20] + "2.csv"
    src = open(source, 'r', newline='')
    reader = csv.reader(src, delimiter=';')
    dst = open(temp_file, 'w', newline='')
    writer = csv.writer(dst, delimiter=';')
    writer.writerow(["Time","Flow 1 [m^3]","Flow 2 [m^3]","Temp [C]","Diff 1 [L]","Diff 2 [L]","Press Min [bar]","Press Max [bar]","Press Inst [bar]", "RSSI [1-255]"])
    for row in reader:
        writer.writerow(row)
    src.close()
    dst.close()
    os.remove(source)
    os.rename(temp_file, source)


def replace_header(source):
    temp_file = source[0:20] + "2.csv"
    src = open(source, 'r', newline='')
    reader = csv.reader(src, delimiter=';')
    dst = open(temp_file, 'w', newline='')
    writer = csv.writer(dst, delimiter=';')
    writer.writerow(["Time","Flow 1 [m^3]","Flow 2 [m^3]","Temp [C]","Diff 1 [L]","Diff 2 [L]","Press Min [bar]","Press Max [bar]","Press Inst [bar]", "RSSI [1-255]"])
    next(reader)
    for row in reader:
        writer.writerow(row)
    src.close()
    dst.close()
    os.remove(source)
    os.rename(temp_file, source)


def graph_data(source):
    graph_name = source[0:10]

    df = pd.read_csv(source, delimiter=';', index_col=False)
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S')
    #df0 = pd.DataFrame(df, columns=['Diff 1 [L]','Press Inst [bar]'])
    #print(df0)
    #df0.plot(marker='.')

    #df.plot(x ='Time', y='Diff 1 [L]', kind = 'bar')
    
    df1 = pd.DataFrame(df, columns=['Time','Diff 1 [L]'])
    df1 = df1.dropna()
    ax = df1.plot(x ='Time', 
                  y='Diff 1 [L]', 
                  kind = 'line', 
                  title=graph_name, 
                  figsize=(16,5))

    df2 = pd.DataFrame(df, columns=['Time','Press Min [bar]', 'Press Max [bar]', 'Press Inst [bar]'])
    df2 = df2.dropna()
    df2.plot(ax=ax, 
             x ='Time', 
             kind = 'line', 
             grid=True)

    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    _=plt.xticks(rotation=45)    
    ax.set_yticks(np.arange(0, 21, 1))
    
    plt.savefig(graph_name)
    #plt.show()

def main():
    #for i in ['10']:
    for i in ['15']:
        data_file = '2021-03-' + i + '-formatted.csv'
        #add_header(data_file)
        #replace_header(data_file)
        graph_data(data_file)

    #data_file = "2021-02-28-formatted.csv"
    #add_header(data_file)
    #graph_data(data_file)

if __name__ == "__main__":
    main()
