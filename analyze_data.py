"""
This script has two functions:
- Generate a water demand pattern based on real volume measurements
- Resample pressure measurements to a desired interval, to be used for generating pressure residuals
"""
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates


def head2bar(head):
    return head * 0.09804

def bar2head(bar):
    return bar * 10.1974

def get_interpolated_pressure(source):
    """
    Get resampled values of pressure
    """
    df = pd.read_csv(source, delimiter=';', parse_dates=['Time'], index_col='Time')
    pressure_upsample = df['Press Inst [bar]'].resample('1T').mean()
    pressure_interpolate = pressure_upsample.interpolate()
    pressure_downsample = pressure_interpolate.resample('10T').mean()
    return pressure_downsample

def get_interpolated_flow(source):
    """
    Get resampled values of flow
    """
    df = pd.read_csv(source, delimiter=';', parse_dates=['Time'], index_col='Time')
    flow_upsample = df['Flow 1 [m^3]'].resample('1T').mean()
    flow_interpolate = flow_upsample.interpolate()
    flow_downsample = flow_interpolate.resample('10T').mean()
    return flow_downsample


def get_flow_rate(df):
    """
    Get the average flow for a time range, based on measured volume.
    """
    flow_rate_frame = pd.DataFrame.copy(df)
    flow_rate_frame.iloc[0] = 0
    for i in range(1, len(df)):
        # Convert from m^3 to L with 1000. Should be divided by 600 for 10 minutes
        flow_rate_frame.iloc[i] = 120*(1000/600)*df.iloc[i] - 120*(1000/600)*df.iloc[i-1]
    return flow_rate_frame

def graph_dataframes(dfs):
    """
    Graph all dataframes (dfs) that is set as input
    """
    first_frame = dfs[0]
    ax = first_frame.plot(kind = 'line', 
                  title='Water Demand Pattern for 2021-03-06', 
                  figsize=(16,5))

    for i in range(1, len(dfs)):
        dfs[i].plot(ax=ax,
                color='r', 
                kind = 'line', 
                grid=True,
                label='Estimated Flow Rate [LPS]')
    ax.legend()
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=60))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.show()

def print_frame(df):
    """
    Set options for the printing of dataframes
    """
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df)

def main():
    """
    - Save interpolated pressure for pressure residual generation and flow rate for EPANET model
    - Graph flow rate and volume measurements to see difference
    """
    source_location = "../../data/2021/03-mar/"
    save_location = "../../data/pressure_residual_calculation/"
    data_file = source_location + '2021-03-02-formatted.csv'

    # Load dataframes
    df = pd.read_csv(data_file, delimiter=';', parse_dates=['Time'], index_col='Time')
    df_press = pd.DataFrame(df, columns=['Press Inst [bar]'])
    df_flow = pd.DataFrame(df, columns=['Flow 1 [m^3]'])
    df_diff = pd.DataFrame(df, columns=['Diff 1 [L]'])

    # Data Preprocessing    
    df_press = df_press.dropna()
    df_flow = df_flow.dropna()
    df_diff = df_diff.rename(columns={'Diff 1 [L]': 'Measured Volume [L]'}).dropna()

    # Process dataframes
    df_press_i = get_interpolated_pressure(data_file)
    df_flow_i = get_interpolated_flow(data_file)
    df_flow_rate = get_flow_rate(df_flow_i)

    # Save pressure and flow rate
    df_merge = pd.merge(df_press_i, df_flow_rate, on='Time')
    df_merge.to_excel(save_location + 'out.xlsx')

    # Graph dataframes in dfs
    dfs = [df_diff, df_flow_rate]
    graph_dataframes(dfs)

if __name__ == "__main__":
    main()
