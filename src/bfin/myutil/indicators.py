from scipy.signal import argrelextrema
import numpy as np
import pandas_ta as ta

def add_extrema(df, **kwargs):
    '''
    kwargs
        period_short        int
        period_medium       int
        period_long         int
    '''
    periods = (
        (kwargs.get('period_short'), 'S'),
        (kwargs.get('period_medium'), 'M'),
        (kwargs.get('period_long'), 'L'),
    )

    for period, letter in periods:
        if period is None:
            continue
        ilocs_max = argrelextrema(df['High'].values, np.greater_equal, order=period)[0]
        ilocs_min = argrelextrema(df['Low'].values, np.less_equal, order=period)[0]
        ind_max = df.iloc[ilocs_max].index
        ind_min = df.iloc[ilocs_min].index
        df.loc[ind_max, f'E{letter}_HI'] = df.loc[ind_max, 'High']
        df.loc[ind_min, f'E{letter}_LO'] = df.loc[ind_min, 'Low']


def add_moving_average_ribbon(df):
    '''
    Adds the basic 50-100-200 moving average ribbon to the dataframe
    '''
    df['ma_50'] = ta.sma(df['Close'], 50)
    df['ma_100'] = ta.sma(df['Close'], 100)
    df['ma_200'] = ta.sma(df['Close'], 200)