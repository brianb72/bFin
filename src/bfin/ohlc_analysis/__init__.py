import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from tabulate import tabulate
import bfin.myutil as Utils
from datetime import timedelta

class OHLCAnalysis(object):

    @staticmethod
    def extrema_dataframe_to_table(instrument, time_frame, df_extrema, extrema_period):
        fmt = Utils.floatfmt(instrument)
        headers = ['Date', 'Value', 'Pips', 'Percent', 'Bars']
        floatfmt = [None, fmt, fmt, '.4%', None]
        table = []
        last_value = None
        for index, row in df_extrema.iterrows():
            table.append([
                row.name,
                row['extrema'],
                row['extrema'] - last_value if last_value else 0,
                (row['extrema'] - last_value) / last_value  if last_value else 0,
                int(row['bars']),
            ])
            last_value = row['extrema']
        return headers, table, tuple(floatfmt)


    @staticmethod
    def generate_extrema_dataframe(df, period):
        data = pd.DataFrame(index=df.index)
        ilocs_max = argrelextrema(df['High'].values, np.greater_equal, order=period)[0]
        ilocs_min = argrelextrema(df['Low'].values, np.less_equal, order=period)[0]
        ind_max = df.iloc[ilocs_max].index
        ind_min = df.iloc[ilocs_min].index
        data.loc[ind_max, 'extrema_high'] = df.loc[ind_max, 'High']
        data.loc[ind_min, 'extrema_low'] = df.loc[ind_min, 'Low']
        data['extrema'] = data['extrema_high']
        data.loc[data['extrema'].isnull(), 'extrema'] = data['extrema_low']
        data.dropna(subset=['extrema'], inplace=True)
        data['next_date'] = data.index
        data['next_date'] = data['next_date'].shift(-1)
        data['next_date'] = data['next_date'] - timedelta(seconds=1)
        data['extrema_change'] = data['extrema'].ne(data['extrema'].shift().fillna(0)).cumsum()
        data.at[data.iloc[-1].name, 'next_date'] =  df.iloc[-1].name
        for index, row in data.iterrows():
            data.at[index, 'bars'] = len(df.loc[index:row['next_date']])
        data['bars'] = data['bars'].shift().fillna(0)
        return data


    @staticmethod
    def add_extrema_dataframe_to_data(data, extrema):
        for index, row in extrema.iterrows():
            start_date = index
            end_date = row['next_date']
            for column in ['extrema_high', 'extrema_low', 'extrema', 'extrema_change']:
                data.loc[start_date:end_date, column] = row[column]



    @staticmethod
    def range_dataframe_to_table(df_ranges, instrument, time_frame, column_to_rank=None, is_rank_ascending=False):
        if column_to_rank:
            if column_to_rank not in df_ranges:
                raise RuntimeError(f'OHLCAnalysis.range_dataframe_to_table() dataframe does not contain column_to_rank "{column_to_rank}"')

        fmt = Utils.floatfmt(instrument)
        headers = ['Time', 'Range']
        floatfmt = [None, fmt]
        if column_to_rank:
            headers.append('Rank')
        table = []
        ranks = df_ranges[column_to_rank].rank(method='dense', ascending=is_rank_ascending)
        for index, row in df_ranges.iterrows():
            data = [
                row.name,
                row['bar_range'],
            ]
            if column_to_rank:
                data.append(ranks[index])
            table.append(data)
        return headers, table, tuple(floatfmt)


    @staticmethod
    def generate_range_dataframe(df, time_frame):
        '''
        Returns a dataframe with ['bar_time', 'bar_range'] columns.
        bar_time is based on time_frame
        '''
        data = pd.DataFrame(index=df.index)
        data['bar_range'] = df['High'] - df['Low']

        if time_frame in ('M1', 'M5', 'M15', 'M30', 'H1', 'H4'):
            data['bar_time'] = data.index.strftime('%H')
        elif time_frame in ('D'):
            data['bar_time'] = data.index.strftime('%w')  # Sunday = 0
        elif time_frame in ('W'):
            data['bar_time'] = data.index.strftime('%U')  # Week starts on Sunday
        elif time_frame in ('M'):
            data['bar_time'] = data.index.strftime('%m')
        else:
            raise RuntimeError(f'OHLCAnalysis.generate_range_dataframe() unknown time_frame {time_frame}')

        hours = []
        means = []
        for index, block in data.groupby('bar_time'):
            hours.append(index)
            means.append(block.mean()['bar_range'])

        result = pd.DataFrame({
            'bar_time': np.array(hours, dtype=np.int32),
            'bar_range': np.array(means, dtype=np.float)
        })
        result['bar_rank'] = result['bar_range'].rank(method='dense', ascending=False)
        result.set_index('bar_time', inplace=True)
        return result