import pandas as pd
import pytz
import myutil as Utils


class EconomicNews(object):
    def __init__(self, json_data, use_timezone=None):
        '''
            json_data: Economic data
            use_timezone: None or pytz.timezone('America/New_York')
        '''
        self.df_news = pd.DataFrame(json_data)
        self.df_news['dateUtc'] = pd.to_datetime(self.df_news['dateUtc'], utc=True)
        if use_timezone:
            self.df_news['dateUtc'] = self.df_news['dateUtc'].tz_convert(pytz.timezone('America/New_York'))

    def get_event_name_by_id(self, eventId):
        df = self.df_news[(self.df_news['eventId'] == eventId)]
        if df.empty:
            return None
        return df['name'].value_counts()

    def get_event_id_by_name(self, indicator_name):
        df = self.df_news[(self.df_news['name'] == indicator_name)]
        if df.empty:
            return None
        return df['eventId'].value_counts()


    def get_actual_for_date(self, indicator_name, country_code, target_date):
        df = self.df_news
        data = df[(df['name'] == indicator_name) & (df['countryCode'] == country_code) & (df['dateUtc'] <= target_date)]
        if data.empty:
            return None
        return data.iloc[-1]['actual']

    def get_actual_for_date_range(self, indicator_name, country_code, start_date, end_date, use_column_name='Value'):
        df = self.df_news
        data =  df[(df['name'] == indicator_name) & (df['countryCode'] == country_code) & (df['dateUtc'] >= start_date) & (df['dateUtc'] <= end_date)][['dateUtc', 'actual']]
        data.rename(columns = {'dateUtc': 'Date', 'actual': use_column_name}, inplace = True)
        return Utils.preprocess_data(data)

    def add_indicator_to_dataframe(self, df, indicator_name, country_code, new_column_name='Value'):
        start_date = df.iloc[0].name
        end_date = df.iloc[-1].name
        df_actual = self.get_actual_for_date_range(indicator_name, country_code, start_date, end_date, new_column_name)
        df = pd.merge_asof(df, df_actual, left_index=True, right_index=True, direction='backward')
        df[new_column_name] = df[new_column_name].shift(-1)
        return df



'''
INPUT DATAFRAME #1
Rows of OHLC data, can be daily, hourly, or minute

            date    OHLC
2022-01-01 17:00    ...
2022-01-02 17:00    ...
2022-01-03 17:00    ...
2022-01-04 17:00    ...
2022-01-05 17:00    ...
2022-01-06 17:00    ...
2022-01-07 17:00    ...
2022-01-08 17:00    ...

INPUT DATAFRAME #2
Economic Data, will always be hourly
Shows when the value of an economic indicator like "GDP" or "Unemployment" changes.
            date    value
2022-01-01 09:00       1.0
2022-01-04 09:00       0.7
2022-01-06 17:00       2.5
2022-01-07 18:00       1.4

OUTPUT DATAFRAME
Desired output frame is OHLC data combined with values
            date    OHLC    value    
2022-01-01 17:00    ...     1.0
2022-01-02 17:00    ...     1.0
2022-01-03 17:00    ...     1.0
2022-01-04 17:00    ...     0.7
2022-01-05 17:00    ...     0.7
2022-01-06 17:00    ...     2.5
2022-01-07 17:00    ...     2.5   <-- bar closes hour before 2022-01-07 18:00 update to 1.4, so no change here
2022-01-08 17:00    ...     1.4





'''