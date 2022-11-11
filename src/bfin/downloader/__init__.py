import os
import pandas_datareader.data as reader
from pandas_datareader._utils import RemoteDataError
from datetime import datetime, timedelta

from bfin.oanda.api.config import Config, DEFAULT_PATH
from v20.errors import V20Timeout, V20ConnectionError
import json
import pandas as pd
import time
import pytz
import bfin.myutil as Utils
from bfin.directories import CACHE_DIR, QUANDL_KEY_PATH
from pathlib import Path

class Downloader(object):
    def __init__(self):
        self.oanda_config = Config()
        self.oanda_config.load(DEFAULT_PATH)
        self.oanda_api = self.oanda_config.create_context()
        self.quandl_key = None


    def load_quandl_key(self):
        try:
            path = Path('~/.quandlkey').expanduser()
        except (ValueError, TypeError) as e:
            print(f'Downloader() could not expand quandl path: {e}')
            return None

        if path.is_file():
            with open(path, 'r') as fp:
                key = fp.read()
            return key
        return None


    def download(self, instrument, timeframe, start_date, end_date=None, delay_between_requests=0.5):
        file_name = f'{instrument}-{timeframe}-{Utils.format_date(start_date)}-{Utils.format_date(end_date)}.csv'
        if Utils.check_for_cached_file(file_name):
            print(f'Loading from cache...   {Utils.format_date(start_date, hyphens=True)} to {Utils.format_date(end_date, hyphens=True)}')
            return Utils.load_from_cache(file_name)

        if len(instrument) == 7 and instrument[3] == '_' and  instrument.count('_') == 1:
            print(f'Downloading from Oanda...   {Utils.format_date(start_date, hyphens=True)} to {Utils.format_date(end_date, hyphens=True)}')
            data =  self.download_from_oanda(instrument, timeframe, start_date, end_date, delay_between_requests)
            Utils.write_to_cache(file_name, data)
            return data
        elif instrument.count('/') == 1:
            print(f'Downloading from QUANDL...   {Utils.format_date(start_date, hyphens=True)} to {Utils.format_date(end_date, hyphens=True)}')
            data = self.download_from_quandl(instrument, start_date, end_date)
            Utils.write_to_cache(file_name, data)
            return data
        else:
            if timeframe.upper() != "D":
                raise RuntimeError(f'Instrument {instrument} {timeframe} is downloaded from Yahoo which only supports daily data.')
            print(f'Downloading from Yahoo...   {Utils.format_date(start_date)} to {Utils.format_date(end_date)}')
            data = self.download_from_yahoo(instrument, start_date, end_date)
            Utils.write_to_cache(file_name, data)
            return data


    def download_from_quandl(self, instrument, start_date, end_date=None):
        str_start_date = start_date.strftime('%Y-%m-%d')
        if end_date:
            str_end_date = end_date.strftime('%Y-%m-%d')
        else:
            str_end_date = datetime.now().strftime('%Y-%m-%d')
        try:
            df = reader.DataReader(instrument, 'quandl', str_start_date, str_end_date, api_key=self.quandl_key)
        except RemoteDataError as e:
            raise RuntimeError(f'RemoteDataError downloading from QUANDL, {e}')
        return df


    def download_from_yahoo(self, instrument, start_date, end_date=None):
        str_start_date = start_date.strftime('%Y-%m-%d')
        if end_date:
            str_end_date = end_date.strftime('%Y-%m-%d')
        else:
            str_end_date = datetime.now().strftime('%Y-%m-%d')
        try:
            df = reader.DataReader(instrument, 'yahoo', str_start_date, str_end_date, api_key=self.quandl_key)
        except RemoteDataError as e:
            raise RuntimeError(f'RemoteDataError downloading from Yahoo, {e}')

        df.index = pd.to_datetime(df.index, utc=True)
        df.index = df.index.tz_convert(pytz.timezone('America/New_York'))
        return df

    def download_from_oanda(self, instrument, timeframe, start_date, end_date=None, delay_between_requests=0.5):
        def _load_dataframe(raw_body):
            """
                Passed the response.raw_body from a Oanda.v20 request. Convert the data to dataframe and return.
                No indicators are added at this stage.
            """
            df = pd.json_normalize(json.loads(raw_body), 'candles')
            df.rename(columns={'time': 'Date', 'volume': 'Volume', 'mid.o': 'Open', 'mid.h': 'High', 'mid.l': 'Low',
                               'mid.c': 'Close'}, inplace=True)
            try:
                df.set_index('Date', inplace=True)
            except KeyError:
                return None

            df.index = pd.to_datetime(df.index)
            df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(
                pd.to_numeric, errors='coerce')
            # df.drop(['complete'], axis=1, inplace=True)
            return df

        if not end_date:
            tzUTC = pytz.timezone('UTC')
            end_date = tzUTC.localize(datetime.now() + timedelta(days=1))

        kwargs = {
            'granularity': timeframe,
            'fromTime': self.oanda_api.datetime_to_str(start_date),
            'count': 5000,
            'includeFirst': True,
        }

        df_full = None
        downloaded_pages = 0

        while True:
            try:
                # print(f'Downloading with {kwargs}')
                response = self.oanda_api.instrument.candles(instrument, **kwargs)
            except V20Timeout as e:
                raise RuntimeError(f'Timed out [{e}]')
            except V20ConnectionError as e:
                raise RuntimeError(f'Connection error [{e}]')
            if response.status != 200:
                raise RuntimeError(f'Request failed with HTTP status code {response.status}, check instrument and date.')

            if (df_block := _load_dataframe(response.raw_body)) is None:
                if (downloaded_pages == 0):
                    raise RuntimeError(f'Error, no data returned')
                else:
                    # print('No data')
                    break
            if len(df_block) == 0:
                break
            downloaded_pages += 1
            last_date = df_block.iloc[-1].name
            if df_full is None:
                df_full = df_block
            else:
                df_full = pd.concat([df_full, df_block])

            if end_date:
                if last_date >= end_date:
                    return df_full[df_full.index <= end_date]

            kwargs['fromTime'] = self.oanda_api.datetime_to_str(last_date)
            kwargs['includeFirst'] = False
            time.sleep(delay_between_requests)
        df_full.index = df_full.index.tz_convert(pytz.timezone('America/New_York'))
        return df_full