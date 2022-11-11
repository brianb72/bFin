from bfin.oanda.api.config import Config, DEFAULT_PATH
from v20.errors import V20Timeout, V20ConnectionError
import pandas as pd
import json
import time


class OandaHistoricalError(Exception):
    """ Raise on error in OandaHistorical """


class OandaHistorical(object):
    def __init__(self):
        # TODO error checking on creation
        self.config = Config()
        self.config.load(DEFAULT_PATH)
        self.api = self.config.create_context()

    def _load_dataframe(self, raw_body):
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

    def get_data(self, currency_pair, granularity, bar_count, from_time=None, include_first=None):
        kwargs = {
            'granularity': granularity,
            'count': bar_count,
        }
        if from_time:
            kwargs['fromTime'] = self.api.datetime_to_str(from_time)

        if include_first:
            kwargs['includeFirst'] = include_first

        try:
            response = self.api.instrument.candles(currency_pair, **kwargs)
        except V20Timeout as e:
            raise OandaHistoricalError(f'Timed out [{e}]')
        except V20ConnectionError as e:
            raise OandaHistoricalError(f'Connection error [{e}]')
        if response.status != 200:
            raise OandaHistoricalError(f'Request failed with HTTP status code {response.status}, check instrument and date.')
        if (df_new := self._load_dataframe(response.raw_body)) is None:
            raise OandaHistoricalError(f'Error, no data returned')

        return df_new

    def get_range_data(self, currency_pair, granularity, from_time, to_time):
        """
            Download bars between date range. If range is more than 5000 bars, this call will fail with HTTP status 400.
        """
        kwargs = {
            'granularity': granularity,
            'fromTime': self.api.datetime_to_str(from_time),
            'toTime': self.api.datetime_to_str(to_time),
        }

        try:
            # print(f'Downloading with {kwargs}')
            response = self.api.instrument.candles(currency_pair, **kwargs)
        except V20Timeout as e:
            raise OandaHistoricalError(f'Timed out [{e}]')
        except V20ConnectionError as e:
            raise OandaHistoricalError(f'Connection error [{e}]')
        if response.status == 400:
            raise OandaHistoricalError(f'Request failed with HTTP status code {response.status} (note: range may not exceed 5000 bars)')
        if response.status != 200:
            raise OandaHistoricalError(f'Request failed with HTTP status code {response.status}')
        if (df := self._load_dataframe(response.raw_body)) is None:
            raise OandaHistoricalError(f'Error, no data returned')
        return df

    def get_bulk_data(self, currency_pair, granularity, start_time, end_time=None, delay_between_requests=0.5):
        """
            Loads bulk data from from_time to the current date.
        """
        kwargs = {
            'granularity': granularity,
            'fromTime': self.api.datetime_to_str(start_time),
            'count': 5000,
            'includeFirst': True,
        }

        df_full = None
        downloaded_pages = 0

        while True:
            try:
                # print(f'Downloading with {kwargs}')
                response = self.api.instrument.candles(currency_pair, **kwargs)
            except V20Timeout as e:
                raise OandaHistoricalError(f'Timed out [{e}]')
            except V20ConnectionError as e:
                raise OandaHistoricalError(f'Connection error [{e}]')
            if response.status != 200:
                raise OandaHistoricalError(f'Request failed with HTTP status code {response.status}, check instrument and date.')

            if (df_block := self._load_dataframe(response.raw_body)) is None:
                if (downloaded_pages == 0):
                    raise OandaHistoricalError(f'Error, no data returned')
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

            if end_time:
                if last_date >= end_time:
                    return df_full[df_full.index <= end_time], downloaded_pages

            kwargs['fromTime'] = self.api.datetime_to_str(last_date)
            kwargs['includeFirst'] = False
            time.sleep(delay_between_requests)

        return df_full, downloaded_pages



