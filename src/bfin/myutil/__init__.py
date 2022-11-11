import os
import math
import pandas.errors
import pytz
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse
from itertools import product
from pathlib import Path
from bfin.directories import CACHE_DIR
from bfin.oanda.historical import OandaHistorical, OandaHistoricalError
from datetime import timedelta


def is_instrument_forex(instrument):
    '''
    A forex instrument has the format XXX_YYY, is 7 characters long, and has a '_' at index 3
    '''
    if len(instrument) == 7 and instrument[3] == '_':
        return True
    return False


def parsedate(date_string, pytz_timezone=None):
    try:
        if pytz_timezone:
            return pytz_timezone.localize(parse(date_string))
        else:
            return parse(date_string)
    except (ValueError, TypeError) as e:
        raise RuntimeError(f'Could not parse date {date_string}')


def get_just_date_now(pytz_timezone=None):
    n = datetime.now()
    dt = datetime(n.year, n.month, n.day)
    if pytz_timezone:
        dt = pytz_timezone.localize(dt)
    return dt

def format_date(dt, hyphens=False):
    if hyphens:
        return f'{dt.year:04}-{dt.month:02}-{dt.day:02}'
    else:
        return f'{dt.year:04}{dt.month:02}{dt.day:02}'


# For files loaded from cache
def setup_dataframe_date_index(df):
    try:
        df.set_index('Date', inplace=True)
        df.index = pd.to_datetime(df.index, utc=True)
    except KeyError:
        pass
    df.index = df.index.tz_convert(pytz.timezone('America/New_York'))
    return df


def does_cache_dir_exist():
    return CACHE_DIR.is_dir()


def check_for_cached_file(file_name):
    use_file_name = remove_slashes_from_filename(file_name)
    return (CACHE_DIR / use_file_name).is_file()


def load_from_cache(file_name):
    use_file_name = remove_slashes_from_filename(file_name)
    if not does_cache_dir_exist():
        raise RuntimeError(f'Cache directory "{CACHE_DIR}" does not exist.')
    if not check_for_cached_file(use_file_name):
        raise RuntimeError(f'File {use_file_name} does not exist in cache.')
    try:
        df = pd.read_csv(CACHE_DIR / use_file_name)
    except pandas.errors.ParserError as e:
        raise RuntimeError(f'Error loading file {use_file_name}, {e}')
    return setup_dataframe_date_index(df)


def write_to_cache(file_name, df):
    use_file_name = remove_slashes_from_filename(file_name)
    if not does_cache_dir_exist():
        return
    df.to_csv(CACHE_DIR /  use_file_name)


def load_from_csv(full_file_path):
    if not os.path.exists(full_file_path):
        raise RuntimeError(f'File "{full_file_path}" does not exist.')
    df = pd.read_csv(full_file_path)
    return setup_dataframe_date_index(df)



def period_generator(*args, filtered=True):
    '''
    Generates all possible combinations of values from a list of period generators.
    The shortest period generator appears first in args, the longest appears last
    args        [(start, stop, step), ...]
    filtered    If true the yielded tuple must not have a shorter value that is equal or greater than a longer value
                example: (1, 2, 3, 4) valid   (1, 2, 2, 3)  invalid  (1, 3, 2, 4)  invalid
    yields      [(val_0_a, val_0_b, val_0_c), (val_1_a, val_1_b, val_1_c), ...]
    '''
    vals = []
    for arg in args:
        vals.append([i for i in range(*arg)])
    vals = product(*vals)
    for row in vals:
        if filtered and any([0 if row[i] < row[i+1] else 1 for i in range(len(row)-1)]):
            continue
        yield(row)

def stop_take_generator(trading_settings):
    stop_loss_gen = trading_settings.get('stop_loss_gen')
    take_profit_gen = trading_settings.get('take_profit_gen')
    if stop_loss_gen is None and take_profit_gen is None:
        raise RuntimeError(f'myutil.stop_take_generator() passed trading settings with no generators, one generator is required')
    if stop_loss_gen is None:
        stop_loss_gen = (0,1,1)
    if take_profit_gen is None:
        take_profit_gen = (0,1,1)
    for stop_loss in range(*stop_loss_gen):
        for take_profit in range(*take_profit_gen):
            d = {}
            if stop_loss != 0:
                d['stop_loss'] = stop_loss
            if take_profit != 0:
                d['take_profit'] = take_profit
            yield d





def get_ranges_from_dataframe(df_input, column_name, drop_first_zero=True, drop_nan=True, bar_length=False):
    '''
    Input Dataframe
        Date        Column_Name
        2022-01-01  1.5
        2022-01-02  1.5
        2022-01-03  1.5
        2022-01-04  0.7
        2022-01-05  2.3
        2022-01-06  2.3
    Output Dataframe
        Index   StartDateTime   EndDateTime     Value
        0       2022-01-01      2022-01-03      1.5
        1       2022-01-04      2022-01-04      0.7
        2       2022-01-05      2022-01-06      2.3
    if drop_first_zero is True, if the first row in Output has a Value of zero the first row is dropped
    '''
    ranges = []
    data = pd.DataFrame(index=df_input.index)
    data['change'] = df_input[column_name].ne(df_input[column_name].shift().fillna(0)).cumsum()
    for index, block in data.groupby(data['change']):
        start_date = block.iloc[0].name
        end_date = block.iloc[-1].name
        value = df_input.loc[start_date][column_name]
        if bar_length:
            bars = len(df_input.loc[start_date:end_date])
            ranges.append([start_date, end_date, value, bars])
        else:
            ranges.append([start_date, end_date, value])
    if bar_length:
        df_result = pd.DataFrame(columns=['StartDateTime', 'EndDateTime', 'Value', 'Bars'], data=ranges)
    else:
        df_result = pd.DataFrame(columns=['StartDateTime', 'EndDateTime', 'Value'], data=ranges)
    if drop_first_zero:
        if df_result.iloc[0]['Value'] == 0:
            df_result = df_result[1:]
    if drop_nan:
        df_result = df_result.dropna()
    return df_result


def add_ranges_to_dataframe(df_target, df_ranges, column_name):
    '''
    Input Dataframe
        Index   StartDateTime   EndDateTime     Value
        0       2022-01-01      2022-01-03      1.5
        1       2022-01-04      2022-01-04      0.7
        2       2022-01-05      2022-01-06      2.3
    Output Dataframe
        Date        Column_Name
        2022-01-01  1.5
        2022-01-02  1.5
        2022-01-03  1.5
        2022-01-04  0.7
        2022-01-05  2.3
        2022-01-06  2.3
    '''
    for index, row in df_ranges.iterrows():
        df_target.loc[row["StartDateTime"]:row["EndDateTime"], column_name] = row['Value']



def parse_start_and_end_dates(str_start_date, str_end_date):
    '''
    Convert the string dates to tzaware datetimes and return them
    '''
    if not str_start_date and str_end_date:
        raise RuntimeError('enddate used without startdate, provide a startdate.')

    tzEST = pytz.timezone('America/New_York')
    try:
        start_date = parsedate(str_start_date, pytz_timezone=tzEST)
    except RuntimeError as e:
        raise RuntimeError(f'Could not parse start date, {e}')

    if str_end_date:
        try:
            end_date = parsedate(str_end_date, pytz_timezone=tzEST)
        except RuntimeError as e:
            raise RuntimeError(f'Could not parse end date, {e}')
    else:
        end_date = tzEST.localize(datetime.now())

    return start_date, end_date


def remove_slashes_from_filename(file_name):
    '''
    Some instruments have forward and backslashes in their name, which will cause problems when using the instrument name as a filename.
    Change a forward or backward slash to an underscore.
    '''
    return file_name.replace('\\', '_').replace('/', '_')



def pip_size(instrument):
    if instrument is not None and len(instrument) == 7 and instrument[3] == '_':
        # Forex pairs
        if instrument == 'USD_JPY':
            return 0.01
        else:
            return 0.0001
    else:
        # Default
        return 0.01

def floatfmt(instrument):
    if instrument is not None and len(instrument) == 7 and instrument[3] == '_':
        # Forex pairs
        if instrument == 'USD_JPY':
            return '.2f'
        else:
            return '.4f'
    else:
        # Default
        return '.2f'

def forex_split_instrument_to_currencies(instrument):
    '''
    Splits an instrument 'EUR_USD' into ('EUR', 'USD')
    '''
    if not is_instrument_forex(instrument):
        raise RuntimeError(f'MyUtil.forex_split_instrument_to_currencies() passed non-forex instrument: {instrument}')
    currencies = instrument.split('_')
    if len(currencies) != 2:
        raise RuntimeError(f'MyUtil.forex_split_instrument_to_currencies() could not decode currencies: {instrument}')
    return currencies[0].upper(), currencies[1].upper()


def trade_units_available(instrument, price_current, free_equity, margin_ratio=None, home_currency=None):
    '''
    https://www.oanda.com/us-en/trading/how-calculate-profit-loss/
    https://www1.oanda.com/forex-trading/analysis/currency-units-calculator
    '''    
    if price_current == 0 or price_current is None or np.isnan(price_current):
        raise RuntimeError(f'MyUtil.trade_units_available() passed zero or or missing current_price')
    if free_equity <= 0 or free_equity is None or np.isnan(free_equity):
        return 0
    if is_instrument_forex(instrument):
        if margin_ratio is None or home_currency is None:
            raise RuntimeError(f'MyUtil.trade_units_available() passed a forex instrument {instrument} but margin_ratio or home_currency is missing.')
        first_currency, second_currency = forex_split_instrument_to_currencies(instrument)
        if home_currency.upper() == first_currency:
            return math.floor(free_equity * margin_ratio)
        elif home_currency.upper() == second_currency.upper():
            return math.floor(free_equity * margin_ratio / price_current)
        else:
            raise NotImplementedError(f'MyUtils.trade_units_available() need implementation for non-home currency trade')
    else:
            return math.floor(free_equity / price_current)


def trade_equity_result(instrument, direction, units, price_open, price_close, home_currency=None):
    value_at_open = price_open * units
    value_at_close = price_close * units
    if is_instrument_forex(instrument):
        if home_currency is None:
            raise RuntimeError(f'MyUtil.trade_equity_result() passed a forex instrument {instrument} but home_currency is missing.')
        first_currency, second_currency = forex_split_instrument_to_currencies(instrument)
        if home_currency.upper() == first_currency:
            return direction * (value_at_close - value_at_open) / price_close
        elif home_currency.upper() == second_currency:
            return direction * (value_at_close - value_at_open)
        else:
            raise NotImplementedError(f'MyUtils.trade_units_available() need implementation for non-home currency trade')
    else:
        return direction * (value_at_close - value_at_open)




def decode_generator_string(str_generator):
    '''
    Generator is a string in the format of "start, stop, step" or "(start, stop, step)"
    '''
    try:
        arr = str_generator.replace('(','').replace(')','').split(',')
        if len(arr) != 3:
            raise RuntimeError(f'MyUtil.decode_generator_string() expecting 3 arguments, got {len(arr)} in string {str_generator}')
        return (int(arr[0]), int(arr[1]), int(arr[2]))
    except (ValueError, TypeError) as e:
        raise RuntimeError(f'MyUtil.decode_generator_string() could not parse string {str_generator}, got error {e}')



def get_padded_date(start_date, time_frame, longest_period, pad_month=0, pad_week=0, pad_day=0):
    '''
    When downloading data, we need to download more data than specified so long indicators have enough of
    a lookback period to provide valid indicator data from start_date. Pad the date with either the
    longest period, or a number of months/weeks/days, whichever is longer.
    pad_month, pad_week, pad_day can be an integer value, or a True value which will be converted to 1 month/week/day.
    Returns a padded date that downloading should start from.
    '''
    extra = 1.25
    if longest_period:
        if time_frame == 'M1':
            frame_time = timedelta(minutes=longest_period * extra)
        elif time_frame == 'M5':
            frame_time = timedelta(minutes=5 * longest_period * extra)
        elif time_frame == 'M15':
            frame_time = timedelta(minutes=15 * longest_period * extra)
        elif time_frame == 'M30':
            frame_time = timedelta(minutes=30 * longest_period * extra)
        elif time_frame == 'H1':
            frame_time = timedelta(hours=longest_period * extra)
        elif time_frame == 'H4':
            frame_time = timedelta(hours=4 * longest_period * extra)
        elif time_frame == 'D':
            frame_time = timedelta(hours=24 * longest_period * extra)
        elif time_frame == 'W':
            frame_time = timedelta(weeks=longest_period * extra)
        elif time_frame == 'M':
            frame_time = timedelta(days=31 * longest_period * extra)
        else:
            raise RuntimeError(f'get_padded_date() unknown time_frame {time_frame}')
    else:
        frame_time = timedelta()

    if not isinstance(pad_month,int):
        use_month = 1 if pad_month else 0
    else:
        use_month = pad_month

    if not isinstance(pad_week,int):
        use_week = 1 if pad_week else 0
    else:
        use_week = pad_week

    if not isinstance(pad_day,int):
        use_day = 1 if pad_day else 0
    else:
        use_day = pad_day

    pad_time = max(timedelta(days=33 * use_month), timedelta(days=9 * use_week), timedelta(days=1.5 * use_day))
    
    return start_date - max(frame_time, pad_time)