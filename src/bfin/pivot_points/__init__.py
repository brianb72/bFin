import pandas as pd
from tabulate import tabulate
import pandas.tseries.offsets as of
from datetime import timedelta


def _calculate_pivots(df_daily, signals, prefix, limit_digits=4):
    pivots = []

    for idx, (name, block) in enumerate(signals.groupby('group')):
        # Record the current data
        price_high = df_daily.loc[block.index]['High'].max()
        price_low = df_daily.loc[block.index]['Low'].min()
        price_close = df_daily.loc[block.iloc[-1].name]['Close']
        pivots.append({
            'start': block.iloc[0].name,
            'end': block.iloc[-1].name,
            'bars': len(block),
            'high': price_high,
            'low': price_low,
            'close': price_close,
        })
        # Calculate pivot points for the current block using the previous block
        if idx > 0:
            prev = pivots[idx - 1]
            cur = pivots[idx]
            prev_high = prev['high']
            prev_low = prev['low']
            prev_close = prev['close']
            pivot = (prev_high + prev_low + prev_close) / 3
            cur[f'{prefix}S3'] = round(prev_low - (2 * (prev_high - pivot)), limit_digits)
            cur[f'{prefix}S2'] = round(pivot - (prev_high - prev_low), limit_digits)
            cur[f'{prefix}S1'] = round((pivot * 2) - prev_high, limit_digits)
            cur[f'{prefix}PP'] = round(pivot, limit_digits)
            cur[f'{prefix}R1'] = round((pivot * 2) - prev_low, limit_digits)
            cur[f'{prefix}R2'] = round(pivot + (prev_high - prev_low), limit_digits)
            cur[f'{prefix}R3'] = round(prev_high + (2 * (pivot - prev_low)), limit_digits)
    return pivots


def calculate_monthly_pivots_from_daily(df_daily):
    signals = pd.DataFrame(index=df_daily.index)
    signals['group'] = signals.index.strftime('%Y-%m')
    return _calculate_pivots(df_daily, signals, 'Mn')


def calculate_monthly_pivots_from_intraday(df_intraday):
    signals = pd.DataFrame(index=df_intraday.index)
    signals['group'] = (signals.index + of.Hour(7)).strftime('%Y-%m')
    return _calculate_pivots(df_intraday, signals, 'Mn')


def calculate_weekly_pivots_from_daily(df_daily):
    signals = pd.DataFrame(index=df_daily.index)
    signals['group'] = signals.index.strftime('%Y-%U')
    return _calculate_pivots(df_daily, signals, 'Wk')


def calculate_weekly_pivots_from_intraday(df_intraday):
    signals = pd.DataFrame(index=df_intraday.index)
    signals['group'] = (signals.index + of.Hour(7)).strftime('%Y-%U')
    return _calculate_pivots(df_intraday, signals, 'Wk')


def calculate_daily_pivots_from_daily(df_daily):
    signals = pd.DataFrame(index=df_daily.index)
    signals['group'] = signals.index.strftime('%Y-%m-%d')
    return _calculate_pivots(df_daily, signals, 'Day')


def calculate_daily_pivots_from_intraday(df_intraday):
    signals = pd.DataFrame(index=df_intraday.index)
    signals['group'] = (signals.index + of.Hour(7)).strftime('%Y-%m-%d')
    return _calculate_pivots(df_intraday, signals, 'Day')



def _adjust_pivots_end_date(pivots, last_date=None):
    '''
    Sets each pivots end date to one minute before the next bars start date, needed for mapping Daily -> Intraday
    '''
    for idx in range(len(pivots) - 1, 0, -1):
        pivots[idx - 1]['end'] = pivots[idx]['start'] - timedelta(minutes=1)
    if last_date:
        pivots[-1]['end'] = last_date


def add_interday_to_intraday(df_intra, pivots_inter, prefix):
    '''
    Add a daily, weekly, or monthly pivots to an hourly or minute chart.
    The _adjust_pivots_end_date should be called on pivots to ensure the intraday Friday data is filled in.
    '''
    columns = [f'{prefix}{x}' for x in ['S3', 'S2', 'S1', 'PP', 'R1', 'R2', 'R3']]
    if len(pivots_inter) == 0:
        return
    for pivot in pivots_inter[1:]:
        df_intra.loc[pivot['start']:pivot['end'], columns] = [pivot[x] for x in columns]


def pivots_list_to_string_table(table):
    headers = ('start', 'end', 'bars', 'S3', 'S2', 'S1', 'PP', 'R1', 'R2', 'R3')
    headers = {x: x for x in headers}
    return tabulate(table, headers=headers, floatfmt='.4f')
