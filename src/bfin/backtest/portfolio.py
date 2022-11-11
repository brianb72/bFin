'''

Signal dataframe columns:
    signal
    signal_long
    signal_short
    short_mavg
    long_mavg
    entry_long
    exit_long
    entry_short
    exit_short
    signal_change
    close
    next_close

'''

import numpy as np
import pandas as pd
import math
import bfin.myutil as Utils






class Portfolio(object):
    def __init__(self, instrument, df_ohlc, signals, **kwargs):
        '''
        kwargs:
        '''
        self.instrument = instrument
        self.df_ohlc = df_ohlc
        self.signals = signals
        self.kwargs = kwargs

        self.pip_size = kwargs.get('pip_size', Utils.pip_size(instrument))
        self.initial_equity = kwargs.get('initial_equity')
        self.b_top_off_equity = kwargs.get('b_top_off_equity', False)
        self.take_profit = kwargs.get('take_profit')
        self.stop_loss = kwargs.get('stop_loss')
        self.start_date = kwargs.get('start_date')
        if Utils.is_instrument_forex(instrument):
            self.home_currency = kwargs.get('home_currency', 'USD')
            self.margin_ratio = kwargs.get('margin_ratio', 1.0)
        else:
            self.home_currency = None
            self.margin_ratio = None
        self.positions = None


    @staticmethod
    def merge_equity_curve_and_positions(equity_curve, positions):
        return pd.merge(positions, equity_curve, left_on='date_closed', right_index=True)

    @staticmethod
    def positions_to_table(positions, instrument=None):
        fmt = Utils.floatfmt(instrument)
        headers = ['date_opened', 'date_closed', 'dir', 'signal', 'bars', 'opened', 'closed', 'profit', 'favor', 'adver']
        floatfmt = [None, None, '.0f', None, None, fmt, fmt, fmt, fmt, fmt]
        if 'equity' in positions:
            headers.append('equity')
            floatfmt.append('.2f')
        table = []
        for index, position in positions.iterrows():
            data = [position['date_opened'],
                position['date_closed'],
                position['direction'],
                position['close_type'],
                position['bars_held'],
                position['price_opened'],
                position['price_closed'],
                position['pips_profit'],
                position['pips_favorable'],
                position['pips_adverse']]
            try:
                data.append(position['equity'])
            except KeyError:
                pass
            table.append(data)
        return headers, table, tuple(floatfmt)


    def generate_equity_curve(self, positions, initial_equity, b_top_off_equity=False):
        if len(positions) == 0:
            return pd.DataFrame({'equity': []}, index=[]).rename_axis('Date')
        instrument = self.instrument
        home_currency = self.home_currency
        margin_ratio = self.margin_ratio
        added_equity = 0
        equity_curve = [initial_equity]
        equity_open_dates = [positions.iloc[0]['date_opened']]
        equity = initial_equity


        for index, position in positions.iterrows():
            if b_top_off_equity and equity < initial_equity:
                amount_to_add = initial_equity - equity
                equity += amount_to_add
                added_equity += amount_to_add
            direction = position['direction']
            date_closed = position['date_closed']
            price_opened = position['price_opened']
            price_closed = position['price_closed']
            units = Utils.trade_units_available(instrument, price_opened, equity, margin_ratio=margin_ratio, home_currency=home_currency)
            equity_result = Utils.trade_equity_result(instrument, direction, units, price_opened, price_closed, home_currency=home_currency)
            equity += equity_result
            equity_curve.append(equity)
            equity_open_dates.append(date_closed)

        return pd.DataFrame({'equity': equity_curve}, index=equity_open_dates).rename_axis('date')



    def generate_positions(self):
        positions = []
        for name, block in self.signals.groupby('signal_change'):
            if len(block) < 2:  # Ignore trades that last less than 2 bars
                continue
            direction = block['signal'].iloc[0]
            if direction == 0:
                continue

            # Get opening and close price
            price_opened = block.iloc[0]['close']
            price_closed = block.iloc[-1]['next_close']
            if np.isnan(price_opened) or np.isnan(price_closed):
                continue

            # Get max favorable / max adverse values
            price_max = self.df_ohlc.loc[block.index]['High'][1:].max()
            price_min = self.df_ohlc.loc[block.index]['Low'][1:].min()
            if direction == 1:
                pips_favorable = price_max - price_opened
                pips_adverse = price_opened - price_min
            elif direction == -1:
                pips_favorable = price_opened - price_min
                pips_adverse = price_max - price_opened
            else:
                raise RuntimeError(f'Portfolio.generate_positions() invalid direction {direction}, must be -1 or 1.')
            if np.isnan(pips_favorable) or np.isnan(pips_adverse):
                continue

            # Get trade time range and set close on signal as default
            date_opened = block.iloc[0].name
            date_closed = block.iloc[-1].name
            bars_held = len(block)
            close_type = 'Signal'

            # If there is a stop_loss or take_profit, adjust the trade to exit on the stop/take price.
            if self.stop_loss is not None or self.take_profit is not None:
                block_bars = self.df_ohlc.loc[block.index]
                price_stop_loss = None
                price_take_profit = None
                bars_stop = None
                bars_take = None
                # Calculate stop/take price level and bars for new trade
                if direction == 1.0:
                    if self.stop_loss is not None:
                        price_stop_loss = price_opened - (self.pip_size * self.stop_loss)
                        bars_stop = block_bars[block_bars['Low'] <= price_stop_loss]
                    if self.take_profit is not None:
                        price_take_profit = price_opened + (self.pip_size * self.stop_loss)
                        bars_take = block_bars[block_bars['High'] >= price_take_profit]
                elif direction == -1.0:
                    if self.stop_loss is not None:
                        price_stop_loss = price_opened + (self.pip_size * self.stop_loss)
                        bars_stop = block_bars[block_bars['High'] >= price_stop_loss]
                    if self.take_profit is not None:
                        price_take_profit = price_opened - (self.pip_size * self.stop_loss)
                        bars_take = block_bars[block_bars['Low'] <= price_take_profit]
                else:
                    raise RuntimeError(f'MACrossStrategy.generate_positions() unknown direction {direction}')

                # Adjust closed price and date, change signal to exit type
                if bars_stop is not None and len(bars_stop) > 0:
                    price_closed = price_stop_loss
                    date_closed = bars_stop.iloc[0].name
                    iloc_closed = block_bars.index.get_loc(date_closed)
                    bars_held = iloc_closed - 1
                    close_type = 'Stop'
                elif bars_take is not None and len(bars_take) > 0:
                    price_closed = price_take_profit
                    date_closed = bars_take.iloc[0].name
                    iloc_closed = block_bars.index.get_loc(date_closed)
                    bars_held = iloc_closed - 1
                    close_type = 'Take'

            pips_profit = (price_closed - price_opened) if direction == 1.0 else (price_opened - price_closed)


            positions.append({
                'date_opened': date_opened,
                'date_closed': date_closed,
                'price_opened': price_opened,
                'price_closed': price_closed,
                'pips_profit': pips_profit,
                'bars_held': bars_held,
                'direction': direction,
                'price_max': price_max,
                'price_min': price_min,
                'pips_favorable': pips_favorable,
                'pips_adverse': pips_adverse,
                'close_type': close_type,
            })

        df_positions = pd.DataFrame(positions)
        if self.initial_equity:
            equity_curve = self.generate_equity_curve(df_positions, self.initial_equity, self.b_top_off_equity)
            return self.merge_equity_curve_and_positions(equity_curve, df_positions)
        else:
            return df_positions


    def clamp_positions(self, positions):
        '''
        Find pips_profit values that are extreme outliners, and clamp them QHIGH
        '''
        Q1 = positions[positions['pips_profit'] >= 0]['pips_profit'].quantile(0.25)
        Q3 = positions[positions['pips_profit'] >= 0]['pips_profit'].quantile(0.75)
        IQR = Q3 - Q1
        QHIGH = Q3 + 1.5 * IQR
        for index, position in positions[positions['pips_profit'] > QHIGH].iterrows():
            positions.at[position.name, 'pips_profit'] = QHIGH
            positions.at[position.name, 'price_opened'] = positions.loc[position.name]['price_opened'] + QHIGH
            # TODO adjust bars_held and date_closed too?
        return positions


    def analyze_positions(self, positions):
        df_winners = positions[positions['pips_profit'] > 0]
        df_losers = positions[positions['pips_profit'] <= 0]


        num_of_trades = len(positions)
        profit_mean = positions['pips_profit'].mean()
        profit_total = positions['pips_profit'].sum()

        count_winners = len(df_winners)
        profit_winners_mean = df_winners['pips_profit'].mean()
        profit_losers_mean = df_losers['pips_profit'].mean()

        columns = ['trades', 'win', 'profit_total', 'profit_mean',  'winners_mean', 'losers_mean']
        data = [num_of_trades, count_winners, profit_total, profit_mean, profit_winners_mean, profit_losers_mean]

        if 'equity' in positions:
            final_equity = positions.iloc[-1]['equity']
            columns.append('equity')
            data.append(final_equity)

        df = pd.DataFrame(data=[data], columns=columns)
        return df

