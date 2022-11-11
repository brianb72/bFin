from bfin.backtest import Strategy
import pandas as pd
import pandas_ta as ta
import numpy as np

class StrategyMACross(Strategy):
    STRATEGY_NAME = "StrategyMACross"
    def __init__(self, instrument, df_ohlc, **kwargs):
        """
        kwargs:
            'periods': [(value, ...), ... ]
        """
        self.instrument = instrument
        self.df_ohlc = df_ohlc

        try:
            self.periods = kwargs['periods']
        except KeyError:
            raise RuntimeError('StrategyMACross() needs "periods" kwargs')

        if (start_date := kwargs.get('start_date')) is not None:
            self.df_ohlc = df_ohlc[df_ohlc.index >= start_date]
            self.start_date = start_date
        else:
            self.df_ohlc = df_ohlc
            self.start_date = None

        if len(self.periods) != 2:
            raise RuntimeError(f'StrategyMACross() needs 2 period(s), got {len(self.periods)}')
        if isinstance(self.periods, tuple):
            self.period_short = self.periods[0]
            self.period_long = self.periods[1]
        elif isinstance(self.periods, dict):
            try:
                self.period_short = self.periods['short']
            except KeyError:
                raise RuntimeError('StrategyMACross.__init__() - dict periods missing required value "short"')
            try:
                self.period_long = self.periods['long']
            except KeyError:
                raise RuntimeError('StrategyMACross.__init__() - dict periods missing required value "long"')
        else:
            raise RuntimeError(f'StrategyMACross.__init__() - Unknown periods, expecting tuple or dict {self.periods}')

    def generate_signals(self):
        # 1.0 holding a long, 0.0 no position flat, -1.0 holding a short
        signals = pd.DataFrame(index=self.df_ohlc.index)
        signals['signal'] = 0.0
        signals['signal_long'] = 0.0
        signals['signal_short'] = 0.0

        # Create the two moving averages which will be used to generate the signals
        signals['short_mavg'] = ta.sma(self.df_ohlc['Close'], self.period_short)
        signals['long_mavg'] = ta.sma(self.df_ohlc['Close'], self.period_long)

        # Create the signals when the MA's are in the correct position, a 1.0 means to buy and hold, and -1.0 means to sell and hold
        signals['signal_long'][self.period_short:] = np.where(
            signals['short_mavg'][self.period_short:] > signals['long_mavg'][self.period_short:], 1.0, 0.0)
        signals['signal_short'][self.period_short:] = np.where(
            signals['short_mavg'][self.period_short:] < signals['long_mavg'][self.period_short:], -1.0, 0.0)
        signals['signal'] = signals['signal_long'] + signals['signal_short']

        # Create the entry and exit orders on the transitions of signal, convinence variables for charting package showing entries and exits
        signals['entry_long'] = np.where((signals['signal_long'].shift(1) <= 0) & (signals['signal_long'] > 0), 1.0,
                                         0.0)
        signals['exit_long'] = np.where((signals['signal_long'].shift(1) > 0) & (signals['signal_long'] <= 0), 1.0, 0.0)
        signals['entry_short'] = np.where((signals['signal_short'].shift(1) >= 0) & (signals['signal_short'] < 0), 1.0,
                                          0.0)
        signals['exit_short'] = np.where((signals['signal_short'].shift(1) < 0) & (signals['signal_short'] >= 0), 1.0,
                                         0.0)

        # Create an autoincrementing column that increases every time signal changes, used like a "key" to identify individual trades
        signals['signal_change'] = signals['signal'].ne(signals['signal'].shift().fillna(0)).cumsum()

        # Add needed price information
        signals['close'] = self.df_ohlc['Close']
        signals['next_close'] = self.df_ohlc['Close'].shift(-1)

        # While holding how much profit per row
        return signals