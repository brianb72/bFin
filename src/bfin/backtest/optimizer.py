import pandas as pd
from datetime import timedelta
import bfin.myutil as Utils
from collections import Counter
import multiprocessing
from datetime import datetime
from tabulate import tabulate
import math
from bfin.backtest.portfolio import Portfolio

MOST_COMMON_COUNT = 10

class Optimizer(object):
    def __init__(self, df_ohlc, instrument, timeframe, **kwargs):
        '''
        kwargs:
            strategy                class to use as strategy
            use_multiprocessing     bool use multiprocessor pool if true
            start_date              datetime to start trading
            pip_size                pip size of instrument
            generators
                periods   [{'name': (start, stop, step)}, ...]  List of periods to use as generators, ordered shortest to longest
                trading     {'stop_loss': (start, stop, step), 'take_profit': (start, stop, step)}
            portfolio
                initial_equity      int     Initial deposit in dollars
                margin_ratio        int     Multiple such as 25
                top_off_equity      bool    If equity <= initial_equity after a trade, deposit more equity to bring balance up to initial_equity
        '''
        self.df_ohlc = df_ohlc
        self.instrument = instrument
        self.timeframe = timeframe
        self.kwargs = kwargs

        self.pip_size = kwargs.get('pip_size', Utils.pip_size(instrument))
        self.use_multiprocessing = kwargs.get('use_multiprocessing', True)

        try:
            self.strategy = kwargs['strategy']
        except KeyError:
            raise RuntimeError('WindowOptimizer() needs "strategy" kwargs')


        self.start_date = kwargs.get('start_date')
        if self.start_date is None:
            raise RuntimeError('WindowOptimizer() needs "start_date" kwargs')

        self.portfolio_settings = kwargs.get('portfolio')

        try:
            self.generators = kwargs['generators']
        except KeyError:
            raise RuntimeError('WindowOptimizer() needs "generators" kwargs')

        try:
            self.periods = self.generators['periods']
        except KeyError:
            raise RuntimeError('WindowOptimizer() needs "generator.periods" kwargs')

        self.trading = self.generators.get('trading')


    def optimize(self):
        counter = Counter()
        total_data = {}
        if self.use_multiprocessing:
            pool = multiprocessing.Pool()
        else:
            pool = None

        # Perform the work
        work_list = []
        data_slice = self.df_ohlc[self.df_ohlc.index >= self.start_date]
        for periods in Utils.period_generator(*self.periods.values()):
            work_list.append([
                self.instrument,
                data_slice,
                self.strategy,
                periods,
                self.portfolio_settings,
            ])

        if self.use_multiprocessing:
                finished_work = pool.map(process_periods, work_list)
        else:
            finished_work = []
            for work in work_list:
                finished_work.append(process_periods(work))

        # Add the finished work to the counter and total data
        for work in finished_work:
            periods = work['periods']
            portfolio = work['portfolio']
            positions = work['positions']
            analysis = work['analysis']
            key = periods
            analysis.index = [str(key)]
            analysis.index.names = ['settings']
            if 'equity' in positions:
                counter[key] = positions.iloc[-1].equity
            else:
                counter[key] = positions['pips_profit'].sum()
            total_data[key] = {
                'portfolio': portfolio,
                'positions': positions,
                'analysis': analysis,
            }

        # Return the results
        return counter, total_data


def process_periods(args):
    instrument, df, use_strategy, periods, portfolio_settings = args
    kwargs = { 'periods': periods }
    strategy = use_strategy(instrument, df, **kwargs)
    signals = strategy.generate_signals()
    if portfolio_settings:
        portfolio = Portfolio(instrument, df, signals, **portfolio_settings)
    else:
        portfolio = Portfolio(instrument, df, signals)
    positions = portfolio.generate_positions()
    return {
        'periods': periods,
        'portfolio': portfolio,
        'positions': positions,
        'analysis': portfolio.analyze_positions(positions),
    }