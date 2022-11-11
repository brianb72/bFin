import os
import click
from bfin.oanda.api.config import Config as OandaConfig, ConfigValueError as OandaConfigValueError
from bfin.oanda.historical import OandaHistorical, OandaHistoricalError
import pandas as pd
import pandas_ta as ta
import bfin.myutil as Utils
from bfin.downloader import Downloader
from bfin.chart_printer import ChartPrinter
import bfin.pivot_points as Pivots
from bfin.ohlc_analysis import OHLCAnalysis
from bfin.directories import CACHE_DIR
from bfin.backtest.strategy_ma_cross import StrategyMACross
from bfin.backtest.portfolio import Portfolio
from tabulate import tabulate
from bfin.backtest.optimizer import Optimizer
from pathlib import Path
import sys

USE_MULTIPROCESSING = True

@click.group('bfin')
@click.pass_context
def run_bfin(ctx):
    """
    A financial charting and analysis package.
    """



@click.command('download')
@click.argument('instrument', required=True)
@click.argument('timeframe', required=True)
@click.argument('startdate', required=True)
@click.argument('enddate', required=False)
@click.option('--save', 'save_to_file', is_flag=True, required=False, help="Save to file with automatic name")
@click.option('--file', '--filename', 'file_name', type=str, required=False, help="Save to a file with specified name")
def command_download(instrument, timeframe, startdate, enddate, save_to_file, file_name):
    """
    Download data and display to screen, or save to file

    \b
    instrument      EUR_USD, ^GSPC, FRED/NROU
    timeframe       M1, M5, M15, M30, H1, H4, D, W, M
    startdate       YYYY-MM-DD
    enddate         YYYY-MM-DD
    """
    try:
        chart_start_date, chart_end_date = Utils.parse_start_and_end_dates(startdate, enddate)
    except RuntimeError as e:
        print(e)
        return

    if save_to_file:
        use_file_name = Path(Utils.remove_slashes_from_filename(f'{instrument}_{timeframe}_{chart_start_date.strftime("%Y-%m-%d")}_{chart_end_date.strftime("%Y-%m-%d")}.csv'))
    elif file_name:
        use_file_name = Path(file_name)
    else:
        use_file_name = None

    if use_file_name is not None and use_file_name.exists():
        confirm = input(f'"{use_file_name}" exists, overwrite? [y/N] ')
        confirm = confirm.lower()
        if confirm != 'y' and confirm != 'yes':
            print('...aborting.')
            return
        print('\n')

    downloader = Downloader()
    try:
        df = downloader.download(instrument, timeframe, chart_start_date, chart_end_date)
    except RuntimeError as e:
        print(f'Error downloading data, {e}')
        return

    if use_file_name is not None:
        print(f'Writing to "{use_file_name}"')
        df.to_csv(use_file_name)
    else:
        with pd.option_context('display.max_rows', None,'display.max_columns', None,'display.precision', 4):
            print(df.to_markdown())


@click.command('chart')
@click.argument('instrument', required=True)
@click.argument('timeframe', required=True)
@click.argument('startdate', required=True)
@click.argument('enddate', required=False)
@click.option('--output', required=True, type=str, help='Directory to write output')
@click.option('--bars', 'bars_per_chart', type=int, required=False, help="Bars per chart, if omitted break into sessions")
@click.option('--titles', is_flag=True, help="Add titles to output images")
@click.option('--extrema', 'extrema_period', type=int, required=False, help="Period for extrema")
@click.option('-noma', '--no-moving-averages', 'no_moving_averages', is_flag=True, required=False, default=False, help="Do not add moving average ribbon")
@click.option('-pd', '--pivot-day', 'pivot_day', is_flag=True, required=False, help="Add daily pivots")
@click.option('-pw', '--pivot-week', 'pivot_week', is_flag=True, required=False, help="Add weekly pivots")
@click.option('-pm', '--pivot-month', 'pivot_month', is_flag=True, required=False, help="Add monthly pivots")
@click.option('-pa', '--pivot-all', 'pivot_all', is_flag=True, required=False, help="Add all pivots")
@click.option('-s', '--sessions', 'sessions', is_flag=True, required=False, help="Show US session close")
@click.option('-sa', '--sessions-all', 'sessions_all', is_flag=True, required=False, help="Show all sessions open and close")
def command_chart(instrument, timeframe, startdate, enddate, output, bars_per_chart, titles, extrema_period, no_moving_averages, pivot_day, pivot_week, pivot_month, pivot_all, sessions, sessions_all):
    '''
        Create chart images for an instrument

        \b
        instrument      EUR_USD, ^GSPC, FRED/NROU
        timeframe       M1, M5, M15, M30, H1, H4, D, W, M
        startdate       YYYY-MM-DD
        enddate         YYYY-MM-DD
    '''

    try:
        output_path = Path(output)
    except (TypeError, ValueError) as e:
        print(f'Could not parse output path: {e}')
        return
    
    output_path = output_path.expanduser()

    if not output_path.is_dir():
        print(f'Output path must be an existing directory. [{output_path}]')

    if timeframe.upper() in ('D', 'W', 'M') and not bars_per_chart:
        print(f'For daily, weekly, or monthly charts the "--bars" option must be used.')
    try:
        chart_start_date, chart_end_date = Utils.parse_start_and_end_dates(startdate, enddate)
    except RuntimeError as e:
        print(e)
        return

    max_period = 200
    if extrema_period is not None and extrema_period > max_period:
        max_period = extrema_period

    data_start_date = Utils.get_padded_date(chart_start_date, timeframe, max_period, pivot_day, pivot_week, pivot_month)
    downloader = Downloader()
    try:
        df_ohlc = downloader.download(instrument, timeframe, data_start_date, chart_end_date)
    except RuntimeError as e:
        print(f'Error downloading data, {e}')
        return

    if not no_moving_averages:
        df_ohlc['ma_50'] = ta.sma(df_ohlc['Close'], 50)
        df_ohlc['ma_100'] = ta.sma(df_ohlc['Close'], 100)
        df_ohlc['ma_200'] = ta.sma(df_ohlc['Close'], 200)

    if pivot_all or pivot_day:
        pivots_daily = Pivots.calculate_daily_pivots_from_intraday(df_ohlc)
        Pivots.add_interday_to_intraday(df_ohlc, pivots_daily, 'Day')
    if pivot_all or pivot_week:
        pivots_weekly = Pivots.calculate_weekly_pivots_from_intraday(df_ohlc)
        Pivots.add_interday_to_intraday(df_ohlc, pivots_weekly, 'Wk')
    if pivot_all or pivot_month:
        pivots_monthly = Pivots.calculate_monthly_pivots_from_intraday(df_ohlc)
        Pivots.add_interday_to_intraday(df_ohlc, pivots_monthly, 'Mn')

    chart = ChartPrinter()

    kwargs = {
        'b_use_multiprocessing': USE_MULTIPROCESSING,
        'datetime_format': '%m-%d %H:%M',
        'ylim_adjust': 0,
        'start_date': chart_start_date,
    }

    if sessions_all:
        print('using sessions all')
        kwargs['session_lines'] = 'all'
    elif sessions:
        print('using session us')
        kwargs['session_lines'] = 'us'

    if extrema_period:
        kwargs['extrema'] =  OHLCAnalysis.generate_extrema_dataframe(df_ohlc, extrema_period)

    if bars_per_chart:
        kwargs['bars_per_chart'] = bars_per_chart

    if titles:
        kwargs['auto_titles'] = True

    try:
        chart.write_html(
            df_ohlc,
            output_path,
            instrument,
            timeframe,
            Utils.pip_size(instrument),
            **kwargs,
        )
    except RuntimeError as e:
        print(f'Error generating charts for {instrument} - [{e}]')
        return



@click.command('analysis')
@click.argument('instrument', required=True)
@click.argument('timeframe', required=True)
@click.argument('startdate', required=True)
@click.argument('enddate', required=False)
@click.option('--extrema', required=False, type=int)
@click.option('--ranges', 'ohlc_ranges', is_flag=True, help='Show OHLC ranges')
@click.option('--all', 'all_options', is_flag=True, help='Show all options with default values')
def command_analysis(instrument, timeframe, startdate, enddate, extrema, ohlc_ranges, all_options):
    '''
        Analyze an instrument and print a report

        \b
        instrument      EUR_USD, ^GSPC, FRED/NROU
        timeframe       M1, M5, M15, M30, H1, H4, D, W, M
        startdate       YYYY-MM-DD
        enddate         YYYY-MM-DD
    '''
    if all_options:
        extrema = 100
        ohlc_ranges = True

    if not extrema and not ohlc_ranges:
        print('\nSelect at least one type of analysis.\n')
        return

    try:
        chart_start_date, chart_end_date = Utils.parse_start_and_end_dates(startdate, enddate)
    except RuntimeError as e:
        print(e)
        return

    data_start_date = Utils.get_padded_date(chart_start_date, timeframe, extrema if extrema else 0)
    downloader = Downloader()
    try:
        df_ohlc = downloader.download(instrument, timeframe, data_start_date, chart_end_date)
    except RuntimeError as e:
        print(f'Error downloading data, {e}')
        return

    if ohlc_ranges:
        print('=' * 80)
        print('= Bar Ranges\n')
        df_ranges = OHLCAnalysis.generate_range_dataframe(df_ohlc, timeframe)
        headers, table, floatfmt = OHLCAnalysis.range_dataframe_to_table(df_ranges, instrument, timeframe, column_to_rank='bar_range')
        print(tabulate(table, headers=headers, floatfmt=floatfmt))
        print('\n\n')


    if extrema:
        print('=' * 80)
        print(f'= Extrema {extrema}\n')
        df_extrema = OHLCAnalysis.generate_extrema_dataframe(df_ohlc, extrema)
        headers, table, floatfmt = OHLCAnalysis.extrema_dataframe_to_table(instrument, timeframe, df_extrema, extrema)
        print(tabulate(table, headers=headers, floatfmt=floatfmt))
        print('\n\n')



@click.command('optimize')
@click.argument('instrument', required=True)
@click.argument('timeframe', required=True)
@click.argument('startdate', required=True)
@click.argument('enddate', required=False)
@click.option('--topoff_equity', is_flag=True, help='Top off equity if less than initial')
def command_optimize(instrument, timeframe, startdate, enddate, topoff_equity):
    '''
        Optimize a strategy against market data

        \b
        instrument      EUR_USD, ^GSPC, FRED/NROU
        timeframe       M1, M5, M15, M30, H1, H4, D, W, M
        startdate       YYYY-MM-DD
        enddate         YYYY-MM-DD
    '''
    try:
        chart_start_date, chart_end_date = Utils.parse_start_and_end_dates(startdate, enddate)
    except RuntimeError as e:
        print(e)
        return

    click.echo(f'Optimizing {instrument} {timeframe} {chart_start_date} {chart_end_date}\n')
    click.echo(f'Strategies')
    click.echo(f'   1. Moving Average Cross')
    strategy_number = click.prompt(f'\nEnter Strategy Number', type=int)
    if strategy_number == 1:
        click.echo(f'\nRequired parameters: generator_short, generator_long\n')
        click.echo(f'Generator format is "<start>, <stop>, <step>"')
        str_gen_short = click.prompt(f'Enter short period generator', type=str)
        gen_short = Utils.decode_generator_string(str_gen_short)
        str_gen_long = click.prompt(f'Enter long period generator', type=str)
        gen_long = Utils.decode_generator_string(str_gen_long)
        generators = dict(gen_short=gen_short, gen_long=gen_long)
        strategy = StrategyMACross
    else:
        print(f'Unknown strategy [{strategy_number}]')
        return

    kwargs = {
        'strategy': strategy,
        'use_multiprocessing': USE_MULTIPROCESSING,
        'start_date': chart_start_date,
        'pip_size': Utils.pip_size(instrument),
        'generators': {
            'periods': generators,
        },
        'portfolio': {
            'initial_equity': 10000,
            'margin_ratio': 25,
            'top_off_equity': False,
        }
    }

    data_start_date = Utils.get_padded_date(chart_start_date, timeframe, 200)
    print(f'\nDownload start: {data_start_date}, Chart start: {chart_start_date}')
    downloader = Downloader()
    try:
        df_ohlc = downloader.download(instrument, timeframe, data_start_date, chart_end_date)
    except RuntimeError as e:
        print(f'Error downloading data, {e}')
        return

    optimizer = Optimizer(df_ohlc, instrument, timeframe, **kwargs)
    counter, total_data = optimizer.optimize()

    print(f'\nOptimizer results for: {strategy.STRATEGY_NAME}')
    print(f'Initial equity: {kwargs["portfolio"]["initial_equity"]}')
    print(f'Generators: {generators}\n')

    analysis_list = [o['analysis'] for o in total_data.values()]
    df_analysis = pd.concat(analysis_list)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision', 4):
        sort_column = 'equity' if 'equity' in df_analysis else 'profit_total'
        print(df_analysis.sort_values(sort_column, ascending=False).head(15).fillna(0).to_markdown())



@click.command('backtest')
@click.argument('instrument', required=True)
@click.argument('timeframe', required=True)
@click.argument('startdate', required=True)
@click.argument('enddate', required=False)
@click.option('--topoff_equity', is_flag=True, help='Top off equity if less than initial')
@click.option('--output', type=str, help='Output directory to write the chart')
@click.option('--bars', 'bars_per_chart', type=int, help='Number of bars per chart on the output chart, if omitted split on sessions.')
def command_backtest(instrument, timeframe, startdate, enddate, topoff_equity, output, bars_per_chart):
    '''
        Backtest a strategy against market data

        \b
        instrument      EUR_USD, ^GSPC, FRED/NROU
        timeframe       M1, M5, M15, M30, H1, H4, D, W, M
        startdate       YYYY-MM-DD
        enddate         YYYY-MM-DD
    '''
    try:
        chart_start_date, chart_end_date = Utils.parse_start_and_end_dates(startdate, enddate)
    except RuntimeError as e:
        print(e)
        return

    if output is not None:
        try:
            output_path = Path(output).expanduser()
        except (TypeError, ValueError) as e:
            print(f'Could not parse output path: {e}')
            return
        if not output_path.is_dir():
            print(f'Output path must be an existing directory. [{output_path}]')
            return
    else:
        if bars_per_chart is not None:
            print(f'Cannot use --bars without --output')
            return

    click.echo(f'Backtesting {instrument} {timeframe} {chart_start_date} {chart_end_date}\n')
    click.echo(f'Strategies')
    click.echo(f'   1. Moving Average Cross')
    strategy_number = click.prompt(f'\nEnter Strategy Number', type=int)
    if strategy_number == 1:
        print(f'\nRequired parameters: period_short, period_long\n')
        period_short = click.prompt(f'Enter period_short', type=int)
        period_long = click.prompt(f'Enter period_long', type=int)
    else:
        print(f'Unknown strategy [{strategy_number}]')
        return
    print('\n')

    data_start_date = Utils.get_padded_date(chart_start_date, timeframe, 200)
    print(f'Download start: {data_start_date}, Chart start: {chart_start_date}')
    downloader = Downloader()
    try:
        df_ohlc = downloader.download(instrument, timeframe, data_start_date, chart_end_date)
    except RuntimeError as e:
        print(f'Error downloading data, {e}')
        return

    initial_equity = 10000

    print('Backtesting...\n')
    print(f'\nInitial Equity: {initial_equity}\n')

    if strategy_number == 1:
        kwargs = {'periods': (period_short, period_long), 'start_date': chart_start_date }
        strategy = StrategyMACross(instrument, df_ohlc, **kwargs)
        signals = strategy.generate_signals()
        kwargs = {
            'margin_ratio': 25.0,
            'b_top_off_equity': False,
            'start_date': chart_start_date,
            'b_use_multiprocessing': USE_MULTIPROCESSING,
            'datetime_format': '%m-%d %H:%M',
            'ylim_adjust': 0,            
        }

        portfolio = Portfolio(instrument, df_ohlc, signals, **kwargs)
        positions = portfolio.generate_positions()
        equity_curve = portfolio.generate_equity_curve(positions, initial_equity, b_top_off_equity=topoff_equity)
        positions_equity = Portfolio.merge_equity_curve_and_positions(equity_curve, positions)
        headers, table, floatfmt = Portfolio.positions_to_table(positions_equity, instrument)
        print(tabulate(table, headers=headers, floatfmt=floatfmt))
        total_pips = positions['pips_profit'].sum()
        print(f'\nTotal pips profit: {total_pips}\n')
        winners = len(positions[positions['pips_profit'] > 0])
        losers = len(positions) - winners
        print(f'Trades: {len(positions)}, winners {winners}, losers {losers}')

        df_ohlc['ma_100'] = ta.sma(df_ohlc['Close'], period_short)
        df_ohlc['ma_200'] = ta.sma(df_ohlc['Close'], period_long)

        if output:
            kwargs['signals'] = {
                'df_signals': signals,
                'marker_entries': True,
                'marker_exits': True,
            }
            if bars_per_chart:
                kwargs['bars_per_chart'] = bars_per_chart
            try:
                chart = ChartPrinter()
                chart.write_html(
                    df_ohlc,
                    output_path,
                    instrument,
                    timeframe,
                    Utils.pip_size(instrument),
                    **kwargs,
                )
            except RuntimeError as e:
                print(f'Error generating charts for {instrument} - [{e}]')
                return


run_bfin.add_command(command_chart)
run_bfin.add_command(command_download)
run_bfin.add_command(command_backtest)
run_bfin.add_command(command_optimize)
run_bfin.add_command(command_analysis)


def main():
    '''
    Package entry point
    '''
    # Load Oandas config
    v20_path = os.path.expanduser('~/.v20.conf')

    if not os.path.exists(v20_path):
        print(f'\nA valid configuration file was not found at "{v20_path}"')
        print(f'An account and access token from Oanda\'s REST-V20 API are required. (https://developer.oanda.com/)\n')
        response = input('Would you like to create a configuration file? (y/n)> ')
        response = response.lower()
        if response == 'y' or response == 'yes':
            print('\n')
            oanda = OandaConfig()
            oanda.update_from_input()
            try:
                oanda.validate()
            except OandaConfigValueError as e:
                print(f'Oanda Configuration Error: {e}')
                sys.exit()
            oanda.dump()
            print('Configuration file written.\n\n')
        else:
            print('\nA valid configuration file is required, exiting.')
            sys.exit()


 
    try:
        run_bfin(prog_name='bfin')
    except RuntimeError as e:
        print(f'\n[Error] {e}\n')

