import numpy as np
import pandas as pd
import pandas_ta as ta
import pytz
from functools import reduce
import mplfinance as mpf
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt
from datetime import timedelta
import os
from multiprocessing import Pool, TimeoutError
from pathlib import Path
import bfin.pivot_points as PivotPoints
import bfin.myutil as Utils
import math
from bfin.ohlc_analysis import OHLCAnalysis
from datetime import timedelta
from matplotlib.lines import Line2D
from matplotlib.dates import date2num, num2date
from matplotlib import pyplot


class ChartPrinter(object):
    PIVOT_COLORS = dict(
        Mn=dict(R3='Tomato', R2='Tomato', R1='Tomato', PP='Navy', S1='RoyalBlue', S2='RoyalBlue', S3='RoyalBlue'),
        Wk=dict(R3='SaddleBrown', R2='SaddleBrown', R1='SaddleBrown', PP='DarkSlateGray', S1='SaddleBrown', S2='SaddleBrown', S3='SaddleBrown'),
        Day=dict(R3='SaddleBrown', R2='SaddleBrown', R1='SaddleBrown', PP='Black', S1='SaddleBrown', S2='SaddleBrown', S3='SaddleBrown'),
    )

    def __init__(self):
        pass

    def _break_into_chunks_by_ilocs(self, df, bars_per_chart=300):
        chunks = max(1, math.floor(len(df) / bars_per_chart))
        return np.array_split([i for i in range(len(df))], chunks)


    def _break_into_sessions_by_ilocs(self, df):
        session_changes = df[(df.index.hour >= 17) & (df.iloc[df.index.get_indexer(df.index) - 1].index.hour < 17)].index
        last_session = session_changes[0]
        session_iloc_chunks = []
        for cur_session in session_changes[1:]:
            session_iloc_chunks.append((
                df.index.get_loc(last_session),
                df.index.get_loc(cur_session)
            ))
            last_session = cur_session
        session_iloc_chunks.append((
            session_iloc_chunks[-1][1],
            len(df)-1
        ))
        session_iloc_chunks.pop(0)
        return session_iloc_chunks


    def add_session_lines(self, data, time_frame, session_lines, vlines):
        '''
        session_lines: None, 'us', 'all'
        '''
        if session_lines == 'all':
            for us_opens in data[(data.index.hour == 8) & (data.index.minute == 0)].index:
                vlines['vlines'].append(us_opens)
                vlines['colors'].append('blue')
            for us_closes in data[(data.index.hour == 17) & (data.index.minute == 0)].index:
                vlines['vlines'].append(us_closes)
                vlines['colors'].append('black')
            for eu_opens in data[(data.index.hour == 3) & (data.index.minute == 0)].index:
                vlines['vlines'].append(eu_opens)
                vlines['colors'].append('red')
            for eu_closes in data[(data.index.hour == 12) & (data.index.minute == 0)].index:
                vlines['vlines'].append(eu_closes)
                vlines['colors'].append('brown')
            # Tokyo 7:00pm to 4:00am,  Sydney 5:00pm to 2:00am
            for jp_opens in data[(data.index.hour == 18) & (data.index.minute == 0)].index:
                vlines['vlines'].append(jp_opens)
                vlines['colors'].append('orange')
        elif session_lines == 'us':
            for us_closes in data[(data.index.hour == 17) & (data.index.minute == 0)].index:
                vlines['vlines'].append(us_closes)
                vlines['colors'].append('black')


    def add_bar_highlight_lines(self, data, highlights, vlines):
        selectors = []
        if (bar_hour := highlights.get('hour')) is not None:
            selectors.append((data.index.hour == bar_hour))
        if (bar_minute := highlights.get('minute')) is not None:
            selectors.append((data.index.minute == bar_minute))
        selector = reduce(lambda x, y: x & y, selectors)
        for bar in data[selector].index:
            vlines['vlines'].append(bar)
            vlines['colors'].append('yellow')


    def add_pivot_lines_continuous(self, df, pip_size, ylims, alines, labels):
        line_widths = dict(Mn=3, Wk=2, Day=0.75)
        for prefix in ['Mn', 'Wk', 'Day']:
            for level in ['R3', 'R2', 'R1', 'PP', 'S1', 'S2', 'S3']:
                pivot_name = f'{prefix}{level}'
                if pivot_name not in df:
                    continue
                df_lines = Utils.get_ranges_from_dataframe(df, pivot_name, drop_first_zero=True, drop_nan=True)
                color = self.PIVOT_COLORS[prefix][level]
                for index, row in df_lines.iterrows():
                    value = row['Value']
                    line = [(row['StartDateTime'], value), (row['EndDateTime'], value)]
                    alines['alines'].append(line)
                    alines['colors'].append(color)
                    extra_width = 0.5 if level == 'PP' else 0
                    alines['linewidths'].append(line_widths[prefix] + extra_width)
                    if prefix == 'Mn':
                        labels.append(dict(
                            kwargs=dict(horizontalalignment='center', color='#000000', fontsize='medium',
                                        fontweight='bold'),
                            x=df.index.get_loc(row['StartDateTime']),
                            y=value,
                            text=f'{prefix}{level}',
                        ))
                    elif prefix == 'Wk':
                        labels.append(dict(
                            kwargs=dict(horizontalalignment='center', color='#000000', fontsize='small',
                                        fontweight='bold'),
                            x=df.index.get_loc(row['StartDateTime']),
                            y=value,
                            text=f'{prefix}{level}',
                        ))


    def add_pivot_lines_sessions(self, df, pip_size, ylims, alines, hlines, labels):
        line_widths = dict(Mn=3, Wk=2.5, Day=0.75)
        for prefix in ['Mn', 'Wk', 'Day']:
            for level in ['R3', 'R2', 'R1', 'PP', 'S1', 'S2', 'S3']:
                pivot_name = f'{prefix}{level}'
                if pivot_name not in df:
                    continue
                df_lines = Utils.get_ranges_from_dataframe(df, pivot_name, drop_first_zero=True, drop_nan=True)
                color = self.PIVOT_COLORS[prefix][level]
                for index, row in df_lines.iterrows():
                    value = row['Value']
                    extra_width = 0.5 if level == 'PP' else 0
                    if prefix == 'Mn':
                        hlines['hlines'].append(value)
                        hlines['colors'].append(color)
                        hlines['linewidths'].append(line_widths[prefix] + extra_width)
                        labels.append(dict(
                            kwargs=dict(horizontalalignment='center', color='#000000', fontsize='large', fontweight='heavy'),
                            x=0,
                            y=value,
                            text=f'{prefix}{level}',
                        ))
                    else:
                        line = [(row['StartDateTime'], value), (row['EndDateTime'], value)]
                        alines['alines'].append(line)
                        alines['colors'].append(color)
                        alines['linewidths'].append(line_widths[prefix] + extra_width)
                        if prefix == 'Day':
                            if level == 'R2' and ylims[1] < value:
                                ylims[1] = value + (pip_size * 3)
                            elif level == 'S2' and ylims[0] > value:
                                ylims[0] = value - (pip_size * 3)
                        if prefix == 'Wk':
                            labels.append(dict(
                                kwargs=dict(horizontalalignment='center', color='#000000', fontsize='medium', fontweight='bold'),
                                x=0,
                                y=value,
                                text=f'{prefix}{level}',
                            ))
                        elif prefix == 'Day':
                            labels.append(dict(
                                kwargs=dict(horizontalalignment='center', color='#000000', fontsize='small', fontweight='bold'),
                                x=len(df) - 1,
                                y=value,
                                text=f'{prefix}{level}',
                            ))


    def add_extrema_alines(self, data, ylims, alines, pip_size):
        if 'extrema_change' not in data:
            return
        for index, block in data.groupby('extrema_change'):
            value = block.iloc[0]['extrema']
            first_date = block.iloc[0].name
            last_date = block.iloc[-1].name
            if not np.isnan(block.iloc[0]['extrema_high']):
                color = 'blue'
            elif not np.isnan(block.iloc[0]['extrema_low']):
                color = 'red'
            else:
                raise RuntimeError(f'ChartPrinter() extrema high and low are both null.')
            alines['alines'].append([(first_date, value), (last_date, value)])
            alines['colors'].append(color)
            alines['linewidths'].append(1)
            if value <= ylims[0]:
                ylims[0] = value - pip_size * 5
            elif value >= ylims[1]:
                ylims[1] = value + pip_size * 5


    def add_extrema_labels(self, ax, data, floatfmt):
        if 'extrema_change' not in data:
            return
        pixel_adjust = 20
        transform = ax.transData.inverted()
        text_pad = transform.transform((0, pixel_adjust))[1] - transform.transform((0, 0))[1]
        kwargs = dict(horizontalalignment='center', color='#000000', fontsize='small', fontweight='bold')

        for _, block in data.groupby('extrema_change'):
            value = block.iloc[0]['extrema']
            index =  data.index.get_indexer([block.iloc[0].name])
            try:
                index = index[0]
            except (IndexError, TypeError):
                raise RuntimeError(f'ChartPrinter() get_indexer error in add_extrema_labels()')
            if not np.isnan(block.iloc[0]['extrema_high']):
                ax.text(index, value + text_pad, f'{value:{floatfmt}}', verticalalignment='top', **kwargs)
            elif not np.isnan(block.iloc[0]['extrema_low']):
                ax.text(index, value - text_pad, f'{value:{floatfmt}}', verticalalignment='bottom', **kwargs)


    def _save_chart_image(self, data, image_full_path, instrument, time_frame, pip_size, config_dict):
        figscale = config_dict.get('figscale', 1.0)
        figratio = config_dict.get('figratio', (5,2))
        ap = []
        labels = []
        alines = dict(alines=[], colors=[], alpha=1, linewidths=[])
        hlines = dict(hlines=[], colors=[], alpha=1, linewidths=[], linestyle='-.')
        vlines = dict(vlines=[], colors=[], alpha=1, linewidths=[])
        extra_params = {}

        if auto_titles := config_dict.get('auto_titles', False):
            title = f'{instrument} {time_frame} {Utils.format_date(data.iloc[0].name, hyphens=True)} to {Utils.format_date(data.iloc[-1].name, hyphens=True)}'
        else:
            title = ''


        # Process overlays
        if (overlays := config_dict.get('overlays')) is not None:
            for overlay in overlays:
                if (column_name := overlay.get('column_name')) is None:
                    raise RuntimeError(f'ChartPrinter()._save_chart_image() an overlay is missing a column_name [{overlay}]')
                if column_name not in data:
                    raise RuntimeError(f'ChartPrinter()._save_chart_image() an overlay has a column_name that does not exist in data [{overlay}]')
                color = overlay.get('color', 'black')
                width = overlay.get('width', 1)
                overlay_type = overlay.get('overlay_type', 'line')
                if overlay_type not in ('line', 'scatter'):
                    raise RuntimeError(f'ChartPrinter()._save_chart_image() an overlay has an unknwown type [{overlay}]')
                ap.append(mpf.make_addplot(
                    data[column_name], type=overlay_type, color=color, width=width, secondary_y=False, panel=0
                ))

        # Process indicators
        if (indicators := config_dict.get('indicators')) is not None:
            panel_count = 0
            for indicator in indicators:
                panel_count += 1

                for line in indicator.get('lines', []):
                    if (column_name := line.get('column_name')) is None:
                        raise RuntimeError(f'ChartPrinter()._save_chart_image() an indicator line is missing a column_name [{line}]')
                    if column_name not in data:
                        raise RuntimeError(f'ChartPrinter()._save_chart_image() an indicator line has a column_name that does not exist in data [{line}]')
                    color = line.get('color', 'black')
                    width = line.get('width', 1)
                    line_type = line.get('type', 'line')
                    line_style = line.get('style', 'solid')
                    if line_type not in ('line', 'scatter', 'bar'):
                        raise RuntimeError(f'ChartPrinter()._save_chart_image() an overlay has an unknwown type [{line}]')
                    if line_type == 'line':
                        ap.append(mpf.make_addplot(
                            data[column_name], type=line_type, linestyle=line_style, color=color, width=width, secondary_y=False, panel=panel_count
                        ))
                    else:
                        ap.append(mpf.make_addplot(
                            data[column_name], type=line_type, color=color, width=width, secondary_y=False, panel=panel_count
                        ))


                for level in indicator.get('levels', []):
                    if (value := level.get('value')) is None:
                        raise RuntimeError(f'ChartPrinter()._save_chart_image() a level line is missing a value [{level}]')
                    color = level.get('color', 'black')
                    width = level.get('width', 1)
                    line_style = level.get('style', 'solid')
                    df_value = pd.DataFrame(index=data.index)
                    df_value['Value'] = value
                    ap.append(mpf.make_addplot(
                        df_value['Value'], type='line', linestyle=line_style, color=color, width=width, secondary_y=False, panel=panel_count
                    ))


            extra_params['num_panels'] = panel_count + 1
            extra_params['panel_ratios'] = tuple([4] + [2 for i in range(panel_count)])

        if (signals_config := config_dict.get('signals')) is not None:
            if (df_signals := signals_config.get('df_signals')) is None:
                raise RuntimeError(f'ChartPrinter()._save_chart_image() config_dict contains signals group but no dataframe.')
            start_date = data.iloc[0].name
            end_date = data.iloc[-1].name
            df_markers = df_signals[(df_signals.index >= start_date) & (df_signals.index <= end_date)]
            marker_size = 100
            if signals_config.get('marker_entries', False):
                ap.append(mpf.make_addplot((df_markers['entry_long'] * df_markers['close']).replace(0.0, np.NaN), type='scatter', color='blue', marker='^', markersize=marker_size, secondary_y=False))
                ap.append(mpf.make_addplot((df_markers['entry_short'] * df_markers['close']).replace(0.0, np.NaN), type='scatter', color='red', marker='v', markersize=marker_size, secondary_y=False))
            if signals_config.get('marker_exits', False):
                ap.append(mpf.make_addplot((df_markers['exit_long'] * df_markers['close']).replace(0.0, np.NaN), type='scatter', color='blue', marker='v', markersize=marker_size, secondary_y=False))
                ap.append(mpf.make_addplot((df_markers['exit_short'] * df_markers['close']).replace(0.0, np.NaN), type='scatter', color='red', marker='^', markersize=marker_size, secondary_y=False))


        ylim_adjust = config_dict.get('ylim_adjust', 0)
        ylims = [data['Low'].min() - ylim_adjust, data['High'].max() + ylim_adjust]

        session_lines = config_dict.get('session_lines')
        self.add_session_lines(data, time_frame, session_lines, vlines)

        self.add_extrema_alines(data, ylims, alines, pip_size)

        if highlights := config_dict.get('highlights'):
            self.add_bar_highlight_lines(data, highlights, vlines)

        if 'bars_per_chart' in config_dict:
            self.add_pivot_lines_continuous(data, pip_size, ylims, alines, labels)
        else:
            self.add_pivot_lines_sessions(data, pip_size, ylims, alines, hlines, labels)

        if (datetime_format := config_dict.get('datetime_format')) is None:
            if time_frame in ['M1', 'M5', 'M15', 'M30']:
                datetime_format = '%H:%m'
            elif time_frame in ['H1', 'H4']:
                datetime_format = '%Y-%m-%d'
            elif time_frame in ['W', 'M']:
                datetime_format = '%Y-%m'

        if 'ma_50' in data:
            ap.append(mpf.make_addplot(data[f'ma_50'], color='green', width=1, secondary_y=False))
        if 'ma_100' in data:
            ap.append(mpf.make_addplot(data[f'ma_100'], color='blue', width=1, secondary_y=False))
        if 'ma_200' in data:
            ap.append(mpf.make_addplot(data[f'ma_200'], color='red', width=2, secondary_y=False))

        mc = mpf.make_marketcolors(up='w', down='k')

        chart_style = mpf.make_mpf_style(
            base_mpf_style='charles',
            # marketcolors=mc,
            rc={'axes.edgecolor': 'black'}
        )

        fig, ax = mpf.plot(data, type='candle', style=chart_style, ylabel='', ylim=ylims, title=title,
                figratio=figratio, figscale=figscale, datetime_format=datetime_format, addplot=ap,
                alines=alines, hlines=hlines, vlines=vlines,
                tight_layout=True, xrotation=0, warn_too_much_data=100000, returnfig=True,
                **extra_params,
        )



        if indicators is not None:
            for index, indicator in enumerate(indicators):
                if (title := indicator.get('title')) is not None:
                    title_location = indicator.get('title_location', 'right')
                    ax[2 * (index + 1)].set_title(title, y=1.0, pad=-14, loc=title_location)


        self.add_extrema_labels(ax[0], data, Utils.floatfmt(instrument))
        if len(labels) > 0:
            transform = ax[0].transData.inverted()
            text_pad = transform.transform((0, 20))[1] - transform.transform((0, 0))[1]
            for label in labels:
                if ylims[0] <= label['y'] <= ylims[1]:
                    ax[0].text(label['x'], label['y'] + text_pad, label['text'], verticalalignment='top', **label['kwargs'])

        if image_full_path:
            fig.savefig(image_full_path, bbox_inches='tight')
        else:
            plt.show()
        plt.close(fig)



    def show_in_notebook(self, df, instrument, time_frame, pip_size, **kwargs):
        extrema = kwargs.get('extrema')
        if extrema is not None:
            OHLCAnalysis.add_extrema_dataframe_to_data(df, extrema)

        if (chart_start_date := kwargs.get('start_date')) is None:
            chart_start_date = df.iloc[0].name
        data = df[df.index >= chart_start_date]

        if bars_per_chart := kwargs.get('bars_per_chart'):
            if not isinstance(bars_per_chart, int) or bars_per_chart <= 0:
                raise RuntimeError(f'ChartOHLCPrinter() kwarg "bars_per_chart" must be a positive integer.')
            session_ilocs = self._break_into_chunks_by_ilocs(data, bars_per_chart=bars_per_chart)
        else:
            session_ilocs = self._break_into_sessions_by_ilocs(data)

        for sc in range(len(session_ilocs)):
            SESSION_BAR_PADDING = 0
            data_chunk = data.iloc[max(0, session_ilocs[sc][0] - SESSION_BAR_PADDING):min(len(df) - 1, session_ilocs[sc][-1] + SESSION_BAR_PADDING)]
            self._save_chart_image(
                data_chunk,
                None,
                instrument,
                time_frame,
                pip_size,
                kwargs,
            )




    def write_html(self, df, save_path, instrument, time_frame, pip_size, **kwargs):
        b_use_multiprocessing = kwargs.get('b_use_multiprocessing')
        extrema = kwargs.get('extrema')
        if extrema is not None:
            OHLCAnalysis.add_extrema_dataframe_to_data(df, extrema)

        chart_data = []
        if (chart_start_date := kwargs.get('start_date')) is None:
            chart_start_date = df.iloc[0].name
        data = df[df.index >= chart_start_date]

        print('Splitting into sessions...')
        if bars_per_chart := kwargs.get('bars_per_chart'):
            if not isinstance(bars_per_chart, int) or bars_per_chart <= 0:
                raise RuntimeError(f'ChartOHLCPrinter() kwarg "bars_per_chart" must be a positive integer.')
            session_ilocs = self._break_into_chunks_by_ilocs(data, bars_per_chart=bars_per_chart)
        else:
            session_ilocs = self._break_into_sessions_by_ilocs(data)

        print('Generating charts...')
        with open(save_path / f'chart-{instrument}-{time_frame}-{Utils.format_date(chart_start_date)}.html', 'w') as fp:
            fp.write('<HTML>\n')
            fp.write('<BODY>\n')
            fp.write('<TABLE>\n')


            for sc in range(len(session_ilocs)):
                SESSION_BAR_PADDING = 0
                data_chunk = data.iloc[max(0, session_ilocs[sc][0] - SESSION_BAR_PADDING):min(len(df) - 1, session_ilocs[sc][-1] + SESSION_BAR_PADDING)]
                if b_use_multiprocessing:
                   chart_data.append((
                        data_chunk,
                        save_path / f'chart-{instrument}-{time_frame}-{sc}',
                        instrument,
                        time_frame,
                        pip_size,
                        kwargs,
                    ))
                else:
                    self._save_chart_image(
                        data_chunk,
                        save_path / f'chart-{instrument}-{time_frame}-{sc}',
                        instrument,
                        time_frame,
                        pip_size,
                        kwargs,
                    )

                title = f'{data.iloc[session_ilocs[sc][0]].name} to {data.iloc[session_ilocs[sc][-1]].name}'
                fp.write(
                    f'<TR><TD style="background: #808080;border: 1px solid black;padding: 20px 0px 20px;text-align: center;">')
                fp.write(f'<B>{title}</B></TD></TR>\n')

                fp.write(f'<TR><TD style="border: 1px solid black;padding: 3px 0px;text-align: center;">')
                fp.write(f'<IMG SRC="./chart-{instrument}-{time_frame}-{sc}.png">')
                fp.write(f'</TD></TR>\n')

            fp.write(f'</TABLE>\n')
            fp.write('</BODY></HTML>\n')

        if b_use_multiprocessing:
            print(f'Writing images to {save_path}...')
            with Pool() as pool:
                pool.starmap(self._save_chart_image, chart_data)

