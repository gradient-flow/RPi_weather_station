#!/usr/bin/env python3

import sys
from time import sleep
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mariadb

import public_passwords as pw


def load_data(con_: 'mariadb.connection') -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Loads dataframes from db
    :param con_: connection to MariaDB - Wetter
    :return: triple of dataframes: measured data of the last 3 days, actual forecast and significant weather codes.
    """
    logging.info('load from db: ')
    df = pd.read_sql(con=con_, sql=f"""
        SELECT * FROM wetter.messung WHERE zeit >= (TIMESTAMP(sysdate())- INTERVAL 3 day);
        """).set_index('id')

    df_raw = pd.read_sql(con=con_, sql=f"""
        select
            id, ts, station_id, last_update,
            temperatur, druck, sonnenscheinminuten, wind_max_1h, wind, niederschlag_1h, p_regen, ww
        from wetter.forecast_dwd
        where ts >= TIMESTAMP(sysdate())""").set_index('id')

    df_ww = pd.read_sql(con=con_, sql=f"""select * from wetter.ww_codes """).set_index('id')
    return df, df_raw, df_ww


def preprocess_graph(df_mess_, df_fc_raw_, df_ww_codes_) -> \
        (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, (float, float), (float, float)):
    """
    draws graph and saves it at path
    :param df_mess_: raw data with temperature, zeit, pressure, hell7
    :param df_fc_raw_: pd.DataFrame with forecast values
    :param df_ww_codes_: pd.DataFrame with forecast texts
    :return: 4 dataframes (30-min aggregated measures, mean forecast, light on_off timestamps,
     significant weather forecast)
    and two tuples with the y-axis limits for the temperature and pressure
    """
    df = df_mess_.set_index('zeit')
    light = df['hell'].dropna()
    on_off = list(light.loc[light != light.shift(-1)][:-1].index)

    df_agg = df.resample('30min').agg(
        {'temperature': 'mean', 'pressure': 'mean', 'hell': 'median'})


    # graph limits:
    a_p = df_agg.pressure.min()
    b_p = df_agg.pressure.max()
    lims_p = (a_p - 0.05 * (b_p - a_p), b_p + 0.05 * (b_p - a_p))
    a_t = df_agg.temperature.min()
    b_t = df_agg.temperature.max()
    lims_t = (min(-2, a_t - 0.05 * (b_t - a_t)), max(10, b_t + 0.05 * (b_t - a_t)))
    df_agg['hell'] = (a_p - 50) + df_agg['hell'] * (b_p + 100 - a_p)  # interpolate between pressure values

    # Forecast:
    now = pd.Timestamp.now(tz='CET')
    fc = df_fc_raw_.loc[(df_fc_raw_.ts.dt.tz_localize('CET') >= now)
                        & (df_fc_raw_.ts.dt.tz_localize('CET') <= now + pd.DateOffset(hours=36))].copy()
    mittel = fc.groupby('ts').mean()

    wetter = (fc.groupby('ts')
              [['ww']]
              .agg(lambda x: x.value_counts().index[0])
              .resample('2h')
              .agg(lambda x: x.value_counts().index[0])
              .reset_index()
              .merge(df_ww_codes_, left_on='ww', right_on='id')  # .set_index('ts')
              )
    return df_agg, mittel, on_off, wetter, lims_t, lims_p


def draw_graph(df_mess_: pd.DataFrame,
               df_fc_raw_: pd.DataFrame,
               df_ww_codes_: pd.DataFrame,
               path: str = '/var/www/html/img/wetter3.jpg',
               show_on_off: bool = False) -> 'matplotlib.pyplot':
    """
    draws graph and saves it at path
    :param df_mess_: raw data with temperature, zeit, pressure, hell
    :param df_fc_raw_: pd.DataFrame with forecast values
    :param df_ww_codes_: pd.DataFrame with forecast descriptions
    :param path: path where final plot is saved to
    :param show_on_off: flags if light changes shoud be shown
    :return: plot with measured values and forecast
    """
    df_agg, mittel, on_off, wetter, lims_t, lims_p = preprocess_graph(df_mess_, df_fc_raw_, df_ww_codes_)

    # Draw:

    plot_title = f'Wetterdaten (letzte 3 Tage); erstellt: {pd.Timestamp.now(tz="CET").strftime("%Y-%m-%d %H:%M")}'
    ax_t = df_agg.temperature.plot(grid=True, secondary_y=True, figsize=(12, 8), style='-', color='red', linewidth=3.5,
                                   title=plot_title)

    mittel[['temperatur']].resample('h').mean().plot(color='red', ax=ax_t, secondary_y=True, linewidth=3, style='--',
                                                     legend=None)
    ax_t.set_ylabel('Temperatur in °C', color='red')
    ax_t.set_ylim(lims_t)

    (mittel['p_regen']
     .resample('h')
     .mean() * (lims_t[1] - lims_t[0]) / 100 + lims_t[0]) \
        .plot(secondary_y=True, ax=ax_t, linestyle='--', color='deepskyblue', linewidth=3, legend=None)

    bottom, top = plt.ylim()
    for i in range(2, len(wetter) - 1):
        plt.text(wetter.iloc[i, 0], bottom + 1, str(wetter.iloc[i, 1]) + ' - ' + wetter.iloc[i, 2], rotation='vertical',
                 color='grey', alpha=1)

    ax_p = df_agg['hell'].plot(color='yellow', linewidth=0.01)
    ax_p = df_agg['pressure'].plot(style='-', color='indigo', grid=True, linewidth=3)
    ax_p.set_ylabel('Luftdruck in hPa', color='indigo')
    ax_p.set_ylim(lims_p)
    ax_p.fill_between(x=df_agg.index, y1=df_agg['hell'], facecolor='yellow', alpha=0.3)

    ax_t.set_zorder(ax_p.get_zorder() + 1)
    ax_t.patch.set_visible(False)
    ax_t.axhline(y=0, color='gray', linestyle='--')

    if show_on_off:
        for ts in on_off:
            plt.text(ts, lims_t[0] + 0.5, '* ' + ts.strftime('%H:%M'), rotation='vertical', color='g')

    # ax_p.xaxis.set_minor_locator(ticker.NullLocator())
    # positions = pd.date_range(start=pd.to_datetime('now') + pd.DateOffset(hours=-72),
    #                           end=pd.to_datetime('now') + pd.DateOffset(hours=36), freq='4h')
    # labels = [p.strftime('%d.%m. %H') + ' Uhr' for p in positions]
    # ax_p.set_xticks(positions)
    # ax_p.set_xticklabels(labels, fontsize=8, rotation=60, ha='right')
    # ax_p.set_xlabel('Zeit')

    plt.savefig(path)
    return plt


def preprocess_fc(df_fc_raw_: pd.DataFrame, df_ww_codes_: pd.DataFrame) \
        -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, 'Timestamp'):
    """
    Preprocesses raw fc data to be plotted
    :param df_fc_raw_: raw fc data with temperature, zeit, pressure, hell
    :param df_ww_codes_: significant weather codes
    :return: 3 dataframes (latest forecast, aggregated, significant weather descriptions) and latest update timestamp
    """
    latest_fc = df_fc_raw_.loc[df_fc_raw_.ts >= pd.to_datetime('now')].copy()
    last_update = latest_fc['last_update'].min()

    # different stations - take mean of scalar values
    mean_fc = latest_fc.groupby('ts').mean()

    # significant weather ww - take mode and merge with description
    significant_weather = (latest_fc.groupby('ts')[['ww']]
                           .agg(lambda x: x.value_counts().index[0]).
                           resample('6h')
                           .agg(lambda x: x.value_counts().index[0])
                           .reset_index()
                           .merge(df_ww_codes_, left_on='ww', right_on='id')  # .set_index('ts')
                           )
    return latest_fc, mean_fc, significant_weather, last_update


def draw_fc(df_fc_raw_: pd.DataFrame, df_ww_codes_: pd.DataFrame,
            path: str = '/var/www/html/img/fc.jpg') -> 'matplotlib.pyplot':
    """
    draws forecast graph and saves it at path
    :param df_fc_raw_: raw data with temperature, zeit, pressure, hell
    :param df_ww_codes_: significant weather codes
    :param path: path
    :return: plt with two subfigures
    """
    latest_fc, mean_fc, significant_weather, last_update = preprocess_fc(df_fc_raw_, df_ww_codes_)

    # draw plots:
    f, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(12, 8))
    # upper plot:
    mean_fc[['temperatur']].resample('h').mean().plot(color='red', grid=True, ax=ax1, linewidth=3)
    bottom, top = ax1.get_ylim()
    sonne = (mean_fc[['sonnenscheinminuten']].resample('h').mean() * (1 / 60) * (top - bottom) + bottom)
    sonne.plot(color='yellow', grid=True, ax=ax1, alpha=0.9, linewidth=3)
    ax1_r = mean_fc[['wind_max_1h', 'wind']].plot(grid=True, ax=ax1, alpha=1, secondary_y=True,
                                                  color=['green', 'turquoise'])
    ax1.set_ylabel('Temperatur in °C', color='red')

    ax1_r.set_ylabel('Windgeschwindigkeit in m/s', color='green')
    ax1.set_title(
        (f'dwd Vorhersage, erstellt: {pd.Timestamp.now(tz="CET").strftime("%Y-%m-%d %H:%M")}; '
         f'MOSMIX Daten von {last_update.strftime("%Y-%m-%d %H:%M")}')
    )
    ax1.fill_between(x=sonne.index,
                     y1=[item for sublist in sonne[['sonnenscheinminuten']].values.tolist() for item in sublist],
                     y2=bottom - 10, facecolor='yellow', alpha=0.3)
    ax1.set_ylim(bottom, top)
    lines = ax1.get_lines() + ax1.right_ax.get_lines()
    ax1.legend(lines, ['Temperatur', 'Sonnenschein-Anteil', 'Mittlerer Wind', 'Windböen'])

    # lower plot:
    (mean_fc[['niederschlag_1h']].resample('h').mean() * 100 / 60).plot.area(color='lightblue', grid=True, ax=ax2,
                                                                             linewidth=3)
    ax2_r = mean_fc[['p_regen']].resample('h').mean().plot(color='blue', grid=True, ax=ax2, secondary_y=True)
    ax2_r.set_ylim(0, 100)
    ax2.set_ylabel('Niederschlag in mm', color='lightblue')
    ax2_r.set_ylabel('Regenwahrscheinlichkeit in Prozent', color='blue')
    lines = ax2.get_lines() + ax2.right_ax.get_lines()
    ax2.legend(lines, ['Niederschlag (1h)', 'Regenwahrscheinlichkeit'])

    # ticks:
    positions = [p for p in mean_fc.index if (p.hour % 6 == 0)]
    labels = [p.strftime('%d.%m. %H') + ' Uhr' for p in positions]
    ax2.set_xticks(positions)
    ax2.set_xticklabels(labels, fontsize=8, rotation=60, ha='right')
    ax2.set_xlabel('Datum')
    bottom, top = plt.ylim()

    # ww texts
    for i in range(0, len(significant_weather)):
        plt.text(significant_weather.iloc[i, 0],
                 bottom + 1,
                 f'{significant_weather.iloc[i, 2]}  ({significant_weather.iloc[i, 1]})',
                 rotation='vertical', color='grey', alpha=0.8)
        
    # day of week texts:
    day_locs = [p for p in positions if (p.hour == 12)]
    weekdays = {6: 'So', 0: 'Mo', 1: 'Di', 2: 'Mi', 3: 'Do', 4: 'Fr', 5: 'Sa'}
    for i, dl in enumerate(day_locs):
        plt.text(dl, top + 5, weekdays.get(dl.weekday(), ''), size=16, color='black')
    plt.savefig(path)
    return plt


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='/home/pi/logs/graph_wetter.log',
                        level=logging.INFO)
    logging.info(' *  Script started - connecting to DB')

    try:
        con = mariadb.connect(
            database='wetter',
            **pw.mariadb_cred
        )
    except mariadb.Error as e:
        logging.error(f'Error connecting to MariaDB Platform: {e}')
        sys.exit(1)
    logging.info('connected to MariaDB - wetter')
    # sleep(15)  # Wait until DB is updated...

    # load
    df_mess, df_fc_raw, df_ww_codes = load_data(con)
    logging.info('data loaded from DB, preparing graphs')
    _ = draw_graph(df_mess, df_fc_raw, df_ww_codes)
    _ = draw_fc(df_fc_raw, df_ww_codes)
    logging.info('Script finished successfully')
