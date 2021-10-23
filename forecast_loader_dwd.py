#!/usr/bin/env python3

import sys
from io import BytesIO
import pandas as pd
import mariadb

import logging

import zipfile
from lxml import etree
import re
from urllib.request import urlopen

import public_passwords as pw


def get_element_value_list(tree, element):
    # see https://github.com/dirkclemens/dwd-opendata-kml/blob/master/dwd-opendata-kml.py
    def numeric(s):
        try:
            if '-' in s:
                return 0
            else:
                return int(s)
        except ValueError:
            return round(float(s) * 1.0, 1)
    for df_ in tree.xpath('////*[name()="dwd:Forecast" and @*[name()="dwd:elementName" and .="%s"]]' % element):
        # strip unnecessary whitespaces
        elements = re.sub(r'\s+', r';', str(df_.getchildren()[0].text).lstrip(' '))
        lst = elements.split(';')
        for index, item in enumerate(lst):  # convert from string
            lst[index] = numeric(lst[index])
        return lst


def return_fc_df(tree):
    # see https://github.com/dirkclemens/dwd-opendata-kml/blob/master/dwd-opendata-kml.py
    for df_ in tree.xpath('////*[name()="dwd:IssueTime"]'):
        updated_at = pd.to_datetime(df_.text)

    ele_TimeStamp = []
    for df_ in tree.xpath('//*[name()="dwd:ForecastTimeSteps"]'):
        timeslots = df_.getchildren()
        for timeslot in timeslots:
            tm = timeslot.text
            ele_TimeStamp.append(tm)

    fc_df = pd.DataFrame(pd.to_datetime(ele_TimeStamp), columns=['timestamp'])

    temps = get_element_value_list(tree, 'TTT')
    for index, item in enumerate(temps):
        temps[index] = float(item) - 273.1
    fc_df['temperatur'] = temps

    ele_PPPP = get_element_value_list(tree, 'PPPP')  # =x/100
    for index, item in enumerate(ele_PPPP):
        ele_PPPP[index] = float(ele_PPPP[index]) / 100.0
    fc_df['druck'] = ele_PPPP

    ele_FX1 = get_element_value_list(tree, 'FX1')
    fc_df['wind_max_1h'] = ele_FX1  # m/s

    ele_ww = get_element_value_list(tree, 'ww')
    fc_df['ww'] = ele_ww

    ele_SunD = get_element_value_list(tree, 'SunD1')  # =round(x)
    for index, item in enumerate(ele_SunD):
        ele_SunD[index] = round(float(ele_SunD[index]) / 60)
    fc_df['sonnenscheindauer'] = ele_SunD  # in minutes/h

    ele_Neff = get_element_value_list(tree, 'Neff')  # =x*8/100
    fc_df['wolken_eff'] = ele_Neff  # in percent

    fc_df['p_regen_general'] = get_element_value_list(tree, 'R101')

    fc_df['niederschlag_1h'] = get_element_value_list(tree, 'RR1c')

    fc_df['wind'] = get_element_value_list(tree, 'FF')

    temps = get_element_value_list(tree, 'T5cm')
    for index, item in enumerate(temps):
        temps[index] = float(item) - 273.1
    fc_df['temperatur_boden'] = temps
    fc_df['sonneneinstrahlung'] = get_element_value_list(tree, 'Rad1h')

    fc_df = fc_df.sort_values('timestamp').set_index('timestamp')
    return fc_df, updated_at


def fetch_forecast(station_id_='P830'):
    url = (f'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/{station_id_}/kml/'
           f'MOSMIX_L_LATEST_{station_id_}.kmz'
           )
    kmz = zipfile.ZipFile(BytesIO(urlopen(url).read()), 'r')
    kml_filename = kmz.namelist()[0]
    tree = etree.parse(kmz.open(kml_filename, 'r'))
    df_, time = return_fc_df(tree)
    kmz.close()

    df_['last_update'] = time
    df_['station_id'] = station_id_
    return df_


def update_db(con_: 'mariadb.connection', df_: pd.DataFrame):
    # connected:
    cur = con_.cursor()

    logging.info('deleting_old_first:')
    latest = pd.read_sql(con=con_,
                         sql=f""" select ts, station_id, last_update from wetter.forecast_dwd 
                            where ts >= TIMESTAMP('{df.index.min().strftime('%Y-%m-%d %H:%M:%S')}') 
                            and station_id = '{station_id}'
                            """)
    for _, row in latest.iterrows():
        try:
            cur.execute(f"""
             delete from wetter.forecast_dwd  where 
                (station_id='{row['station_id']}' 
                and last_update <= TIMESTAMP('{row['last_update']}')
                and ts= TIMESTAMP('{row['ts']}')
             )
             """)
        except mariadb.Error as e:
            logging.error(f'Error when deleting row: {e} (row\n {row})')
            pass

    logging.info('now writing into wetter.forecast_dwd')
    for _, row in df_.reset_index().iterrows():
        try:
            cur.execute(
                f"""INSERT INTO forecast_dwd 
                        (ts,
                         station_id,
                         temperatur,
                         druck,
                         wind_max_1h,
                         ww,
                         sonnenscheinminuten,
                         wolken,
                         p_regen, 
                         niederschlag_1h,
                         wind,
                         temperatur_boden,
                         sonnenstrahlung,
                         last_update)
                    VALUES (TIMESTAMP('{row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}'),
                             '{row['station_id']}',
                             {row['temperatur']}, 
                             {row['druck']}, 
                             {row['wind_max_1h']}, 
                             {row['ww']}, 
                             {row['sonnenscheindauer']}, 
                             {row['wolken_eff']}, 
                             {row['p_regen_general']},
                             {row['niederschlag_1h']}, 
                             {row['wind']}, 
                             {row['temperatur_boden']}, 
                             {row['sonneneinstrahlung']}, 
                             TIMESTAMP('{row['last_update'].strftime('%Y-%m-%d %H:%M:%S')}')
                    )""")
        except mariadb.Error as e:
            logging.error(f'Error when inserting row: {e}, row {row}')
            pass

    con_.commit()
    logging.info(f'Done. Last Inserted ID: {cur.lastrowid}')
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='/home/pi/logs/db_dwd_wetter_fc.log',
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

    for station_id in ['N2147', 'P830', '10865']:
        logging.info(f'Getting and storing {station_id}')
        df = fetch_forecast(station_id)
        update_db(con, df)
    con.close()
    logging.info('done')
