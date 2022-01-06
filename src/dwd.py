#!/usr/bin/env python3

import logging
import mariadb
import pandas as pd
from typing import Union

from io import BytesIO
import zipfile
from lxml import etree
import re
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup


class TextToDB:
    """
    Class used to read text weather forecast and save it to database.
    """
    def __init__(self,
                 connection: 'mariadb.connection',
                 url: str):
        """
        Initialize with given connection and url.

        :param connection: Maria DB connection
        :param url: website's url
        """
        self._con = connection
        self._url = url
        self._text = None

    def _read_text(self) -> None:
        """
        Reads url and gets the text using BeautifulSoup

        :return: None (changes self._text)
        """
        response = requests.get(self._url)
        soup = BeautifulSoup(response.text, 'html.parser')
        self._text = soup.get_text(strip=True)
        return

    def _write_text_to_db(self) -> None:
        """
        Writes the DWD text forecast into database
        :return: None
        """
        latest = pd.read_sql(con=self._con,
                             sql=f"""select ts, fc_text from wetter.forecast_text
                                     where forecast=1 ORDER BY ts DESC LIMIT 1 """)['fc_text'].values[0]
        if (self._text.find("'") >= 0) | (self._text is None) | (self._text == latest):  # simple sql injection avoided
            logging.warning('text with errors or not new, aborted.')
        else:
            logging.info('now writing into wetter.forecast_text')
            cur = self._con.cursor()
            try:
                cur.execute(f"""INSERT INTO forecast_text 
                                (ts, forecast, fc_text) VALUES 
                                (CURRENT_TIMESTAMP , 1, '{self._text}')""")
            except mariadb.Error as e:
                logging.error(f'Error when inserting text: {e}')
                pass
            self._con.commit()
            logging.info(f'Done. Last Inserted ID: {cur.lastrowid}')
        return

    def run(self) -> None:
        """
        Gets text and saves it into DB.

        :return: None
        """
        logging.info('Loading Text')
        self._read_text()
        logging.info('Writing Text to DB')
        self._write_text_to_db()
        logging.info('Closing Connection')
        self._con.close()
        return

    @property
    def con(self):
        return self._con

    @property
    def url(self):
        return self._url

    @property
    def text(self):
        return self._text


class DwdForecastLoader:
    """
    Class used to load raw numerical forecast from dwd, extract needed values and save them into DB.
    """
    def __init__(self, con: 'MariaDB.connection'):
        self._con = con
        self._station_id = None
        self._df = None
        pass

    @property
    def con(self):
        return self._con

    @property
    def station_id(self):
        return self._station_id

    @property
    def df(self):
        return self._df

    @staticmethod
    def _get_element_value_list(tree: etree.ElementTree, element: str) -> list:
        # see https://github.com/dirkclemens/dwd-opendata-kml/blob/master/dwd-opendata-kml.py
        def _numeric(s) -> Union[float, int]:
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
                lst[index] = _numeric(lst[index])
            return lst

    @staticmethod
    def _return_fc_df(tree: etree.ElementTree) -> (pd.DataFrame, pd.Timestamp):
        # see https://github.com/dirkclemens/dwd-opendata-kml/blob/master/dwd-opendata-kml.py
        updated_at = None
        for df_ in tree.xpath('////*[name()="dwd:IssueTime"]'):
            updated_at = pd.to_datetime(df_.text)

        ele_TimeStamp = []
        for df_ in tree.xpath('//*[name()="dwd:ForecastTimeSteps"]'):
            timeslots = df_.getchildren()
            for timeslot in timeslots:
                tm = timeslot.text
                ele_TimeStamp.append(tm)

        fc_df = pd.DataFrame(pd.to_datetime(ele_TimeStamp), columns=['timestamp'])

        temps = DwdForecastLoader._get_element_value_list(tree, 'TTT')
        for index, item in enumerate(temps):
            temps[index] = float(item) - 273.1
        fc_df['temperatur'] = temps

        ele_PPPP = DwdForecastLoader._get_element_value_list(tree, 'PPPP')  # =x/100
        for index, item in enumerate(ele_PPPP):
            ele_PPPP[index] = float(ele_PPPP[index]) / 100.0
        fc_df['druck'] = ele_PPPP

        ele_FX1 = DwdForecastLoader._get_element_value_list(tree, 'FX1')
        fc_df['wind_max_1h'] = ele_FX1  # m/s

        ele_ww = DwdForecastLoader._get_element_value_list(tree, 'ww')
        fc_df['ww'] = ele_ww

        ele_SunD = DwdForecastLoader._get_element_value_list(tree, 'SunD1')  # =round(x)
        for index, item in enumerate(ele_SunD):
            ele_SunD[index] = round(float(ele_SunD[index]) / 60)
        fc_df['sonnenscheindauer'] = ele_SunD  # in minutes/h

        ele_Neff = DwdForecastLoader._get_element_value_list(tree, 'Neff')  # =x*8/100
        fc_df['wolken_eff'] = ele_Neff  # in percent

        fc_df['p_regen_general'] = DwdForecastLoader._get_element_value_list(tree, 'R101')

        fc_df['niederschlag_1h'] = DwdForecastLoader._get_element_value_list(tree, 'RR1c')

        fc_df['wind'] = DwdForecastLoader._get_element_value_list(tree, 'FF')

        temps = DwdForecastLoader._get_element_value_list(tree, 'T5cm')
        for index, item in enumerate(temps):
            temps[index] = float(item) - 273.1
        fc_df['temperatur_boden'] = temps
        fc_df['sonneneinstrahlung'] = DwdForecastLoader._get_element_value_list(tree, 'Rad1h')

        fc_df = fc_df.sort_values('timestamp').set_index('timestamp')
        return fc_df, updated_at

    def _read_forecast(self, station_id: str = 'P830') -> None:
        url = (f'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/{station_id}/kml/'
               f'MOSMIX_L_LATEST_{station_id}.kmz'
               )
        kmz = zipfile.ZipFile(BytesIO(urlopen(url).read()), 'r')
        kml_filename = kmz.namelist()[0]
        tree = etree.parse(kmz.open(kml_filename, 'r'))
        df_, time = DwdForecastLoader._return_fc_df(tree)
        kmz.close()

        df_['last_update'] = time
        df_['station_id'] = station_id

        self._station_id = station_id
        self._df = df_
        return

    def _write_to_db(self) -> None:
        cur = self._con.cursor()

        logging.info('deleting_old_first:')
        latest = pd.read_sql(con=self._con,
                             sql=f""" select ts, station_id, last_update from wetter.forecast_dwd 
                                where ts >= TIMESTAMP('{self._df.index.min().strftime('%Y-%m-%d %H:%M:%S')}') 
                                and station_id = '{self._station_id}'
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
        for _, row in self._df.reset_index().iterrows():
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

        self._con.commit()
        logging.info(f'Done. Last Inserted ID: {cur.lastrowid}')
        return

    def execute(self, station_id='P830'):
        logging.info(f'Reading dwd data at station {station_id}.')
        self._read_forecast(station_id)
        logging.info('Writing data to DB.')
        self._write_to_db()
        logging.info(f'Done with forecast at station {station_id}.')
