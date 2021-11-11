
import mariadb
import sys
import pandas as pd
import logging

import requests
from bs4 import BeautifulSoup


class TextToDB:
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
                                (CURRENT_TIMESTAMP , 1, '{txt}')""")
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
