import mariadb
import sys
import pandas as pd
import logging
# import typing

import requests
# import urllib.request
from bs4 import BeautifulSoup

import public_passwords as pw


def read_text(url: str = 'http://141.38.2.26/weather/text_forecasts/html/VHDL50_DWMG_LATEST_html') -> str:
    """
    Reads url and gets the text using BeautifulSoup
    :param url: website's url, for Bavaria: http://141.38.2.26/weather/text_forecasts/html/VHDL50_DWMG_LATEST_html
    :return: Text from website
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.get_text(strip=True)


def write_text_to_db(con_: 'mariadb.connection', txt: str) -> None:
    """
    Writes the DWD text forecast into database
    :param con_: connection to wetter DB
    :param txt: Forecast text
    :return: None
    """
    cur = con_.cursor()
    latest = pd.read_sql(con=con_,
                         sql=f"""select ts, fc_text from wetter.forecast_text
                                 where forecast=1 ORDER BY ts DESC LIMIT 1 """)['fc_text'].values[0]
    if (txt.find("'") >= 0) | (txt == latest):  # simple sql injection avoided -- phew
        logging.warning('text with errors or not new, aborted.')
        con_.close()
    else:
        logging.info('now writing into wetter.forecast_text')
        try:
            cur.execute(f"""INSERT INTO forecast_text 
                            (ts, forecast, fc_text) VALUES 
                            (CURRENT_TIMESTAMP , 1, '{txt}')""")
        except mariadb.Error as e:
            logging.error(f'Error when inserting text: {e}')
            pass
        con_.commit()
        logging.info(f'Done. Last Inserted ID: {cur.lastrowid}')
        con_.close()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='/home/pi/logs/dwd_text_wetter.log',
                        level=logging.INFO)
    logging.info('script started')

    text = read_text()

    logging.info('connecting to DB')
    try:
        con = mariadb.connect(
            database='wetter',
            **pw.mariadb_cred
        )
    except mariadb.Error as e:
        logging.error(f'Error connecting to MariaDB Platform: {e}')
        sys.exit(1)
    logging.info('connected to MariaDB - wetter')
    write_text_to_db(con, text)
    logging.info('Script finished successfully')
