#!/usr/bin/env python3

import mariadb
import sys
import logging

from src.dwd import TextToDB

import public_passwords as pw


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='/home/pi/logs/dwd_text_wetter.log',
                        level=logging.INFO)
    logging.info('script started - connecting to DB')
    try:
        con = mariadb.connect(
            database='wetter',
            **pw.mariadb_cred
        )
    except mariadb.Error as e:
        logging.error(f'Error connecting to MariaDB Platform: {e}')
        sys.exit(1)

    t2DB = TextToDB(con, 'http://141.38.2.26/weather/text_forecasts/html/VHDL50_DWMG_LATEST_html')
    t2DB.run()

    logging.info('Script finished successfully')
