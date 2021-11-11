#!/usr/bin/env python3

import sys
import mariadb
import logging

from src.dwd_data import DwdForecastLoader

import public_passwords as pw


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

    dwd_fc_loader = DwdForecastLoader(con)

    for station_id in ['N2147', 'P830', '10865']:
        logging.info(f'Getting and storing {station_id}')
        dwd_fc_loader.execute(station_id)

    con.close()
    logging.info('done')
