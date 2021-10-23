#!/usr/bin/env python3

import sys
# import datetime
from time import sleep
import logging
import mariadb
from typing import Callable

import board
import busio
import adafruit_bmp280
import RPi.GPIO as GPIO

import public_passwords as pw


def get_value_repeatedly(func: Callable = lambda x: None, iterations: int = 5, **kwargs):
    """
    Sensors are stuck sometimes, so it helps to repeatedly try to read a proper value
    """
    c = 0
    val = func(**kwargs)
    while (c < iterations) & (val is None):
        val = func(**kwargs)
        c += 1
        sleep(1)
    if val is None:
        logging.warning('could not read value')
    return val


def read_light(pin_in: int = 24) -> int:
    """
    Get the value from light sensor.
    :param pin_in: Specifies connected pin, current setup is GPIO_PIN 24 [No18]
    :return: integer value read by digital light sensor
    """
    # config GPIO:
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(pin_in, GPIO.IN)
    # read
    light = GPIO.input(pin_in)
    GPIO.cleanup()  # cleanup all GPIO
    return light


def read_bmp280_vals(address: int = 0x76) -> (float, float):
    """
    Get temperature and pressure from BMP280.
    :param address: specify I2C address
    :return: tuple (temperature, pressure)
    """
    # setup BMP280 / I2C
    i2c = busio.I2C(board.SCL, board.SDA)
    bmp280 = adafruit_bmp280.Adafruit_BMP280_I2C(i2c, address=address)
    bmp280.sea_level_pressure = 1025.25

    temperature = bmp280.temperature
    pressure = bmp280.pressure
    del bmp280

    return temperature, pressure


def read_weather() -> (bool, float, float):
    """
    Returns the light, temperature and pressure sensor results
    :return: triple (light, temperature, pressure)
    """
    light = get_value_repeatedly(read_light)
    hell = None
    try:
        hell = not bool(light)  # light is actually 1 if it is dark...
    except TypeError as e:
        logging.error(f'Light sensor does not give a proper output! {e}')

    temp_to_db = None
    press_to_db = None
    try:
        temp_to_db, press_to_db = read_bmp280_vals()
    except (ValueError, OSError) as e:
        logging.error(f'Could not read values from BMP280. Check connection - {e}')

    return hell, temp_to_db, press_to_db


def round_or_null(val, n=2) -> [str, float]:
    """
    Will round val to n digits precision. If val is None or fails to be rounded the function returns 'NULL'
    :param val: value to be rounded
    :param n: no of digits after the comma (precision)
    :return: rounded value or, if not possible, string 'NULL'
    """
    res = 'NULL'
    if val is None:
        return res
    try:
        res = round(val, n)
    except TypeError as e:
        logging.error(f'Could not round value {val}: {e}')
    return res


def write_into_db(con_: 'mariadb.connection', hell: bool, temperature: float, pressure: float) -> None:
    """
    Writes values into MariaDB
    :param con_: DB connection
    :param hell: light intensity (boolean)
    :param temperature: measured temperature
    :param pressure: measured air pressure
    :return: None
    """
    # connected:
    cur = con_.cursor()
    logging.info(f'Connected to DB - Now storing values {hell}, {temperature}, {pressure} in DB:')
    try:
        cur.execute(
            f"""INSERT INTO messung (zeit, temperature, pressure, hell) 
                VALUES (CURRENT_TIMESTAMP,
                        {round_or_null(temperature, 2)},
                        {round_or_null(pressure, 2)},
                        {round_or_null(hell)})
            """)
    except mariadb.Error as e:
        logging.error(f'Error when inserting new measurements: {e}')
        pass

    con_.commit()
    logging.info(f'Done. Last Inserted ID: {cur.lastrowid}')
    con_.close()
    return


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename='/home/pi/logs/db_mess_wetter.log',
                        level=logging.INFO)
    logging.info('Script started, reading values')
    sensor_values = read_weather()
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

    write_into_db(con, *sensor_values)
    logging.info('Script finished successfully')
