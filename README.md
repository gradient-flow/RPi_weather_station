# RPi_weather_station
Public Repo of the backend python scripts for a local weather station running on a Raspberry Pi.

The four scripts update the Maria DB with new measurements of temperature, pressure and sunlight, get the latest local weather forecast from dwd (both text and numeric values) and create two plots which are saved on a local webserver.

#### forecast_loader_dwd.py
Reads the publicly available dwd opendata forecast such as temperaure, significant weather etc. and saves it into the DB.

#### get_wetter_text_to_db.py
Reads the DWD Strassenwettervorhersage for Bavaria from http://141.38.2.26/weather/text_forecasts/html/VHDL50_DWMG_LATEST_html and saves it into the DB.

#### emperature_pressure_db.py
Connects to the temperature, pressure and light sensors, reads the values and stores them in the DB.

#### wetter_graph.py
Reads the latest measurements and forecasts and combines these into three plots, aggregating the local measurements with a short-term and a long-term forecast.


