#! python3

### Holder of miscellaneous ETL code to run without needing to modify main.py


import logging, os, download_tidy_up as dtu, sys, db_data_lake as dl, db_data_refinery as dr, pandas as pd

## Set up paths
data_folder = os.path.join(os.getcwd(), 'downloaded_data')
snotel_folder = os.path.join(data_folder, 'snotel')
sf_folder = os.path.join(data_folder, 'streamflow')
tidal_folder = os.path.join(data_folder, 'tide_gauge')
wsu_folder = os.path.join(os.getcwd(), 'downloaded_data', 'wsu')
wsu_subfolder = os.path.join(wsu_folder, 'tidied_data_weather')
sl_file = os.path.join(wsu_folder, 'tidied_data_wsu_weather_station_list.csv')
sl_ps_file = os.path.join(wsu_folder, 'puget_sound_stations.csv')
si_file = os.path.join(wsu_folder, 'tidied_data_wsu_weather_station_info.csv')
# Toggle between windows or mac
# gecko_fullpath = os.path.join(os.getcwd(),'geckodriver_bins','mac','geckodriver')
gecko_fullpath = os.path.join(os.getcwd(), 'geckodriver_bins', 'win', 'geckodriver.exe')
dl_folder = os.path.join(os.getcwd(), 'sqlite', 'dl')
wrk_folder = os.path.join(os.getcwd(), 'sqlite')
dr_folder = os.path.join(os.getcwd(), 'sqlite', 'dr')
dst_folder = os.path.join(wsu_folder,'daylight_savings')
dst_file = os.path.join(dst_folder,'Daylight Savings Transition Dates.csv')

## Set up logging file
logging.basicConfig(level=logging.INFO
                    , filename='run_scratch_logging.txt'
                    , format=' %(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.info('Start run_scratch.py')

## Set up standard out file
sdpath = os.path.join('run_scratch_stdout.txt')
sys.stdout = open(sdpath, 'w')
sys.stderr = open(sdpath, 'a')

## Set up a second handler for output to stdout
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


# Download WSU weather data for daylight savings quality control
logging.info('Getting WSU daylight savings calibrators')
dtu.get_wsu_daylight_savings(output_folder=dst_folder,station_list_file=sl_file,
                       station_info_file = si_file, gecko_fullpath=gecko_fullpath
                             ,dst_file=dst_file)
