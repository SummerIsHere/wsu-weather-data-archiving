#! python3

### Main function to run the entire chain of downloading and extracting data for Seattle station only
###
###

import logging, os, download_tidy_up as dtu, sys, db_data_lake as dl, db_data_refinery as dr, pandas as pd

## Set up paths
data_folder = os.path.join(os.getcwd(), 'downloaded_data')
wsu_folder = os.path.join(os.getcwd(), 'downloaded_data', 'wsu')
wsu_subfolder = os.path.join(wsu_folder, 'tidied_data_weather')
sl_file = os.path.join(wsu_folder, 'seattle_station_list.csv')
si_file = os.path.join(wsu_folder, 'seattle_station_info.csv')
# gecko_fullpath = os.path.join(os.getcwd(),'geckodriver_bins','mac','geckodriver')
gecko_fullpath = os.path.join(os.getcwd(), 'geckodriver_bins', 'win', 'geckodriver.exe')
dl_folder = os.path.join(os.getcwd(), 'sqlite', 'dl')
wrk_folder = os.path.join(os.getcwd(), 'sqlite')
dr_folder = os.path.join(os.getcwd(), 'sqlite', 'dr')
dst_folder = os.path.join(wsu_folder,'daylight_savings')
dst_file = os.path.join(dst_folder,'Daylight Savings Transition Dates.csv')

## Set up logging file
logging.basicConfig(level=logging.INFO
                    , filename='seattle_logging.txt'
                    , format=' %(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.info('Start main.py')

## Set up standard out file
sdpath = os.path.join('seattle_stdout.txt')
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



#dtu.get_wsu_station_list(output_folder=wsu_folder)

# Download WSU weather data for daylight savings quality control
logging.info('Getting WSU daylight savings calibrators')
dtu.get_wsu_daylight_savings(output_folder=dst_folder,station_list_file=sl_file,
                       station_info_file = si_file, gecko_fullpath=gecko_fullpath
                             ,dst_file=dst_file)

# logging.info('Calling get_wsu_weather_data')
dtu.get_wsu_weather_data(output_folder=wsu_subfolder,station_list_file=sl_file,
                       station_info_file = si_file, gecko_fullpath=gecko_fullpath)


#logging.info('Create data lakes')
dl.create_data_lakes(sl_file=sl_file,dl_folder=dl_folder,overwrite=True)

logging.info('Calling load_dl_wsu_weather')
dl.load_dl_wsu_weather(sl_file=sl_file, csv_base_folder=wsu_subfolder, dl_folder = dl_folder, overwrite = True)

logging.info('Create data refineries')
dr.create_data_refineries(sl_file=sl_file, dr_folder = dr_folder, overwrite=True)

logging.info('Calling wsu_progress')
dtu.wsu_progress(station_list_file=sl_file,output_folder=wsu_folder, scan_folder=wsu_subfolder
                , dl_folder=dl_folder, dr_folder=dr_folder)

logging.info('Entering loop for loading dl into dr')
stat_list = pd.read_csv(sl_file)
logging.info(str(stat_list.loc[:, 'station_id']))
for this_stat in stat_list.loc[:, 'station_id']:
    logging.debug('very start of loop')
    this_stat = str(this_stat)
    logging.debug('start loop of ' + this_stat)
    logging.debug('loading ' + this_stat)
    try:
        dr.load_wsu_weather_dl_to_dr(this_stat=this_stat, dl_folder=dl_folder
                                     , working_folder = wrk_folder, dr_folder = dr_folder
                                     ,write_csv=True)
    except Exception as e:
        logging.warning('load_wsu_weather_dl_to_dr: Error thrown')
        logging.warning('The exception caught:')
        logging.warning(str(e))
        logging.info(str(e.args))
        logging.warning('Moving on to next station')
        continue

logging.info('Calling wsu_progress')
dtu.wsu_progress(station_list_file=sl_file,output_folder=wsu_folder, scan_folder=wsu_subfolder, dl_folder=dl_folder
                 , dr_folder=dr_folder)


logging.info('End seattle.py')
