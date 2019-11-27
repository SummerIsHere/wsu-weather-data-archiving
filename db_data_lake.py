#! python3

### This file contains functions related loading minimally transformed web data from text .csv files into data lake tables
### in a sqlite database


import sqlite3, logging, pandas as pd, os, re, download_tidy_up as dtu
from datetime import datetime

### Create an sqlite database for the data lake and initialize table for WSU weather data
### Structures for other tables are not initialized since they are self-contained, single file datasets
### and so can be created by pandas directly without worrying about different columns
def create_dl_db(db_fullpath, overwrite=False):
    conn = sqlite3.connect(db_fullpath, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    ## Create data lake table for WSU weather data
    logging.info('Start creating dl_wsu_weather table')
    if overwrite:
        cur.execute('DROP TABLE IF EXISTS dl_wsu_weather')
        logging.info('Dropped dl_wsu_weather if it existed')
    try:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS dl_wsu_weather
        (
        station_id      text        NOT NULL
        ,location       text        NOT NULL
        ,station_name   text        NOT NULL
        ,date           text        NOT NULL
        ,time_pdt       text
        ,time_pst       text
        ,air_temp_f             real    
        ,second_air_temp_f      real    
        ,dew_point_f            real    
        ,rel_humidity_perc      real    
        ,leaf_wet_u             real    
        ,wind_dir_at_speed_mph  text    
        ,wind_gust_mph          real    
        ,bed_temp_f             real    
        ,two_inch_soil_temp_f   real    
        ,eight_inch_soil_temp_f real    
        ,soil_vwc_perc          real    
        ,total_precip_inch      real    
        ,solar_rad_watts_per_meter_squared      real    
        ,atm_pressure_in_Hg                     real    
        ,source_filename                        text        NOT NULL
        ,source_rownum                          integer     NOT NULL
        ,source_download_timestamp              timestamp   NOT NULL
        ,source_load_timestamp                  timestamp   NOT NULL
        ,PRIMARY KEY (source_filename,source_rownum,source_download_timestamp)
        )
        ''')
        conn.commit()
        logging.info('Created dl_wsu_weather, committed changes')
    except Exception as e:
        logging.warning('Failed to create dl_wsu_weather, following error thrown')
        logging.warning(str(e))

    ## Create table for translating between the column names in the csv files and the column name in the db
    logging.info('Creating header_names table')
    cur.execute('DROP TABLE IF EXISTS header_names')
    logging.info('Dropped header_names if it existed')
    try:
        cur.execute('''
    CREATE TABLE IF NOT EXISTS header_names
    (
    csv_header_name     text    not null
    ,db_column_name     text    not null
    ,db_table_name      text    not null
    ,PRIMARY KEY (db_table_name,csv_header_name)
    )
    ''')
        logging.info('Created header_names')
        ## Populate header names for common audit fields
        logging.info('Start insert into header_names for common audit fields')
        cur.execute("INSERT INTO header_names VALUES ('Download Timestamp','source_download_timestamp','audit')")
        cur.execute("INSERT INTO header_names VALUES ('source_filename','source_filename','audit')")
        cur.execute("INSERT INTO header_names VALUES ('source_load_timestamp','source_load_timestamp','audit')")
        cur.execute("INSERT INTO header_names VALUES ('index','source_rownum','audit')")

        ## Populate header_names for dl_wsu_weather
        logging.info('Start insert into header_names for dl_wsu_weather')
        cur.execute("INSERT INTO header_names VALUES ('Date','date','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Time PDT','time_pdt','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Time PST','time_pst','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Air Temp °F','air_temp_f','dl_wsu_weather')")
        cur.execute(
            "INSERT INTO header_names VALUES (' 2<sup>nd</sup> Air Temp °F','second_air_temp_f','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Dew Point °F','dew_point_f','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Rel Hum %','rel_humidity_perc','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Leaf Wet u.','leaf_wet_u','dl_wsu_weather')")
        cur.execute(
            "INSERT INTO header_names VALUES (' Wind Dir @ Speed mph','wind_dir_at_speed_mph','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Wind Gust mph','wind_gust_mph','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Bed Temp °F','bed_temp_f','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' 2 in. Soil Tmp °F','two_inch_soil_temp_f','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' 8 in. Soil Tmp °F','eight_inch_soil_temp_f','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Soil VWC %','soil_vwc_perc','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Tot Prec in','total_precip_inch','dl_wsu_weather')")
        cur.execute(
            "INSERT INTO header_names VALUES (' Solar Rad W/m²','solar_rad_watts_per_meter_squared','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES (' Atm. Press. inHg','atm_pressure_in_Hg','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Location','location','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Station ID','station_id','dl_wsu_weather')")
        cur.execute("INSERT INTO header_names VALUES ('Station Name','station_name','dl_wsu_weather')")

        ## Populate header_names for dl_co2
        logging.info('Start insert into header_names for dl_co2')
        cur.execute("INSERT INTO header_names VALUES ('Year','year','dl_co2')")
        cur.execute("INSERT INTO header_names VALUES ('Month','month','dl_co2')")
        cur.execute("INSERT INTO header_names VALUES ('Decimal Date','decimal_date','dl_co2')")
        cur.execute("INSERT INTO header_names VALUES ('Avg ppm','avg_co2_ppm','dl_co2')")
        cur.execute("INSERT INTO header_names VALUES ('Interpolated ppm','interpolated_co2_ppm','dl_co2')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Seasonally Adjusted ppm','seasonally_adjusted_co2_ppm','dl_co2')")
        cur.execute("INSERT INTO header_names VALUES ('# Daily Means','num_daily_means','dl_co2')")

        ## Populate header names for dl_global_temperature_anomaly
        logging.info('Start insert into header_names for dl_global_temperature_anomaly')
        cur.execute("INSERT INTO header_names VALUES ('Date','date','dl_global_temperature_anomaly')")
        cur.execute("INSERT INTO header_names VALUES ('DateText','datetext','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Median Global Temp C Deviation','med_global_temp_c_dev','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Bias Uncertainty (Lower 95 CI)','bias_uncertainty_low_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Bias Uncertainty (Upper 95 CI)','bias_uncertainty_upp_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Measurement and Sampling Uncertainty (Lower 95 CI)','meas_samp_uncertainty_low_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Measurement and Sampling Uncertainty (Upper 95 CI)','meas_samp_uncertainty_upp_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Coverage Uncertainty (Lower 95 CI)','covg_uncertainty_low_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Coverage Uncertainty (Upper 95 CI)','covg_uncertainty_upp_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Measurement, Sampling, and Bias Uncertainty (Lower 95 CI)','meas_samp_bias_uncertainty_low_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Measurement, Sampling, and Bias Uncertainty (Upper 95 CI)','meas_samp_bias_uncertainty_upp_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Combined Uncertainty (Lower 95 CI)','combined_uncertainty_low_95_ci','dl_global_temperature_anomaly')")
        cur.execute(
            "INSERT INTO header_names VALUES ('Combined Uncertainty (Upper 95 CI)','combined_uncertainty_upp_95_ci','dl_global_temperature_anomaly')")
        conn.commit()
        logging.info('Committed changes')
    except Exception as e:
        logging.warning('Error creating header_names, following error thrown')
        logging.warning(str(e))

    conn.commit()
    logging.info('Closing connection')
    cur.close()
    conn.close()

### Create data lake files for each station
def create_data_lakes(sl_file,dl_folder,overwrite=False):
    # Cycle through station list creating data bases
    stat_list = pd.read_csv(sl_file)
    for this_stat in stat_list.loc[:, 'station_id']:
        this_stat = str(this_stat)
        logging.info('Creating data lake for ' + this_stat)
        this_stat = str(this_stat)
        logging.info('Loading data lake for ' + this_stat)
        # Create data lake
        db_fullpath = os.path.join(dl_folder, ('dl_' + this_stat + '.db'))
        create_dl_db(db_fullpath=db_fullpath, overwrite=overwrite)
        
### Insert a single file's worth of data into dl_wsu_weather
def insert_wsu_csv(filepath,db_fullpath,overwrite):

    ## Initialize database connection
    conn = sqlite3.connect(db_fullpath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    ## Split out the filename
    filename = os.path.split(filepath)
    filename = filename[-1]

    ## If the same filename is found in the database and we are overwriting, delete those rows. If
    ## the filename is found but we are not overwriting, stop right now
    temp_sql = 'SELECT COUNT(1) FROM dl_wsu_weather WHERE source_filename = \'' + filename + '\''
    same_fn = pd.read_sql(sql=temp_sql, con=conn).iloc[0, 0]
    if same_fn > 0:
        if overwrite:
            logging.debug(filename + ' found in database. Deleting since we are overwriting')
            temp_sql = 'DELETE FROM dl_wsu_weather WHERE source_filename = \'' + filename + '\''
            cur.execute(temp_sql)
        else:
            logging.debug(filename + ' found in database. Since we are not overwriting, stopping here')
            return None

    ## Read in csv file and add audit columns
    tab = pd.read_csv(filepath)
    tab.reset_index(inplace=True)
    tab.loc[:, 'index'] = tab.loc[:, 'index'] + 1

    tab.loc[:, 'source_filename'] = filename
    tab.loc[:, 'source_load_timestamp'] = datetime.now()

    ## Map variable number of data frame columns to database columns and rename data frame columns to match
    col_trans = pd.read_sql_query("SELECT * FROM header_names WHERE db_table_name IN ('dl_wsu_weather','audit')", conn)
    src_col_names = pd.DataFrame({'csv_header_name': tab.columns.values})
    logging.debug('col_trans: ' + str(col_trans))
    logging.debug('src_col_names: ' + str(src_col_names))
    new_src_cols = pd.merge(left=src_col_names, right=col_trans, how='left', on='csv_header_name')
    new_src_cols_list = new_src_cols.loc[:, 'db_column_name'].values.tolist()
    logging.debug('new_src_cols: ' + str(new_src_cols))
    tab.rename(columns=dict(zip(tab.columns.values, new_src_cols_list)), inplace=True)

    ## If any columns have not been converted, list their names and raise an Exception
    db_nn = new_src_cols.loc[:, 'db_column_name'].isnull()
    if db_nn.values.any():
        new_cols = new_src_cols.loc[db_nn, :]
        logging.warning('New columns found')
        logging.warning('new_cols:')
        logging.warning(new_cols)
        raise Exception('New columns found')

    ## Insert data into sqlite db
    tab.to_sql(name='dl_wsu_weather', con=conn, if_exists='append', index=False)

    ## Close database connection
    cur.close()
    conn.close()
    logging.info('Inserted ' + filepath + ' into ' + db_fullpath)


### Load station list.
### Scan for wsu weather data files and insert into data lake db
def load_dl_wsu_weather(sl_file, csv_base_folder, dl_folder, overwrite=False):

    # Cycle through station list
    stat_list = pd.read_csv(sl_file)
    for this_stat in stat_list.loc[:, 'station_id']:

        this_stat = str(this_stat)
        logging.info('Loading data lake for ' + this_stat)

        # Get station csvs
        station_csv_folder = os.path.join(csv_base_folder, this_stat)
        try:
            sfl = dtu.find_station_files(station_csv_folder, this_stat)
            db_fullpath = os.path.join(dl_folder,('dl_'+this_stat + '.db'))
            logging.debug(str(len(sfl)) + ' files to load into ' + db_fullpath)
        except Exception as e:
            logging.warning('load_dl_wsu_weather: Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next station')
            continue
        # Cycle through files, inserting into data lake one at a time
        for thisFile in sfl:
            fp = os.path.join(station_csv_folder,thisFile)
            insert_wsu_csv(filepath = fp, db_fullpath=db_fullpath, overwrite=overwrite)

