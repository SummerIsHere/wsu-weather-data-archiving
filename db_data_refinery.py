#! python3

### This file contains functions related to building the tables in the data refinery using tables in the data lake

### Import statements
import os, shutil, sqlite3, pandas as pd, logging, numpy as np, hashlib
from datetime import timedelta, datetime


### Create an sqlite database for the data lake and initialize table for WSU weather data
### Structures for other tables are not initialized since they are self-contained, single file datasets
### and so can be created by pandas directly without worrying about different columns
def create_dr_db(db_fullpath, overwrite=False):

    conn = sqlite3.connect(db_fullpath, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    ## Create data refinery table for WSU weather data
    logging.info('Start creating dr_wsu_weather table')
    if overwrite:
        cur.execute('DROP TABLE IF EXISTS dr_wsu_weather')
        logging.info('Dropped dr_wsu_weather if it existed')
    try:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS dr_wsu_weather
        (
        station_id      text        NOT NULL
        ,timestamp      timestamp   NOT NULL
        ,location       text
        ,station_name   text
        ,air_temp_f             real
        ,second_air_temp_f      real
        ,dew_point_f            real
        ,rel_humidity_perc      real
        ,leaf_wet_u             real
        ,wind_dir               text
        ,wind_speed_mph         real
        ,wind_gust_mph          real
        ,bed_temp_f             real
        ,two_inch_soil_temp_f   real
        ,eight_inch_soil_temp_f real
        ,soil_vwc_perc          real
        ,total_precip_inch      real
        ,solar_rad_watts_per_meter_squared      real
        ,atm_pressure_in_Hg                     real
        ,load_batch_ts          timestamp
        ,update_batch_ts        timestamp
        ,PRIMARY KEY (station_id,timestamp)
        )
        ''')
        conn.commit()
        logging.info('Successfully created and committed dr_wsu_weather')
    except Exception as e:
        logging.warning('Failed to create dr_wsu_weather, following error thrown')
        logging.warning(str(e))

    ## Create data lineage table
    logging.info('Start creating dr_wsu_lineage table')
    if overwrite:
        cur.execute('DROP TABLE IF EXISTS dr_wsu_lineage')
        logging.info('Dropped dr_wsu_weather if it existed')
    try:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS dr_wsu_lineage
        (
        source_filename             text
        ,source_rownum              integer
        ,source_download_timestamp  timestamp
        ,source_load_timestamp      timestamp
        ,target_lineage_note        text
        ,target_station_id          text        NOT NULL
        ,target_timestamp           timestamp   NOT NULL
        ,load_batch_ts              timestamp
        ,update_batch_ts            timestamp
        )
        ''')
        conn.commit()
        logging.info('Successfully created and committed dr_wsu_lineage')
    except Exception as e:
        logging.warning('Failed to create dr_wsu_weather, following error thrown')
        logging.warning(str(e))


    cur.close()
    conn.close()

### Create data refinery files for each station
def create_data_refineries(sl_file,dr_folder,overwrite=False):
    # Cycle through station list creating data bases
    stat_list = pd.read_csv(sl_file)
    for this_stat in stat_list.loc[:, 'station_id']:
        this_stat = str(this_stat)
        logging.info('Creating data lake for ' + this_stat)
        this_stat = str(this_stat)
        logging.info('Loading data lake for ' + this_stat)
        # Create data lake
        db_fullpath = os.path.join(dr_folder, ('dr_' + this_stat + '.db'))
        create_dr_db(db_fullpath=db_fullpath, overwrite=overwrite)

### Load data from data lake into data refinery for a single station
def load_wsu_weather_dl_to_dr(this_stat, dl_folder,working_folder, dr_folder, write_csv=False):

    this_stat = str(this_stat)

    logging.debug('start load_wsu_weather_dl_to_dr fx for ' + this_stat)
    working_db = os.path.join(working_folder,('wrk_'+this_stat+'.db'))
    dl_db = os.path.join(dl_folder,('dl_'+this_stat+'.db'))
    dr_db = os.path.join(dr_folder,('dr_'+this_stat+'.db'))

    ### Preamble: Copy data lake to a working db.

    ## Copy dl_db to working_db
    logging.info('Copying data lake to working database')
    if os.path.isfile(working_db):
        os.remove(working_db)
    shutil.copy(dl_db, working_db)

    ## Open connection to working_db
    w_conn = sqlite3.connect(working_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    w_cur = w_conn.cursor()

    ## Get a list of station_id's from data lake

    ### Load data from a station_id and do QC transformations
    logging.info('Loading dl_wsu_weather data for station_id ' + this_stat)
    thissql = 'SELECT * FROM dl_wsu_weather WHERE station_id = \'' + this_stat + '\''
    wsu_tab = pd.read_sql_query(thissql, con=w_conn)

    ## Create lineage cols
    lineage_cols = ['source_filename', 'source_rownum', 'source_download_timestamp', 'source_load_timestamp'
        , 'station_id', 'timestamp']

    logging.debug('wsu_tab:')
    logging.debug(wsu_tab.head())
    logging.debug('len(wsu_tab)' + str(len(wsu_tab)))
    logging.debug('wsu_tab dtypes: ' + str(wsu_tab.dtypes))

    ### Section 1: Transformations to create new columns and replace values with nulls as appropriate

    ## Reset station name: just pick one, perhaps the latest one
    this_stat_name = wsu_tab.loc[len(wsu_tab.index)-1, 'station_name']
    logging.info('Setting station name in wsu_tab to ' + this_stat_name)
    wsu_tab.loc[:, 'station_name'] = this_stat_name

    ## Reset location name: just pick one, perhaps the latest one

    this_loc_name = wsu_tab.loc[len(wsu_tab.index)-1,'location']
    logging.info('Setting station location in wsu_tab to ' + this_loc_name)
    wsu_tab.loc[:, 'location'] = this_loc_name

    ## Create a time_pt and timezone column
    logging.info('Creating time_pt and timezone')
    bool_col = wsu_tab['time_pdt'].notnull() & wsu_tab['time_pst'].isnull()
    wsu_tab.loc[bool_col, 'time_pt'] = wsu_tab.loc[bool_col, 'time_pdt']
    wsu_tab.loc[bool_col, 'timezone'] = 'PDT'

    bool_col = wsu_tab['time_pst'].notnull() & wsu_tab['time_pdt'].isnull()
    wsu_tab.loc[bool_col, 'time_pt'] = wsu_tab.loc[bool_col, 'time_pst']
    wsu_tab.loc[bool_col, 'timezone'] = 'PST'

    ## Find rows with 24:00 and turn the date forward by one day
    logging.info('Turning 24:00 hour rows forward a day')
    a = wsu_tab.loc[wsu_tab['time_pt'] == '24:00', 'date']
    c = pd.to_datetime(a)
    c2 = c + timedelta(days=1)
    c2 = c2.dt.strftime(date_format="%Y-%m-%d")
    wsu_tab.loc[wsu_tab.loc[:, 'time_pt'] == '24:00', 'date'] = c2

    ## Go back to those same rows with 24:00 and change to 00:00
    wsu_tab.loc[wsu_tab.loc[:, 'time_pt'] == '24:00', 'time_pt'] = '00:00'

    ## Create PT timestamp column.
    logging.info('Creating PT timestamp')
    dt = wsu_tab.loc[:, 'date'] + ' ' + wsu_tab.loc[:, 'time_pt']
    wsu_tab.loc[:, 'timestamp'] = pd.to_datetime(dt, errors='coerce')

    ## Change PDT timestamps to PST by subtracting an hour. Remove now redundant columns
    logging.info('Converting PDT to PST')
    bool_col = wsu_tab['timezone'] == 'PDT'
    wsu_tab.loc[bool_col, 'timestamp'] = wsu_tab.loc[bool_col, 'timestamp'] - timedelta(hours=1)
    wsu_tab.loc[bool_col, 'timezone'] = 'PST'
    del wsu_tab['time_pt']
    del wsu_tab['time_pdt']
    del wsu_tab['time_pst']
    del wsu_tab['date']

    ## Remove duplicate rows and rows without proper dates or station_ids
    logging.info('Removing duplicate rows and those with no timestamps')
    wsu_tab.dropna(subset=['timestamp'], inplace=True)
    wsu_tab.dropna(subset=['station_id'], inplace=True)
    wsu_tab.drop_duplicates(inplace=True)

    ## Split wind speed into magnitude and direction
    logging.info('Splitting wind_dir_at_speed_mph into wind_dir and wind_speed_mph')
    wsu_tab['wind_dir'], wsu_tab['wind_speed_mph'] = wsu_tab['wind_dir_at_speed_mph'].str.split(pat='@', n=1).str
    wsu_tab['wind_speed_mph'] = wsu_tab['wind_speed_mph'].astype(float)
    del wsu_tab['wind_dir_at_speed_mph']

    ## Convert unreasonable wind speeds to null. Use twice the max recorded speed (https://en.wikipedia.org/wiki/Wind_speed#Highest_speed), 322 mph * 2 = 644
    logging.info('Nulling unreasonably high wind speeds')
    wsu_tab.loc[wsu_tab['wind_gust_mph'] > 644, 'wind_gust_mph'] = None
    wsu_tab.loc[wsu_tab['wind_speed_mph'] > 644, 'wind_speed_mph'] = None

    ## Relative humidity of exactly zero is pretty much impossible on Earth. The lowest ever recorded as 1% (http://articles.chicagotribune.com/2011-12-16/news/ct-wea-1216-asktom-20111216_1_relative-humidity-zero-dew-point) Sp, convert them to nulls.
    logging.info('Nulling zero humidity values')
    wsu_tab.loc[wsu_tab['rel_humidity_perc'] == 0, 'rel_humidity_perc'] = None

    ## Wind direction should be one of the compass point abbreviations. If it is not, set it as null as well as the matching wind speed
    logging.info('Nulling weird wind directions')
    dir_list = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE', 'NNE', 'ENE', 'ESE', 'SSE', 'SSW', 'WSW', 'WNW', 'NNW']
    dir_angle = [0, 270 + 45, 270, 180 + 45, 180, 90 + 45, 90, 45, 22.5, 45 + 22.5, 90 + 22.5, 180 - 22.5,
                 180 + 22.5,
                 270 - 22.5, 270 + 22.5, 360 - 22.5]
    dir_tbl = pd.DataFrame(data={'wind_dir': dir_list, 'dir_angle': dir_angle})
    non_dir_bool = ~wsu_tab['wind_dir'].isin(dir_list)
    wsu_tab.loc[non_dir_bool, 'wind_dir'] = None
    wsu_tab.loc[non_dir_bool, 'wind_speed_mph'] = None

    ## If every data column in a row is null except for total_precip_inch and solar_rad_watts_per_meter_squared, set those two to null as well
    logging.info('Nulling total_precip_inch, solar_rad_watts_per_meter_squared if other data null')
    nulling_data_cols = wsu_tab.columns.values.tolist()
    takeout = ['station_id',
               'timestamp',
               'timezone',
               'location',
               'station_name',
               'source_filename',
               'source_rownum',
               'source_download_timestamp',
               'source_load_timestamp',
               'total_precip_inch',
               'solar_rad_watts_per_meter_squared']
    nulling_data_cols = [x for x in nulling_data_cols if x not in takeout]
    row_bool = True
    for thisCol in nulling_data_cols:
        row_bool = (wsu_tab[thisCol].isnull() & row_bool)
    wsu_tab.loc[row_bool, 'total_precip_inch'] = None
    wsu_tab.loc[row_bool, 'solar_rad_watts_per_meter_squared'] = None

    ## Open connection to data refinery, read in timestamps, then close connection
    dr_conn = sqlite3.connect(dr_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    dr_cur = dr_conn.cursor()
    thissql = 'SELECT DISTINCT timestamp FROM dr_wsu_weather WHERE station_id = \'' + this_stat + '\''
    ts_filter_out = pd.read_sql_query(sql=thissql, con=dr_conn)
    dr_cur.close()
    dr_conn.close()

    ## Exclude timestamps from wsu_tab that are the same station and present in dr_wsu_weather
    ts_filter_out['exclude'] = True
    wsu_tab = pd.merge(wsu_tab, ts_filter_out, on='timestamp', how='left')
    logging.info('station_id ' + this_stat + ' has ' + str(wsu_tab.loc[:, 'timestamp'].count()) + ' rows of data')
    logging.info(str(wsu_tab.loc[:, 'exclude'].count()) + ' of which will be excluded')
    wsu_tab.loc[:, 'timestamp'].count()
    keep_idx = wsu_tab.loc[:, 'exclude'].isnull()
    wsu_tab = wsu_tab.loc[keep_idx, :]
    wsu_tab.drop(labels='exclude', axis=1, inplace=True)
    logging.info('station_id ' + this_stat + ' has ' + str(wsu_tab.loc[:,
                                                           'timestamp'].count()) + ' rows of data remaining after removing timestamps that exist already in data refinery')

    ### Section 2: The primary key of the work table is station_id and timestamp. This section
    ### will process the weather data to collapse duplicates for station_id and timestamp

    ## Mark rows that are duplicate when looking at station_id and timestamp
    logging.info('Marking duplicate station_id and timestamp rows')
    dup_bool = wsu_tab.duplicated(subset=['station_id', 'timestamp'], keep=False)

    ## Get rows that are not duplicated, and insert the data and lineage into database tables
    ## Since this is our first insert, we should replace table
    logging.info('There are ' + str(len(wsu_tab.index)) + ' rows in wsu_tab')
    logging.info('Start separation of rows with just one row per station_id and timestamp')
    singles = (wsu_tab.copy()).loc[~dup_bool, :]
    logging.info(str(len(singles.index)) + ' of ' + str(len(wsu_tab.index)) + ' rows are unique')
    nonaudit_cols = wsu_tab.columns.values.tolist()
    takeout = ['source_filename',
               'source_rownum',
               'source_download_timestamp',
               'source_load_timestamp',
               'aggregation_note']
    nonaudit_cols = [x for x in nonaudit_cols if x not in takeout]
    logging.debug('nonaudit_cols:')
    logging.debug(str(nonaudit_cols))
    singles_lin = singles.loc[:, lineage_cols]
    singles_dat = singles.loc[:, nonaudit_cols]
    singles_lin['aggregation_note'] = 'Non duplicated row, straight copy'
    singles_lin.to_sql(name='wrk_lineage', con=w_conn, if_exists='replace', index=False)
    singles_dat.to_sql(name='wrk_load_wsu_weather', con=w_conn, if_exists='replace', index=False)
    w_conn.commit()
    logging.info(
        'Created wrk_lineage and wrk_load_wsu_weather tables and inserted data without duplicate station_id and timestamp')

    ## Close up connections and run load of working database into data refinery. Then, reopen connection to working database and continue
    w_conn.commit()
    w_cur.close()
    w_conn.close()
    update_dr_wsu_weather(working_db, dr_db)
    logging.info('Update data refinery')
    w_conn = sqlite3.connect(working_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    w_cur = w_conn.cursor()

    ## Data consolidate two: data unique rows - dump as a set into refinery

    ## Get remaining duplicate rows
    logging.info(
        'Removing rows that have already been written to database from further examination, then separate lineage columns')
    rem = wsu_tab.copy().loc[dup_bool, :]

    rem_lin = rem.loc[:, lineage_cols]
    rem_lin['aggregation_note'] = 'More than one row per station_id, timestamp'
    rem_dat = rem.loc[:, nonaudit_cols]
    logging.info(str(len(rem_dat.index)) + ' rows of data remaining')
    del wsu_tab
    logging.debug('Rows in rem: ' + str(len(rem.index)))
    ## Find data only duplicates and write out to database
    logging.info('Drop duplicates in remaining rows after removing lineage columns')
    rem_dat.drop_duplicates(inplace=True)
    dup_bool_l2 = rem_dat.duplicated(subset=['station_id', 'timestamp'], keep=False)
    logging.info('Finding unique records in non-lineage columns and then writing out')
    singles_dat = rem_dat.loc[~dup_bool_l2, :]

    logging.debug('After singles_dat')
    logging.debug('Rows in singles dat: ' + str(len(singles_dat.index)))
    # If there are single rows found, go for it, otherwise go on
    if len(singles_dat.index) > 0:
        singles_ts_list = pd.DataFrame(singles_dat.loc[:, 'timestamp'].unique(), columns=['timestamp'])
        logging.debug('post singles_ts_list')
        singles_ts_list['include'] = True
        logging.debug('before merge')
        logging.debug(str(singles_ts_list))
        logging.debug(str(rem_lin))
        singles_lin = pd.merge(rem_lin, singles_ts_list, on='timestamp', how='inner')
        logging.debug('post merge')
        singles_lin.drop(labels='include', axis=1, inplace=True)
        logging.debug('after drop')
        a = singles_dat.duplicated(subset=['station_id', 'timestamp'], keep=False)
        if a.any():
            logging.warning('There are still duplicates in singles_dat')
            of = os.path.join(working_folder,this_stat + '_duplicates.csv')
            logging.warning('Writing out a to ' + str(of))
            a.to_csv(of,index=False, encoding='utf-8')
            of = os.path.join(working_folder, this_stat + '_data_with_duplicates.csv')
            logging.warning('Writing out singles_dat to ' + str(of))
            singles_dat.to_csv(of, index=False, encoding='utf-8')
            raise Exception('There are still duplicates in singles_dat!')
        else:
            logging.info('Ready to write to working database in ' + working_db)
            singles_lin.to_sql(name='wrk_lineage', con=w_conn, if_exists='replace', index=False)
            singles_dat.to_sql(name='wrk_load_wsu_weather', con=w_conn, if_exists='replace', index=False)
        ## Close up connections and run load of working database into data refinery. Then, reopen connection to working database and continue
        w_conn.commit()
        w_cur.close()
        w_conn.close()
        update_dr_wsu_weather(working_db, dr_db)
        logging.info('Updated data refinery')
        w_conn = sqlite3.connect(working_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        w_cur = w_conn.cursor()

        ## Data consolidate three: go through data station_id, timestamp, and column. After each station_id, timestamp, upsert load into data refinery

        ## Get remaining rows and process by station_id, timestamp, and column
        logging.debug('Remove rows that we wrote out')
        ts_filter_out = pd.DataFrame(singles_dat.loc[:, 'timestamp'].unique(), columns=['timestamp'])
        ts_filter_out['exclude'] = True
        rem = pd.merge(rem, ts_filter_out, on='timestamp', how='left')
        keep_idx = rem.loc[:, 'exclude'].isnull()
        rem = rem.loc[keep_idx, :]
        rem.drop(labels='exclude', axis=1, inplace=True)
        logging.info('station_id ' + this_stat + ' has ' + str(rem.loc[:,
                                                               'timestamp'].count()) + ' rows of data remaining after removing timestamps that we just wrote out')

    # Ex data cols is a list of all data columns except for the wind dir and speed, which will be examined separately
    logging.info('Start processing remaining data by column')
    ex_data_cols = rem.columns.values.tolist()
    takeout = ['station_id',
               'timestamp',
               'location',
               'station_name',
               'timezone',
               'source_filename',
               'source_rownum',
               'source_download_timestamp',
               'source_load_timestamp',
               'wind_dir',
               'wind_speed_mph']
    ex_data_cols = [x for x in ex_data_cols if x not in takeout]

    # Start cycling through timestamps
    ts_list = pd.to_datetime(rem.loc[:,'timestamp'].unique(),errors='coerce')
    logging.debug('rem: ' + str(rem))
    logging.info(str(len(ts_list)) + ' timestamps to go through')
    logging.debug('ts_list: ' + str(ts_list))
    for this_ts in ts_list:
        logging.debug(this_ts)
        logging.debug('pd.to_datetime(this_ts, errors=\'coerce\'): ')
        a = pd.to_datetime(this_ts, errors='coerce')
        logging.debug(str(a))
        logging.debug('rem[\'timestamp\'] == this_ts: ')
        logging.debug(str(rem['timestamp'] == this_ts))
        bool_list = (rem['timestamp'] == this_ts)
        temp_agg_note = 'column by column: '
        # Go through each column, decide on the right value, then populate it in all the rows of the temp table
        for this_col in ex_data_cols:
            logging.debug(this_col)
            dat_series = rem.loc[bool_list, this_col]
            dat_series = dat_series[dat_series.notnull()]
            dat_mean = dat_series.mean()
            dat_mean_upper = dat_mean + (0.1 * abs(dat_mean))
            dat_mean_lower = dat_mean - (0.1 * abs(dat_mean))
            in_range = dat_series.between(dat_mean_lower, dat_mean_upper).all()
            ## If all but one values are null, use the non-null value
            if dat_series.count() == 1:
                logging.debug('in single non null found')
                logging.debug('dat_series:')
                logging.debug(str(dat_series))
                rem.loc[bool_list, this_col] = dat_series.mean()
                temp = ', found single non-null in ' + this_col
                temp_agg_note += temp
            ## If all the values are within 10% of the average, use the average
            elif len(dat_series) > 1 & in_range:
                rem.loc[bool_list, this_col] = dat_mean
                temp = ', mean used for ' + this_col
                temp_agg_note += temp
            else:
                ## Lastly, if we cannot resolve, make then null
                temp = ', null filled in for ' + this_col
                temp_agg_note += temp
                rem.loc[bool_list, this_col] = None

        # Handle conflicting wind speed and directions
        logging.debug('Start handling wind speed and directions')
        wind_temp = rem.loc[bool_list, ['wind_dir', 'wind_speed_mph']]
        wind_temp = wind_temp.loc[wind_temp['wind_dir'].notnull(), :]
        dir_l = wind_temp['wind_dir'].unique()
        # If there is only one direction, average the speeds if they are within 10% of the average. Otherwise,
        # fill with nulls
        win_series = wind_temp['wind_speed_mph']
        wind_mean = win_series.mean()
        wind_mean_upper = wind_mean + (0.1 * abs(wind_mean))
        wind_mean_lower = wind_mean - (0.1 * abs(wind_mean))
        in_range = win_series.between(wind_mean_lower, wind_mean_upper).all()
        if len(dir_l) == 1 & in_range:
            rem.loc[bool_list, 'wind_speed_mph'] = wind_mean
            rem.loc[bool_list, 'wind_dir'] = dir_l
            temp = ', mean used for wind_speed_mph'
            temp_agg_note += temp
        else:
            rem.loc[bool_list, 'wind_speed_mph'] = None
            rem.loc[bool_list, 'wind_dir'] = None
            temp = ', null filled in for wind_dir and wind_speech_mph'
            temp_agg_note += temp

        # Write out temporary data stuff and lineage stuff
        temp_data = rem.copy().loc[bool_list, :]
        temp_data.drop(labels='source_filename', axis=1, inplace=True)
        temp_data.drop(labels='source_rownum', axis=1, inplace=True)
        temp_data.drop(labels='source_download_timestamp', axis=1, inplace=True)
        temp_data.drop(labels='source_load_timestamp', axis=1, inplace=True)
        temp_data.drop_duplicates(keep='first', inplace=True)
        a = temp_data.duplicated(subset=['station_id', 'timestamp'], keep=False)
        if a.any():
            logging.warning('There are still duplicates in temp_data')
            of = os.path.join(working_folder, this_stat + '_temp_data_duplicates.csv')
            logging.warning('Writing out a to '+str(of))
            a.to_csv(of, index=False, encoding='utf-8')
            of = os.path.join(working_folder, this_stat + '_data_with__temp_data_duplicates.csv')
            logging.warning('Writing out temp_data to ' + str(of))
            temp_data.to_csv(of, index=False, encoding='utf-8')
            raise Exception('There are still duplicates in temp_data!')

        # Write out lineage stuff
        logging.debug('temp_agg_note: ')
        logging.debug(str(temp_agg_note))
        rem_lin_temp = rem.copy().loc[bool_list, lineage_cols]
        rem_lin_temp['aggregation_note'] = temp_agg_note
        rem_lin_temp.to_sql(name='wrk_lineage', con=w_conn, if_exists='replace', index=False)
        logging.debug('Wrote lineage info out to wrk_lineage')

        # Write out temporary data stuff
        temp_data.to_sql(name='wrk_load_wsu_weather', con=w_conn, if_exists='replace', index=False)
        logging.debug('Wrote out data to wrk_load_wsu_weather')

        ## Close up connections and run load of working database into data refinery. Then, reopen connection to working database and continue
        w_conn.commit()
        w_cur.close()
        w_conn.close()
        update_dr_wsu_weather(working_db, dr_db)
        logging.debug('Updated data refinery' + dr_db)
        w_conn = sqlite3.connect(working_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        w_cur = w_conn.cursor()

    logging.info('Dropping dl_wsu_weather from working database')
    w_cur.execute('DROP TABLE dl_wsu_weather')
    w_conn.commit()
    w_cur.close()
    w_conn.close()
    ## Write out data refinery to file
    if write_csv:
        dr_conn = sqlite3.connect(dr_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        dr_cur = dr_conn.cursor()
        dr_dt = pd.read_sql_query('SELECT * FROM dr_wsu_weather', con=dr_conn)
        dr_dt.to_csv(dr_db + ".csv", index=False, encoding='utf-8')
        dr_cur.close()
        dr_conn.close()

    logging.info('Closed out of database, exiting')



### Load working table into dr_wsu_weather using an upsert pattern
def update_dr_wsu_weather(working_db, dr_db):
    logging.debug('Starting update_dr_wsu_weather')

    ## Open connection to working_db
    logging.debug('Opening database connections')
    w_conn = sqlite3.connect(working_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    w_cur = w_conn.cursor()

    ## Open connection to data refinery
    dr_conn = sqlite3.connect(dr_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    dr_cur = dr_conn.cursor()

    ## load wrk_lineage into data refinery
    logging.debug('Loading wrk_lineage into ' + dr_db)
    wrk_lineage = pd.read_sql_query('SELECT * FROM wrk_lineage', con=w_conn)
    wrk_lineage.to_sql(name='wrk_lineage', con=dr_conn, if_exists='replace', index=False)

    dr_conn.commit()

    ## load wrk_load_wsu_weather into data refinery
    logging.debug('Loading wrk_load_wsu_weather from ' + working_db)
    wrk_load_wsu_weather = pd.read_sql_query('SELECT * FROM wrk_load_wsu_weather', con=w_conn)
    wrk_load_wsu_weather.to_sql(name='wrk_load_wsu_weather', con=dr_conn, if_exists='replace', index=False)
    logging.debug('Wrote wrk_load_wsu_weather back into ' + dr_db)
    # wrk_load_wsu_weather = pd.read_sql_query('SELECT * FROM wrk_load_wsu_weather', con=w_conn)

    ## Close connection to working db
    w_cur.close()
    w_conn.close()
    logging.debug('Closed connection to ' + working_db)

    ### Remove hashing, since it takes too long
    ## load wrk_load_wsu_weather into data refinery with hash values
    # logging.info('Start calculating hash values')
    # row_cnt = max(wrk_load_wsu_weather.count())
    # check_list = [0.00001, 0.0001, 0.001,0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    # check_list = [x * row_cnt for x in check_list]
    # check_list = list(map(int, check_list))
    #
    # for row in wrk_load_wsu_weather.itertuples():
    #     #logging.debug('row: ' + str(row))
    #     if row[0] in check_list:
    #         logging.debug('Now on row ' + str(row[0]) + ' of ' + str(row_cnt))
    #     a = ''.join(str(row)).encode('utf-8')
    #     ho = hashlib.sha1(a)
    #     hh = ho.hexdigest()
    #     #logging.debug('hexdigest: ' + hh)
    #     wrk_load_wsu_weather.loc[row[0], 'hash_hex'] = hh
    # wrk_load_wsu_weather.to_sql(name='wrk_load_wsu_weather', con=dr_conn, if_exists='replace', index=False)
    # logging.info('Wrote wrk_load_wsu_weather back into ' + dr_db)
    # del wrk_load_wsu_weather
    # logging.info('Deleted wrk_load_wsu_weather data frame')

    ## Start loading


    logging.debug('Start loading wrk_load_wsu_weather into dr_wsu_weather')
    # Create replica
    logging.debug('Creating replica')
    dr_cur.execute('''
    CREATE TEMPORARY TABLE wrk_replica_dr_wsu_weather AS
    SELECT *
    FROM dr_wsu_weather
    LIMIT 0
    ''')

    # Compare work table with final target
    logging.debug('Comparing load with final target')
    up_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dr_cur.execute('''
    CREATE TEMPORARY TABLE wrk_main_load_dr_wsu_weather AS
    SELECT
        src.station_id
        ,src.timestamp
        ,src.location
        ,src.station_name
        ,src.air_temp_f
        ,src.second_air_temp_f
        ,src.dew_point_f
        ,src.rel_humidity_perc
        ,src.leaf_wet_u
        ,src.wind_dir
        ,src.wind_speed_mph
        ,src.wind_gust_mph
        ,src.bed_temp_f
        ,src.two_inch_soil_temp_f
        ,src.eight_inch_soil_temp_f
        ,src.soil_vwc_perc
        ,src.total_precip_inch
        ,src.solar_rad_watts_per_meter_squared
        ,src.atm_pressure_in_Hg
        ,COALESCE(tgt.load_batch_ts,?)  AS load_batch_ts
        ,?  AS  update_batch_ts
    FROM wrk_load_wsu_weather src
    LEFT JOIN dr_wsu_weather tgt
        ON src.station_id = tgt.station_id
        AND src.timestamp = tgt.timestamp
    ''', (up_ts, up_ts))

    # Load into replica
    logging.debug('Loading wrk_replica_dr_wsu_weather')
    dr_cur.execute('''
    INSERT INTO wrk_replica_dr_wsu_weather(
        station_id
        ,timestamp
        ,location
        ,station_name
        ,air_temp_f
        ,second_air_temp_f
        ,dew_point_f
        ,rel_humidity_perc
        ,leaf_wet_u
        ,wind_dir
        ,wind_speed_mph
        ,wind_gust_mph
        ,bed_temp_f
        ,two_inch_soil_temp_f
        ,eight_inch_soil_temp_f
        ,soil_vwc_perc
        ,total_precip_inch
        ,solar_rad_watts_per_meter_squared
        ,atm_pressure_in_Hg
        ,load_batch_ts
        ,update_batch_ts
    )
    SELECT
        station_id
        ,timestamp
        ,location
        ,station_name
        ,air_temp_f
        ,second_air_temp_f
        ,dew_point_f
        ,rel_humidity_perc
        ,leaf_wet_u
        ,wind_dir
        ,wind_speed_mph
        ,wind_gust_mph
        ,bed_temp_f
        ,two_inch_soil_temp_f
        ,eight_inch_soil_temp_f
        ,soil_vwc_perc
        ,total_precip_inch
        ,solar_rad_watts_per_meter_squared
        ,atm_pressure_in_Hg
        ,load_batch_ts
        ,update_batch_ts
    from wrk_main_load_dr_wsu_weather


    ''')

    # Delete from final target rows
    logging.debug('Deleting rows from final target')
    dr_cur.execute('''
    DELETE FROM dr_wsu_weather
    WHERE rowid IN (
        SELECT tgt.rowid
        FROM wrk_replica_dr_wsu_weather rep
        INNER JOIN dr_wsu_weather tgt
            ON rep.station_id = tgt.station_id
            AND rep.timestamp = tgt.timestamp
    )
    ''')
    dr_conn.commit()
    # See what's up with failing unique constriant

    replica = pd.read_sql('SELECT * FROM wrk_replica_dr_wsu_weather', con=dr_conn)

    a = replica.duplicated(subset=['station_id', 'timestamp'], keep=False)
    err = replica.loc[a, :]
    err.to_csv('duplicates from wrk_replica_dr_wsu_weather.csv')

    wrk_lineage = pd.read_sql('SELECT * FROM wrk_lineage', con=dr_conn)
    lin = pd.merge(left=err, right=wrk_lineage, how='inner', on=['station_id', 'timestamp'], copy=True)
    lin.to_csv('lineage of duplicates from wrk_replica_dr_wsu_weather.csv')

    # Delete from final target rows
    logging.debug('Insert rows from replica into final target')
    dr_cur.execute('''
    INSERT INTO dr_wsu_weather
    (
        station_id
        ,timestamp
        ,location
        ,station_name
        ,air_temp_f
        ,second_air_temp_f
        ,dew_point_f
        ,rel_humidity_perc
        ,leaf_wet_u
        ,wind_dir
        ,wind_speed_mph
        ,wind_gust_mph
        ,bed_temp_f
        ,two_inch_soil_temp_f
        ,eight_inch_soil_temp_f
        ,soil_vwc_perc
        ,total_precip_inch
        ,solar_rad_watts_per_meter_squared
        ,atm_pressure_in_Hg
        ,load_batch_ts
        ,update_batch_ts
    )
    SELECT
        station_id
        ,timestamp
        ,location
        ,station_name
        ,air_temp_f
        ,second_air_temp_f
        ,dew_point_f
        ,rel_humidity_perc
        ,leaf_wet_u
        ,wind_dir
        ,wind_speed_mph
        ,wind_gust_mph
        ,bed_temp_f
        ,two_inch_soil_temp_f
        ,eight_inch_soil_temp_f
        ,soil_vwc_perc
        ,total_precip_inch
        ,solar_rad_watts_per_meter_squared
        ,atm_pressure_in_Hg
        ,load_batch_ts
        ,update_batch_ts
    FROM wrk_replica_dr_wsu_weather
    ''')

    ## Commit
    dr_conn.commit()
    logging.debug('Committed changes')

    ### Start loading wrk_lineage into dr_wsu_lineage

    logging.debug('Start loading wrk_lineage into dr_wsu_lineage')

    # Create replica
    logging.debug('Creating replica')
    dr_cur.execute('''
    CREATE TEMPORARY TABLE wrk_replica_dr_wsu_lineage AS
    SELECT *
    FROM dr_wsu_lineage
    LIMIT 0
    ''')

    # Create main load table
    logging.debug('Create main load table')
    up_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dr_cur.execute('''
    CREATE TEMPORARY TABLE wrk_main_load_dr_wsu_lineage AS
    SELECT
        source_filename
        ,source_rownum
        ,source_download_timestamp
        ,source_load_timestamp
        ,aggregation_note   AS  target_lineage_note
        ,station_id     AS  target_station_id
        ,timestamp      AS  target_timestamp
        ,?  AS  load_batch_ts
        ,?  AS  update_batch_ts
    FROM wrk_lineage src
    ''', (up_ts, up_ts))

    # Load into replica
    logging.debug('Loading wrk_replica_dr_wsu_lineage')
    dr_cur.execute('''
    INSERT INTO wrk_replica_dr_wsu_lineage(
        source_filename
        ,source_rownum
        ,source_download_timestamp
        ,source_load_timestamp
        ,target_lineage_note
        ,target_station_id
        ,target_timestamp
        ,load_batch_ts
        ,update_batch_ts
    )
    SELECT
        source_filename
        ,source_rownum
        ,source_download_timestamp
        ,source_load_timestamp
        ,target_lineage_note
        ,target_station_id
        ,target_timestamp
        ,load_batch_ts
        ,update_batch_ts
    from wrk_main_load_dr_wsu_lineage


    ''')

    # Delete from final target rows
    logging.debug('Deleting rows from final target')
    dr_cur.execute('''
    DELETE FROM dr_wsu_lineage
    WHERE rowid IN (
        SELECT tgt.rowid
        FROM wrk_replica_dr_wsu_lineage rep
        INNER JOIN dr_wsu_lineage tgt
            ON rep.target_station_id = tgt.target_station_id
            AND rep.target_timestamp = tgt.target_timestamp
    )
    ''')

    # Insert into final target rows
    logging.debug('Insert rows from replica into final target')
    dr_cur.execute('''
    INSERT INTO dr_wsu_lineage
    (
        source_filename
        ,source_rownum
        ,source_download_timestamp
        ,source_load_timestamp
        ,target_lineage_note
        ,target_station_id
        ,target_timestamp
        ,load_batch_ts
        ,update_batch_ts
    )
    SELECT
        source_filename
        ,source_rownum
        ,source_download_timestamp
        ,source_load_timestamp
        ,target_lineage_note
        ,target_station_id
        ,target_timestamp
        ,load_batch_ts
        ,update_batch_ts
    FROM wrk_replica_dr_wsu_lineage
    ''')

    ## Commit
    dr_conn.commit()
    logging.debug('Committed changes')

    ## Close database connection
    dr_cur.close()
    dr_conn.close()
    logging.debug('Closed database connections, exiting function')
    return None
