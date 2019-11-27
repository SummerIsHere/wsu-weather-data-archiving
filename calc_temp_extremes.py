#! python3

### This file contains functions related to calculating the 1st and 99th percentiles of annual temperatures
### from the WSU weather data set

### Import statements
import os, shutil, sqlite3, pandas as pd, logging, numpy as np,
from datetime import timedelta, datetime


### Load working table into dr_wsu_weather using an upsert pattern
def get_temp_extremes(dr_db,outfile):
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
    logging.debug('Comparing load with final targett')
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
