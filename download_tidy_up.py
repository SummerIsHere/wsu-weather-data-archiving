#! python3

### This file contains functions related to downloading data from public web sources, minimally transforming that data,
### and saving them as text .csv files


import os, ftplib, pandas as pd, requests, logging, bs4, re, shutil, time, sqlite3, zipfile as zf
from datetime import datetime, timedelta, date
from pandas_datareader import wb
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

### Get CO2 Concentration Data Collected in Mauna Loa, Hawaii, USA from NOAA
def get_CO2_conc(output_folder):
    logging.info('Downloading CO2 concentration data from NOAA')

    ## Step 1: Download data file from NOAA ftp site
    base = 'aftp.cmdl.noaa.gov'
    folder = 'products/trends/co2/'
    filebase = 'co2_mm_mlo'
    raw_file = filebase + '.txt'
    raw_file = os.path.join(output_folder, raw_file)
    ftp = ftplib.FTP(base)
    ftp.login()
    ftp.cwd(folder)
    #ftp.retrlines('LIST')
    ftp.retrbinary('RETR co2_mm_mlo.txt', open(raw_file, 'wb').write)
    ftp.quit()


    ## Step 2: Read downloaded file back into a pandas Data Frame

    # Find the last comment line (denoted by #). This line and the line before it will be the column headers. We won't
    # directly import them but we will check that these lines are what we expect
    textfile = open(raw_file, 'r')
    lines = textfile.readlines()
    j = []
    for i in range(0,len(lines),1):
        if '#' in lines[i]:
            j.append(i)
    j = max(j)
    if (lines[j-1]!='#            decimal     average   interpolated    trend    #days\n'
        or lines[j]!='#             date                             (season corr)\n' ):
        raise Exception('Column headers not as expected in ' + raw_file)

    # Start import using space delimiter
    conc = pd.read_csv(raw_file, delimiter='\s+', header=None, skiprows=j+1,
                names=['Year','Month','Decimal Date','Avg ppm',
                       'Interpolated ppm', 'Seasonally Adjusted ppm',
                       '# Daily Means'])

    ## Step 3: Write back out as a .csv file
    outfile = os.path.join(output_folder,'tidied_data_CO2_ppm.csv')
    conc.to_csv(outfile, index=False, encoding='utf-8')
    textfile.close()
    logging.info('Saved tidied data to ' + outfile)

### Download HadCRUT data from the UK Met Office
### This url goes to monthly time series data for ensemble medians and uncertainties about global tempeature
### deviation from the 1961-1990 average
def get_global_temp_data(output_folder):
    logging.info('Downloading global temperature data from UK Met')

    ## Step 1: Download data file from UK Met
    url = 'http://www.metoffice.gov.uk/hadobs/hadcrut4/data/current/time_series/HadCRUT.4.6.0.0.monthly_ns_avg.txt'
    raw_file = os.path.join(output_folder, 'HadCRUT.4.6.0.0.monthly_ns_avg.txt')
    pagey = requests.get(url)
    pagey.raise_for_status()
    open(raw_file, 'w+').write(pagey.text)

    ## Step 2: Read in downloaded file as a pandas Data Frame
    HadCRUT = pd.read_csv(raw_file, delimiter='\s+', header=None,
                         names=['DateText', 'Median Global Temp C Deviation',
                                'Bias Uncertainty (Lower 95 CI)', 'Bias Uncertainty (Upper 95 CI)',
                                'Measurement and Sampling Uncertainty (Lower 95 CI)',
                                'Measurement and Sampling Uncertainty (Upper 95 CI)',
                                'Coverage Uncertainty (Lower 95 CI)',
                                'Coverage Uncertainty (Upper 95 CI)',
                                'Measurement, Sampling, and Bias Uncertainty (Lower 95 CI)',
                                'Measurement, Sampling, and Bias Uncertainty (Upper 95 CI)',
                                'Combined Uncertainty (Lower 95 CI)',
                                'Combined Uncertainty (Upper 95 CI)'
                                ])

    a = HadCRUT.loc[:, 'DateText'].apply(lambda x: datetime.strptime(x, '%Y/%m'))
    HadCRUT.insert(0, 'Date', a)
    logging.debug(HadCRUT.head())

    ## Step 3: Save data frame to file
    outfile = os.path.join(output_folder, 'tidied_data_HadCRUT_global_temperature_anomalies.csv')
    HadCRUT.to_csv(outfile, index=False, encoding = 'utf-8')
    logging.info('Saved tidied global temp data to ' + outfile)

### Download summary file of energy production data from the International Energy Agency (IEA)
def get_iea_data(output_folder):
    ## Step 1: Find the correct excel file and download it from the IEA site
    # Get the webpage and scrape it for the correct link to the excel file
    logging.info('Downloading IEA headline energy data')
    title = 'IEA Headline Energy Data - excel file'
    url = 'https://www.iea.org/statistics'
    pagey = requests.get(url)
    pagey.raise_for_status()
    parsey = bs4.BeautifulSoup(pagey.text, 'html5lib')
    elem = ''.join(['a[title="', title, '"]'])
    a = parsey.select(elem)
    # b = parsey.findAll('a', text=link_text)

    # There could be multiple matches. Find the first valid one and download it
    for i in a:
        excel_link1 = i.get('href')
        excel_link2 = ''.join(['https://www.iea.org', i.get('href')])
        try:
            # Try version 1 of link i
            try:
                xl = requests.get(excel_link1)
                xl.raise_for_status()
                excel_link = excel_link1
            # Try version 2 of link i
            except:
                xl = None
                xl = requests.get(excel_link2)
                xl.raise_for_status()
                excel_link = excel_link2

            xlPath = os.path.join(output_folder, os.path.basename(excel_link))

            # Write out file
            xlFile = open(xlPath, 'wb')
            for chunk in xl.iter_content(100000):
                xlFile.write(chunk)
            xlFile.close()

            ## Step 2: Load Excel file into DataFrame
            temp = pd.ExcelFile(xlPath)
            temp2 = list(filter(lambda x: 'TimeSeries' in x, temp.sheet_names))[0]
            iead = temp.parse(temp2, skiprows=1)
            of = xlPath+'.csv'
            iead.loc[:, 'Download Timestamp'] = datetime.now()
            iead.to_csv(of, index=False, encoding = 'utf-8')
            logging.debug('Saved data')

            ## Step 3: Convert from wide to long
            tfn = os.path.join(output_folder,'tidied_data_iea_headline_energy.csv')
            long = pd.melt(iead.reset_index(), id_vars=['index','Country','Product','Flow','NoCountry','NoProduct','NoFlow'
                ,'Download Timestamp'])
            long.to_csv(tfn, index=False, encoding='utf-8')
            return None
        except:
            continue
    raise Exception('Did not download any IEA data!')

### Download a single data set from the World Bank
def get_wb_data(output_folder, indicator_id, indicator, country='WLD'):
    logging.info('Downloading World Bank data')
    logging.debug("Indicator: " + indicator)
    logging.debug("country: " + country)
    dat = wb.download(indicator=indicator_id, country=country, start=1800, end=2100)
    dat.reset_index(inplace=True)
    master = None
    for j in range(0,len(dat),1):
        temp = pd.DataFrame( { 'Indicator ID': [indicator_id],
                               'Indicator' : [indicator],
                               'Country' : dat.loc[j,'country'],
                               'Year' : dat.loc[j,'year'],
                               'Value' : dat.loc[j,indicator_id]
                               }
                             )
        master = temp.append(master, ignore_index=True)
    master.dropna(inplace=True)
    master.reset_index(inplace=True, drop=True)
    f = os.path.join(output_folder,'tidied_data_world_bank_' + indicator_id +'.csv')
    master.to_csv(f,index=False, encoding='utf-8')
    logging.info('Saved tidied World Bank data')

### Download grain data from USDA
def get_grain_data(output_folder
                  ,baseurl='https://apps.fas.usda.gov/psdonline/downloads/'
                  ,filename = 'psd_grains_pulses_csv.zip'):
    logging.info('Getting USDA grain data')
    ag = requests.get(baseurl+filename)
    ag.raise_for_status()

    # Write out file
    agPath = os.path.join(output_folder,filename)
    with open(agPath, 'wb') as agFile:
        for chunk in ag.iter_content(100000):
            agFile.write(chunk)

    # Unzip file
    logging.info('Unzipping ' + agPath)
    with zf.ZipFile(agPath) as myzip:
        myzip.extract(member='psd_grains_pulses.csv',path=output_folder)
    #os.remove(agPath)
    gFile = os.path.join(output_folder, 'psd_grains_pulses.csv')
    gdata = pd.read_csv(gFile)
    logging.info('Removing irrelevant rows, writing back out to ' + gFile)
    gdata = gdata[(gdata['Attribute_Description'] =='Production') | (gdata['Attribute_Description'] =='Total Supply')]
    gdata.to_csv(gFile,index=False, encoding='utf-8')

### Download nClimDiv climate data from NOAA National Climatic Data Center with temperature,
### precipitation, heating degree days, and cooling degree days for the Puget Sound Lowlands
def get_nClimDiv(output_folder, raw_output_folder):
    logging.info('Downloading nClimDiv temperature and precipitation data from NOAA')
    base = 'ftp.ncdc.noaa.gov'
    folder = 'pub/data/cirs/climdiv/'
    ftp = ftplib.FTP(base)
    ftp.login()
    ftp.cwd(folder)
    fList = ftp.nlst()
    mList = ["climdiv-cddcdv", "climdiv-hddcdv", "climdiv-pcpndv", "climdiv-tmpcdv"]
    for thisMet in mList:
        matching = [s for s in fList if thisMet in s]
        sink = matching[0]+'.txt'
        sink = os.path.join(raw_output_folder, sink)
        logging.info('Saving ' + sink)
        ftp.retrbinary('RETR ' + matching[0], open(sink, 'wb').write)
        temp = pd.read_csv(sink, delimiter='\s+', header=None,
                           names=['StateDivisionElementYear', '01', '02'
                                  ,'03','04','05','06','07'
                                  ,'08','09','10','11','12']
                           ,dtype=str)
        temp.loc[:,'STATE-CODE'] = temp.StateDivisionElementYear.str.slice(0,2)
        temp.loc[:,'DIVISION-NUMBER'] = temp.StateDivisionElementYear.str.slice(2,4)
        temp.loc[:,'ELEMENT'] = temp.StateDivisionElementYear.str.slice(4,6)
        temp.loc[:,'YEAR'] = temp.StateDivisionElementYear.str.slice(start=6)
        mTemp = temp.melt(id_vars=['StateDivisionElementYear','STATE-CODE', 'DIVISION-NUMBER'
                                   ,'ELEMENT','YEAR'], var_name='Month')
        outfile = 'tidied_data_n' + thisMet + '.csv'
        outfile = os.path.join(output_folder,outfile)
        logging.info('Saving tidied data ' + outfile)
        mTemp.to_csv(outfile, index=False, encoding='utf-8')
    ftp.quit()

### Download SNOTEL data from USDA about snowpack levels
# Cycle throw SNOTEL station list, download data and write out as tidy
def get_snotel_data(snotel_folder):
    logging.info('Downloading SNOTEL data from USDA')
    snotel_stations = pd.read_csv(os.path.join(snotel_folder, 'WA_SNOTEL_STATION_LIST.csv'))
    for thisSI in snotel_stations.loc[:,'snotel_station_id']:
        logging.debug('Now on SNOTEL station ' + str(thisSI))
        p1='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/'
        p2=':WA:SNTL|id=%22%22|name/POR_BEGIN,POR_END/WTEQ::value,WTEQ::qcFlag,WTEQ::qaFlag,SNDN::value,SNDN::qcFlag,SNDN::qaFlag?fitToScreen=false'
        url = p1+str(thisSI)+p2
        raw_file = os.path.join(snotel_folder,'snotel_station_id_'+str(thisSI)+'_swe_hx.csv')
        pagey = requests.get(url)
        pagey.raise_for_status()
        logging.info('Writing out ' + raw_file)
        open(raw_file, 'w+').write(pagey.text)

        # Find the last comment line (denoted by #). The line right after is the header
        textfile = open(raw_file, 'r')
        lines = textfile.readlines()
        j = []
        for i in range(0, len(lines), 1):
            if '#' in lines[i]:
                j.append(i)
        j = max(j)
        #print('line j-1: ' + lines[j-1])
        #print('line j: ' + lines[j])
        try:
            logging.debug(lines[j + 1])
        except Exception as e:
            logging.warning('Error during attempted print of header')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.warning(str(e.args))
            logging.warning('Moving on to next station')
            continue

        # Start import using space delimiter
        logging.debug('About to read file back in')
        tidy_snotel = pd.read_csv(raw_file, skiprows=j + 1)
        logging.debug(str(tidy_snotel))
        logging.debug('About to write file back out')
        if len(tidy_snotel.index) == 0:
            logging.debug('length of tidy_snotel is zero, skipping')
            continue
        tidy_snotel.loc[:, 'snotel_station_id'] = thisSI
        tidy_file = os.path.join(snotel_folder, 'tidy_data_snotel_station_id_' + str(thisSI) + '_swe_hx.csv')
        logging.info('Writing out ' + tidy_file)
        tidy_snotel.to_csv(tidy_file,index=False, encoding='utf-8')

    #Cycle through tidy SNOTEL data
    fl = os.listdir(snotel_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidy_data_snotel_station_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(snotel_folder,thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(snotel_folder,thisT))
        masterT = masterT.append(tempT)
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(snotel_folder, 'tidy_data_master_snotel_swe_hx.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file,index=False, encoding='utf-8')

### Download tidal gauge data from NOAA with sea level information
def get_tidal_data(tidal_folder):
    logging.info('Downloading tidal gauge data from NOAA')
    tidal_stations = pd.read_csv(os.path.join(tidal_folder, 'tidal_stations.csv'))
    for thisTS in tidal_stations.loc[:, 'NWLON Station ID']:
        logging.debug('Now on NWLON station ' + str(thisTS))
        p1_STND = 'https://tidesandcurrents.noaa.gov/api/datagetter?product=monthly_mean&application=NOS.COOPS.TAC.WL&begin_date=19010101&end_date=21000101&time_zone=GMT&units=metric&format=csv&datum=STND&station='
        p1_NAVD = 'https://tidesandcurrents.noaa.gov/api/datagetter?product=monthly_mean&application=NOS.COOPS.TAC.WL&begin_date=19010101&end_date=21000101&time_zone=GMT&units=metric&format=csv&datum=NAVD&station='

        url_STND = p1_STND + str(thisTS)
        raw_file_STND = os.path.join(tidal_folder, 'nwlon_station_id_' + str(thisTS) + '_datum_STND.csv')
        pagey_STND = requests.get(url_STND)
        pagey_STND.raise_for_status()
        logging.info('Writing out ' + raw_file_STND)
        open(raw_file_STND, 'w+').write(pagey_STND.text)

        url_NAVD = p1_NAVD + str(thisTS)
        raw_file_NAVD = os.path.join(tidal_folder, 'nwlon_station_id_' + str(thisTS) + '_datum_NAVD.csv')
        pagey_NAVD = requests.get(url_NAVD)
        pagey_NAVD.raise_for_status()
        logging.info('Writing out ' + raw_file_NAVD)
        open(raw_file_NAVD, 'w+').write(pagey_NAVD.text)

        logging.debug('About to read NAVD file back in')
        try:
            tidy_nwlon_navd = pd.read_csv(raw_file_NAVD)
            tidy_nwlon_navd.columns = tidy_nwlon_navd.columns.str.strip()
            tidy_nwlon_navd.loc[:, 'Datum'] = 'NAVD'
            tidy_nwlon_navd.loc[:, 'nwlon_station_id'] = thisTS
            tidy_file_navd = os.path.join(tidal_folder,
                                          'tidied_data_nwlon_station_id_' + str(thisTS) + '_datum_NAVD.csv')
            tidy_nwlon_navd.to_csv(tidy_file_navd, index=False, encoding='utf-8')
        except Exception as e:
            logging.warning('Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to STND file')

        try:
            logging.debug('About to read STND file back in')
            tidy_nwlon_stnd = pd.read_csv(raw_file_STND)
            tidy_nwlon_stnd.columns = tidy_nwlon_stnd.columns.str.strip()
            tidy_nwlon_stnd.loc[:, 'Datum'] = 'STND'
            tidy_nwlon_stnd.loc[:, 'nwlon_station_id'] = thisTS
            tidy_file_stnd = os.path.join(tidal_folder,
                                          'tidied_data_nwlon_station_id_' + str(thisTS) + '_datum_STND.csv')
            tidy_nwlon_stnd.to_csv(tidy_file_stnd, index=False, encoding='utf-8')
        except Exception as e:
            logging.warning('Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next tidal gauge station')

    # Cycle through tidy tidal data
    fl = os.listdir(tidal_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidied_data_nwlon_station_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(tidal_folder, thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(tidal_folder, thisT))
        masterT = masterT.append(tempT)
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(tidal_folder, 'tidy_data_master_tidal_data.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file, index=False, encoding='utf-8')

### Download USGS streamflow data
def get_usgs_streamflow(sf_folder):
    logging.info('Downloading streamflow data from USGS')
    sf_stations = pd.read_csv(os.path.join(sf_folder, 'steamflow_stations.csv'))
    for i in range(len(sf_stations.index)):
        thisSF = str(sf_stations.loc[i, 'USGS Site Number'])
        logging.info('Now on USGS Site Number ' + thisSF)
        url = sf_stations.loc[i, 'URL']
        raw_file = os.path.join(sf_folder, 'usgs_streamflow_site_no_' + thisSF + '.txt')
        pagey = requests.get(url)
        pagey.raise_for_status()
        logging.info('Writing out ' + raw_file)
        open(raw_file, 'w+').write(pagey.text)

        # Find the last comment line (denoted by #). The line right after is the header
        textfile = open(raw_file, 'r')
        lines = textfile.readlines()
        j = []
        for i in range(0, len(lines), 1):
            if '#' in lines[i]:
                j.append(i)
        j = max(j)

        try:
            logging.debug(lines[j + 1])
        except Exception as e:
            logging.warning('Error during attempted print of header')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.warning(str(e.args))
            logging.warning('Moving on to next station')
            continue

        # Start import using space delimiter
        logging.debug('About to read file back in')
        tidy_sf = pd.read_csv(raw_file, skiprows=j + 1, delimiter='\t')
        tidy_sf = tidy_sf.iloc[1:]
        logging.debug(str(tidy_sf))
        logging.debug('About to write file back out')
        if len(tidy_sf.index) == 0:
            logging.debug('length of tidy_sf is zero, skipping')
            continue
        tidy_file = os.path.join(sf_folder, 'tidy_data_usgs_streamflow_site_id_' + thisSF + '.csv')
        logging.info('Writing out ' + tidy_file)
        tidy_sf.to_csv(tidy_file, index=False, encoding='utf-8')

    # Cycle through tidy SNOTEL data
    fl = os.listdir(sf_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidy_data_usgs_streamflow_site_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(sf_folder, thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(sf_folder, thisT))
        masterT = masterT.append(tempT)
    masterT = masterT.loc[:, ['site_no', 'year_nu', 'month_nu', 'mean_va']]
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(sf_folder, 'tidy_data_master_usgs_streamflow.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file, index=False, encoding='utf-8')

### Section of WSU weather data related functions
###
###

### Download a master list of WSU AgWeatherNet weather stations and related information
def get_wsu_station_list(output_folder):
    logging.info('Downloading WSU AgWeatherNet station information')

    ## Step 0: Load existing station list
    lf = os.path.join(output_folder, 'tidied_data_wsu_weather_station_list.csv')
    old_sl = pd.read_csv(lf)

    ## Step 1: Get a list of WSU AgWeather stations
    url = 'http://weather.wsu.edu/?p=92850'
    pagey = requests.get(url)
    pagey.raise_for_status()
    parsey = bs4.BeautifulSoup(pagey.text, 'html5lib')
    a = parsey.select('select[id="stationList"] option')
    sl = None
    for b in a:
        logging.debug(b)
        if int(b.get('value')) > 1:
            temp = pd.DataFrame([{'station_name': b.getText(),
                                  'station_id': b.get('value')}])
            sl = temp.append(sl, ignore_index=True)
    sl.loc[:, 'Download Timestamp'] = datetime.now()
    sl.loc[:, 'station_id'] = sl.loc[:, 'station_id'].astype(int)
    # Keep station_ids from the old list if they are not in the new list
    # Identify what values are in old_sl and not in sl
    key_diff = set(old_sl.station_id).difference(sl.station_id)
    logging.debug('old_sl:')
    logging.debug(old_sl.dtypes)
    logging.debug('sl:')
    logging.debug(sl.dtypes)
    where_diff = old_sl.station_id.isin(key_diff)
    logging.debug(where_diff)
    # Slice old_sl accordingly and append to sl
    sl_w = sl.copy().append(old_sl[where_diff], ignore_index=True)

    sl_w.to_csv(lf, index=False, encoding='utf-8')
    logging.info('Saved tidied station list data')
    logging.debug(sl)
    logging.debug('Step 2: get more detailed info about each station')
    ## Step 2: Get more detailed information about each station
    bURL = 'http://weather.wsu.edu/?p=90150&UNIT_ID='
    masterTab = None
    # Cycle through station list

    for i in range(0, len(sl), 1):
        st = sl.ix[i, 'station_id']
        logging.debug('Now on station ' + str(st))
        url = bURL + str(st)
        logging.debug('url: ' + url)

        # Get page
        # TODO: when loading the page manually, i see a flash of invalid values before real values are loaded
        # Does this mean we need to actually open a browser and wait for elements to appear before we extract
        # data? that would explain all the weird 99999 values we get for some of the station details
        logging.debug('getting page')
        pagey = requests.get(url)
        pagey.raise_for_status()

        parsey = bs4.BeautifulSoup(pagey.text, 'html5lib')

        # Extract station info table
        logging.debug('extracting table info')
        time.sleep(2)
        station_name = parsey.select('div[class="stationDetailsDataDiv"] > div[style="text-align:center"]')[0].find('b').get_text()

        vars = parsey.select('div[class="stationDetailsDataDiv"] div[style="float:left"]')
        vals = parsey.select('div[class="stationDetailsDataDiv"] div[style="float:right"]')
        logging.debug('vars: ')
        logging.debug(vars)
        logging.debug('vals: ')
        logging.debug(vals)
        logging.debug('len(vars): ' + str(len(vars)))
        for j in range(0, len(vars)):
            try:
                logging.debug('j: ' + str(j))
                vr = re.sub(u'[:]', '', vars[j].getText())
                logging.debug('vars j')
                logging.debug(str(vars[j]))
                logging.debug('vr:')
                logging.debug(vr)
                logging.debug('vals j:')
                logging.debug(str(vals[j]))
                tempTab = pd.DataFrame([{'Variable': vr
                                        , 'Value': vals[j].getText()
                                        , 'Station ID': st
                                        , 'Station Name': station_name
                                     }])
                masterTab = tempTab.append(masterTab, ignore_index=True)
            except Exception as e:
                logging.warning('Error while getting station info')
                logging.warning('The exception caught:')
                logging.warning(str(e))
                logging.info(str(e.args))
                logging.warning('breaking...')
                break

        aInfo = parsey.find("h1", string=re.compile("Additional Information")).next_sibling.get_text()
        tempTab = pd.DataFrame([{'Variable': 'Additional Info'
                                    , 'Value': aInfo
                                    , 'Station ID': st
                                    , 'Station Name': station_name
                                 }])
        masterTab = tempTab.append(masterTab, ignore_index=True)
        masterTab.to_csv('temp station info.csv', index=False, encoding='utf-8')


    f = os.path.join(output_folder, 'tidied_data_wsu_weather_station_info.csv')
    masterTab.loc[:, 'Download Timestamp'] = datetime.now()
    masterTab.loc[:, 'Station ID'] = masterTab.loc[:,'Station ID'].astype(int)
    logging.debug("reading old station info")
    old_masterTab = pd.read_csv(f)


    # Keep station_ids from the old list if they are not in the new list
    # Identify what values are in old_masterTab and not in masterTab
    key_diff = set(old_masterTab.loc[:,'Station ID']).difference(masterTab.loc[:,'Station ID'])
    where_diff = old_masterTab.loc[:,'Station ID'].isin(key_diff)
    print(old_masterTab.dtypes)
    print(masterTab.dtypes)
    logging.debug('where_diff: ')
    logging.debug(str(where_diff))
    # Slice old_sl accordingly and append to masterTab
    logging.debug("Adding stations from old file that are missing in new station info")
    masterTab = masterTab.append(old_masterTab[where_diff], ignore_index=True)

    masterTab.to_csv(f, index=False, encoding='utf-8')
    logging.info('Saved tidied station info data')

###  This function brings up the weather.wsu.edu webpage for 15 minute weather station data in Firefox,
###  changes the parameters to a certain date range and station, saves the data straight from the site, tidies up
###  the data, and then saves it as a .csv file
###
### INPUT:
###     start:            The start date of the data to retrieve in the following format: "November 6, 2017". There is no end
###                     because the function automatically generates the end date, which is six days after the start date
###                     (maximum range)
###     station_id:       The id of the station to pull data from. Is appended as a column in the resulting table.
###     station_name:     The name of the station to pull from data from. Only used to append as a column in the resulting
###                     table.
###     output_folder:     The folder to which the tidied data file is written
###     gecko_fullpath:   Full path to the geckodriver to be used
###     browser:          A browser object to use. Default is none, which means a new one will be generated. If a browser
###                     is passed through, it must already be set to a station and have loaded the 15 minute data page for it
###     tempfolder:       The folder to which the data file is downloaded directly from the site
###
### OUTPUT:
###     browser:      Returns a browser object, to possibly pass to another call to this function in order to save time
###                 starting up a new browser.
###     dt:           The latest date in the dataset
###     ud:           The count of the number of unique days in the dataset
def download_tidy_weather_data(start, station_id, station_name, output_folder, gecko_fullpath
                        , browser=None
                        , tempfolder=os.path.join(os.getcwd(), 'temp')
                        ,output_suffix=None):
    logging.info('Start downloading WSU weather data for station_id ' + station_id + ' (' + station_name + ') and beginning with date ' + start)

    ## Step 1: Delete the temporary directory if exists, then create it again
    logging.debug('tempfolder: ' + tempfolder)
    # Remove the tempfolder if it exists, then create it (again)
    try:
        shutil.rmtree(tempfolder)
    finally:
        os.makedirs(tempfolder)

    ## Step 2: Set up Firefox profile so that file will automatically download to the tempfolder
    fp = webdriver.FirefoxProfile()
    fp.set_preference("browser.download.manager.showWhenStarting", False)
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.dir", tempfolder)
    fp.set_preference("browser.download.downloadDir", tempfolder)
    fp.set_preference("browser.download.defaultFolder", tempfolder)
    fp.set_preference("browser.helperApps.neverAsk.openFile", "application/octet-stream")
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")



    ## Step 3: Open a browser and mimic typing in parameters on the data page and pushing buttons, then download a .csv
    ## data file

    # If a browser was not passed, open a new one and set the station id. Otherwise, we should already be on the right
    # station page for 15 minute data
    if browser is None:
        browser = webdriver.Firefox(firefox_profile=fp, executable_path=gecko_fullpath)
        browser.get('http://weather.wsu.edu/?p=92850')
        stationElem = browser.find_element_by_xpath("//select[@id='stationList']/option[@value='" + station_id + "']")
        stationElem.click()

    # Set the start date
    s = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@id='datepickerstart']"))
    )

    logging.debug('EC:')
    logging.debug(EC)

    #time.sleep(5)

    s = browser.find_element(By.XPATH, "//input[@id='datepickerstart']")

    s.clear()
    s.send_keys(start)

    # Convert start to datetime
    starter = datetime.strptime(start, '%b %d, %Y')
    ender = starter + timedelta(days=6)
    end = datetime.strftime(ender, '%b %d, %Y')

    # Set the end date
    e = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@id='datepickerend']"))
    )
    #time.sleep(2)
    e = browser.find_element(By.XPATH, "//input[@id='datepickerend']")
    e.clear()
    e.send_keys(end)

    # Submit date ranges
    submit = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@value='Submit']"))
    )
    #time.sleep(2)
    submit = browser.find_element(By.XPATH, "//input[@value='Submit']")
    submit.click()

    # Download the csv file to the temporary directory
    dl = WebDriverWait(browser, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@id='downloadbutton']"))
    )

    time.sleep(1)
    dl = browser.find_element(By.XPATH, "//input[@id='downloadbutton']")
    time.sleep(1)
    dl.click()
    logging.debug('After click download button')
    time.sleep(1)
    browser.switch_to.alert.accept()
    logging.debug('Alert Accepted')
    time.sleep(2)

    # Wait for a single .csv file to appear in the temporary folder before proceeding
    # Stop if exceeds 30 sec
    start_time = time.time()
    elapsed_time = 0
    csv = []
    part = []
    f = []
    logging.debug('Waiting for file to show up...')
    while elapsed_time < 30 and len(csv) != 1 and len(f) != 1 and len(part)==0:
        f = os.listdir(tempfolder)
        regex_csv = re.compile(r'(.*)[.]csv$')
        csv = [m.group(0) for l in f for m in [regex_csv.match(l)] if m]
        regex_part = re.compile(r'(.*)[.]part$')
        part = [m.group(0) for l in f for m in [regex_part.match(l)] if m]
        elapsed_time = time.time() - start_time
    logging.debug('elapsed_time: ' + str(elapsed_time))


    # If we went over time, throw out an error
    if elapsed_time >= 30:
        browser.quit()
        logging.warning('Waited more than 30 secs and no .csv or too many .csv files were downloaded')
        raise Exception('Waited more than 30 secs and no .csv file or too many .csv files were downloaded')

    ## Step 4: Open the downloaded file to get the station location
    # The first line of the file should be a location
    f2 = os.path.join(tempfolder, f[0])
    try:
        place = pd.read_csv(f2, nrows=1, encoding='ISO-8859-1')
    except Exception as e:
        logging.warning('Error during attempted load of first line of downloaded file.')
        logging.warning('The exception caught:')
        logging.warning(str(e))
        logging.info(str(e.args))
        browser.quit()
        logging.warning('Returning None...')
        return None
    logging.debug('Header from first line of downloaded file: ' + place.head())
    logging.debug('len(place): ' + str(len(place)))
    if len(place) != 1:
        logging.warning('first line of csv file not as expected')
        browser.quit()
        raise Exception('first line of csv file not as expected')
    loc = place.columns.values[0]
    logging.debug('loc (location from first line, first column of downloaded file): ' + loc)
    logging.debug('f2 (full path to downloaded file): ' + f2)

    ## Step 5: Open the downloaded file to load the weather data

    # Read in data past the header into tab
    tab = None
    try:
        tab = pd.read_csv(f2, skiprows=2, encoding='ISO-8859-1')
    # If data frame is empty but there is a first line of csv, this probably means we chose a date range without data
    # The first line then is an error message that will likely contain the first date with valid data
    except pd.io.common.EmptyDataError:
        logging.warning('download_WSU_data: EmptyDataError when reading downloaded file past first two lines')
        tab = pd.read_csv(f2, skiprows=1, encoding='ISO-8859-1')
        temp = tab.columns.values[0][0:10]
        logging.debug('temp: ' + str(temp))
        logging.debug('type(temp): ' + str(type(temp)))
        stp = None
        logging.debug('some more stuff')
        stp = datetime.strptime(temp, '%Y-%m-%d')
        logging.debug('stp: ' + str(stp))
        stf = datetime.strftime(stp, '%b %d, %Y')
        logging.debug('stf: ' + stf)
        logging.info('download_WSU_data: About to call download_WSU_data with start=' + stf)
        return (download_tidy_weather_data(start=stf
                                    , station_id=station_id
                                    , output_folder=output_folder
                                    , station_name=station_name
                                    , gecko_fullpath=gecko_fullpath
                                    , browser=browser))

    logging.debug('Header read in after skipping 2 lines of downloaded file:')
    logging.debug(tab.head())

    # If the data is shorter than expected, we probably went out of range of the data and should retry again with
    # a proper date range
    logging.debug('tab.index:')
    logging.debug(tab.index)
    if len(tab.index) < 2:
        logging.info('Data shorter than expected')
        temp = pd.to_datetime(str(tab.iloc[0, 0])[0:10])
        st = datetime.strftime(temp, '%b %d, %Y')
        logging.info('download_tidy_weather_data: Calling download_tidy_weather_data with start = ' + st)
        return (download_tidy_weather_data(start=st
                                  , station_id=station_id
                                  , output_folder=output_folder
                                  , station_name=station_name
                                  , browser=browser
                                  , gecko_fullpath=gecko_fullpath))
    else:
        logging.debug('made it past out of range handler code')

    ## Step 6: Tidy up the downloaded and then loaded data
    # If data is expected length, add more informational columns, remove the spurious end column,
    # write out the data frame and return values to be used in next run
    logging.debug('Start constructing table to write out')
    tab.loc[:, 'Location'] = loc
    tab.loc[:, 'Station ID'] = station_id
    tab.loc[:, 'Station Name'] = station_name
    tab.loc[:, 'Download Timestamp'] = datetime.now()
    del tab[' ']
    logging.debug('Columns of constructed table:')
    logging.debug(tab.columns.values)
    tab.drop_duplicates(inplace=True)
    logging.debug(tab.tail())
    #dl = pd.to_datetime(tab.loc[:, 'Date'], format='%B %d, %Y', errors='coerce')
    dl = pd.to_datetime(tab.loc[:, 'Date'], format='%Y-%m-%d', errors='coerce')
    temp = dl.copy()
    temp = temp.dropna().drop_duplicates()
    uniq_dts = len(temp)
    logging.debug('Number of unique dates found: ' + str(uniq_dts))
    #tab.loc[:, 'Date DT'] = dl

    # If units are Celsius, something happened wrong, so raise an Exception
    a = tab.columns.values
    a = [i for i, item in enumerate(a) if '°C' in item]
    if len(a) > 0:
        logging.warning('°C units unexpectedly found')
        browser.quit()
        raise Exception('Celsius units detected')

    # Calculate the min date in the dataset in order to append a date to the file being written out
    # Also calculate the max date in the dataset in order to return the next start date to pull from
    mn_dt = min(dl)
    mn_txt = datetime.strftime(mn_dt, '%Y-%m-%d')
    mx_dt = max(dl)
    mx_txt = datetime.strftime(mx_dt, '%b %d, %Y')
    logging.debug('Earliest date in data: ' + str(mn_dt))
    logging.debug('Latest date in data: ' + str(mx_dt))
    if output_suffix is None:
        fname = station_id + '_' + mn_txt + '.csv'
    else:
        fname = station_id + '_' + mn_txt + '_' + output_suffix + '.csv'
    outname = os.path.join(output_folder, fname)
    tab.to_csv(outname, index=False, encoding='utf-8')
    logging.info('Wrote out tidied table to ' + outname)
    return [browser, mx_txt, uniq_dts]

## Utility function to find files related to a station_id. Returns a list of file names
def find_station_files(scan_folder, station_id):
    fl = os.listdir(scan_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^(' + station_id + r')_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    if len(fl) == 0:
        logging.warning('No files found for station_id ' + station_id)
        return None
    else:
        return fl

### Download WSU weather data for all stations in a list
def get_wsu_weather_data(station_list_file, station_info_file, output_folder, gecko_fullpath, max_ts=None):
    logging.info('Downloading WSU AgWeatherNet data')

    ## Prep: If max_ts is None, set to default of the first day of the previous month
    if max_ts is None:
        max_ts = (datetime.now())
        DD = timedelta(days=31)
        max_ts = max_ts - DD
        max_ts = max_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    ## Step 1: Load station list
    stat_list = pd.read_csv(station_list_file)

    ## Step 2: Load station info
    stat_info = pd.read_csv(station_info_file)
    stat_info['Station ID'] = stat_info['Station ID'].astype(str)

    ## Step 3: Cycle through station list and start downloading data
    for i in range(0,len(stat_list.loc[:, 'station_id']),1):
        station_id      =   str(stat_list.loc[i, 'station_id'])
        station_name    =   stat_list.loc[i, 'station_name']
        logging.debug('type(station_id): ' + str(type(station_id)))
        logging.debug('type of station id Station ID column]: ' + str(type(stat_info.loc[:, 'Station ID'][1])))

        # First set start_ts to installation date of station
        logging.debug('stat_info: ' + str(stat_info.head()))
        idx = stat_info.loc[:, 'Station ID'] == station_id
        logging.debug('station id filter idx: ' + str(idx))
        temp_si = (stat_info.copy()).loc[idx, :]
        logging.debug('temp_si after filter on station_id: ' + str(temp_si.head()))
        idx = temp_si.loc[:, 'Variable'] == 'Installation Date'
        logging.debug('variable filter idx: ' + str(idx))
        temp_si = temp_si.loc[idx, :]
        logging.debug('temp_si after filter on variable: ' + str(temp_si.head()))
        val = temp_si.loc[:, 'Value']
        logging.debug('val: ' + str(val))
        start_ts = None
        if len(val) == 1:
            start_ts = pd.to_datetime(val).reset_index(drop=True)[0]
            logging.info('Set start_ts for ' + station_id + ' (' + station_name +') to installation Date: ' + str(start_ts))

        else:
            logging.info('More or less than 1 installation date found for ' + station_name)
            start_ts = datetime.strptime('2001-01-01', '%Y-%m-%d')
            logging.info('Set start_ts for ' + station_id + ' (' + station_name +') to a default: ' + str(start_ts))

        # Look for existing station data. If there is data, set start_ts to the latest start date from those files
        station_output_folder = os.path.join(output_folder,station_id)
        # If the station doesn't exist as a folder, create it
        if not os.path.isdir(station_output_folder):
            logging.debug('Station folder not found! creating...')
            os.mkdir(station_output_folder)
        fl = find_station_files(scan_folder=station_output_folder, station_id=station_id)
        regex = re.compile(station_id + r'_(.*)[.]csv')
        if fl is not None:
            existing_dates = [m.group(1) for l in fl for m in [regex.search(l)] if m]
            logging.debug('existing_dates: ' + str(existing_dates))
            logging.debug('len(existing_dates): ' + str(len(existing_dates)))
            if len(existing_dates) > 0:
                logging.debug('Existing station data found for: ' + station_id)
                nb = pd.to_datetime(existing_dates, format='%Y-%m-%d')
                nb = max(nb)
                start_ts = max(nb, start_ts)
                logging.info('Set start_ts to max from existing station data: ' + str(start_ts))

        # If start_ts is on or later than our max_ts of data to pull, go to next station
        if start_ts >= max_ts:
            logging.info('Start of ' + str(start_ts) + ' is on or after ' + str(max_ts) + '. Moving on to next station.')
            continue

        # Attempt first download
        start = datetime.strftime(start_ts, '%b %d, %Y')
        try:
            br, dt, ud = download_tidy_weather_data(start=start, station_id=station_id, station_name=station_name,
                                                    output_folder=station_output_folder
                                           , gecko_fullpath=gecko_fullpath)
        except Exception as e:
            logging.warning('get_wsu_weather_data: Error thrown for first download for station ' + station_name
                            + ' (' + station_id + ') with start of ' + start)
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next station')
            continue
        logging.debug('br: ' + str(br))
        logging.debug('dt: ' + str(dt))
        logging.debug('ud: ' + str(ud))

        # Continue to download more data from station. ud is the number of unique dates from the last file. While there
        # are more than 2, there might be more data to pull
        while ud > 2:
            # Try to download. If it fails for any reason, continue with next station
            try:
                br, dt, ud = download_tidy_weather_data(start=dt, station_id=station_id, station_name=station_name, browser=br
                                               , output_folder=station_output_folder, gecko_fullpath=gecko_fullpath)
            except Exception as e:
                logging.warning('get_wsu_weather_data: Error thrown for first download for station ' + station_name
                                + ' (' + station_id + ') with start of ' + dt)
                logging.warning('The exception caught:')
                logging.warning(str(e))
                logging.info(str(e.args))
                br.quit()
                logging.warning('Closing browser and moving on to next station')
                break
            logging.debug('br: ' + str(br))
            logging.debug('dt: ' + str(dt))
            logging.debug('ud: ' + str(ud))
        br.quit()
        logging.info('get_wsu_weather_data: finished downloading data for ' + station_name + ' (' + station_id +')')

### Download WSU weather data for daylight savings transition points to serve as data quality checks and calibrators
### Data downloaded can vary depending on whether data was downloaded during or outside of daylight savings time
### Getting this data quality calibration data will serve as a double check of values
def get_wsu_daylight_savings(station_list_file, station_info_file, output_folder, gecko_fullpath, dst_file
                             ,max_ts=None):
    logging.info('Downloading WSU AgWeatherNet data for daylight savings')

    ## Prep: If max_ts is None, set to default of the first day of the previous month
    if max_ts is None:
        max_ts = (datetime.now())
        DD = timedelta(days=31)
        max_ts = max_ts - DD
        max_ts = max_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    ## Step 1: Load station list
    stat_list = pd.read_csv(station_list_file)

    ## Step 2: Load station info
    stat_info = pd.read_csv(station_info_file)
    stat_info['Station ID'] = stat_info['Station ID'].astype(str)

    ## Step 3: Cycle through station list and start downloading data
    for i in range(0,len(stat_list.loc[:, 'station_id']),1):
        station_id      =   str(stat_list.loc[i, 'station_id'])
        station_name    =   stat_list.loc[i, 'station_name']
        logging.debug('type(station_id): ' + str(type(station_id)))
        logging.debug('type of station id Station ID column]: ' + str(type(stat_info.loc[:, 'Station ID'][1])))

        # First set start_ts to installation date of station
        logging.debug('stat_info: ' + str(stat_info.head()))
        idx = stat_info.loc[:, 'Station ID'] == station_id
        logging.debug('station id filter idx: ' + str(idx))
        temp_si = (stat_info.copy()).loc[idx, :]
        logging.debug('temp_si after filter on station_id: ' + str(temp_si.head()))
        idx = temp_si.loc[:, 'Variable'] == 'Installation Date'
        logging.debug('variable filter idx: ' + str(idx))
        temp_si = temp_si.loc[idx, :]
        logging.debug('temp_si after filter on variable: ' + str(temp_si.head()))
        val = temp_si.loc[:, 'Value']
        logging.debug('val: ' + str(val))
        start_ts = None
        if len(val) == 1:
            start_ts = pd.to_datetime(val).reset_index(drop=True)[0]
            logging.info('Set start_ts for ' + station_id + '(' + station_name + ') to installation date: ' + str(start_ts))
        else:
            logging.info('More or less than 1 installation date found, skipping station ' + station_name)
            continue

        # Set station output folder
        station_output_folder = os.path.join(output_folder,station_id)
        # If the station doesn't exist as a folder, create it
        if not os.path.isdir(station_output_folder):
            logging.debug('Station folder not found! creating...')
            os.mkdir(station_output_folder)

        ## Load list of daylight savings dates
        dst_dt_tbl = pd.read_csv(dst_file,parse_dates=[1,2])['Start']

        ## Filter list to get only those dates after installation and before today

        dst_dt_tbl = dst_dt_tbl[(dst_dt_tbl>=start_ts) & (dst_dt_tbl<date.today())]

        #Cycle through dates
        br = None
        dt = None
        for this_dt in dst_dt_tbl:
            logging.info('this_dt: ' + str(this_dt))
            start = datetime.strftime(this_dt, '%b %d, %Y')

            # If dt has been set and is after this_dt, then skip to next. For example,
            # data may not exist even after installation date due to not passing QA checks
            # and so the function returns the first date with quality controlled data
            if dt is not None:
                dtt = datetime.strptime(dt, '%b %d, %Y')
                if dtt > this_dt:
                    logging.info('next date with data (' + str(dtt)+ ') is after ' + str(this_dt) + ', skipping to next')
                    continue
            #If the file already exists, go to next. Look for dates one day before as well, sometimes this happens.
            osf = datetime.strftime(date.today(), '%Y-%m-%d')
            mn_txt = datetime.strftime(this_dt, '%Y-%m-%d')
            mn_txt2 = datetime.strftime((this_dt-timedelta(days=1)), '%Y-%m-%d')
            tname = station_id + '_' + mn_txt + '_' + osf + '.csv'
            tname2 = station_id + '_' + mn_txt2 + '_' + osf + '.csv'
            tfname = os.path.join(output_folder, str(station_id),tname)
            tfname2 = os.path.join(output_folder, str(station_id), tname2)
            #logging.debug('tfname: ' + tfname)
            #logging.debug('is file: '+ str(os.path.isfile(tfname)))
            #logging.debug('tfname2: ' + tfname2)
            #logging.debug('is file2: '+ str(os.path.isfile(tfname2)))
            if os.path.isfile(tfname):
                logging.info(tfname + ' already found, skipping')
                continue
            if os.path.isfile(tfname2):
                logging.info(tfname2 + ' already found, skipping')
                continue
            try:
                br, dt, ud = download_tidy_weather_data(start=start, station_id=station_id
                                                ,station_name=station_name
                                                ,browser=br
                                                ,output_folder=station_output_folder, gecko_fullpath=gecko_fullpath
                                                        ,output_suffix=osf)

            except Exception as e:
                logging.warning('get_wsu_weather_data: Error thrown for download for station ' + station_name
                                + ' (' + station_id + ') with start of ' + start)
                logging.warning('The exception caught:')
                logging.warning(str(e))
                logging.info(str(e.args))
                logging.warning('Closing browser and moving on to next date')
                if br is not None:
                    br.quit()
                ## If eror thrown was about station picking, go to next station
                if "Unable to locate element: //select[@id='stationList']" in str(e):
                    logging.warning('Error was related to a missing station, skipping station...')
                    break
                else:
                    continue
        if br is not None:
            br.quit()
        logging.info('get_wsu_daylight_savings: finished downloading data for ' + station_name + ' (' + station_id +')')

### Write out the date ranges of WSU weather data already downloaded
def wsu_progress(station_list_file,output_folder, scan_folder, dl_folder, dr_folder):
    outfile = os.path.join(output_folder,'wsu_weather_download_progress.csv')
    sl = pd.read_csv(station_list_file)
    sl.loc[:, 'CSV Min Date'] = None
    sl.loc[:, 'CSV Max Date'] = None
    sl.loc[:, 'DL Min Date'] = None
    sl.loc[:, 'DL Max Date'] = None
    sl.loc[:, 'DL Timestamp Count'] = None
    sl.loc[:, 'DR Timestamp Count'] = None

    for i in range(0,len(sl)):
        logging.debug('i: ' + str(i) + ' out of ' + str(len(sl)))
        station_id = str(sl.loc[i, 'station_id'])
        station_id = str(station_id)
        logging.debug('Getting progress for station_id ' + station_id)
        station_scan_folder = os.path.join(scan_folder,station_id)
        try:
            fList = os.listdir(station_scan_folder)
        except Exception as e:
            logging.warning('wsu_progress: Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next station')
            continue
        ## Get progress of downloaded .csv files
        regex = re.compile(station_id + r'_(.*)[.]csv')
        a = [m.group(1) for l in fList for m in [regex.search(l)] if m]
        if len(a) > 0:
            nb = pd.to_datetime(a, format='%Y-%m-%d')
            max_dt = str(max(nb))
            min_dt = str(min(nb))
            sl.ix[i, 'CSV Min Date'] = min_dt
            sl.ix[i, 'CSV Max Date'] = max_dt

        ## Get progress in data lake
        try:
            dl_db = os.path.join(dl_folder,('dl_'+station_id+'.db'))
            dl_conn = sqlite3.connect(dl_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            dl_cur = dl_conn.cursor()
            thissql = 'SELECT DISTINCT date, time_pdt, time_pst FROM dl_wsu_weather WHERE station_id = \'' + station_id + '\''
            dl_ts_cnt = pd.read_sql_query(sql=thissql, con=dl_conn)
            sl.ix[i, 'DL Timestamp Count'] = len(dl_ts_cnt.index)

            thissql = 'SELECT min(date) AS dl_min_date, max(date) as dl_max_date FROM dl_wsu_weather WHERE station_id = \'' + station_id + '\''
            dl_dt = pd.read_sql_query(sql=thissql, con=dl_conn)
            #logging.debug('type of pull: ' + str(type(dl_dt.at[0,'dl_min_date'])))
            sl.ix[i, 'DL Min Date'] = dl_dt.at[0,'dl_min_date']
            sl.ix[i, 'DL Max Date'] = dl_dt.at[0,'dl_max_date']
            #logging.debug(str(sl))
            dl_cur.close()
            dl_conn.close()

            ## Get progress in data refinery
            dr_db = os.path.join(dr_folder,('dr_'+station_id+'.db'))
            dr_conn = sqlite3.connect(dr_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            dr_cur = dr_conn.cursor()
            thissql = 'SELECT DISTINCT timestamp FROM dr_wsu_weather WHERE station_id = \'' + station_id + '\''
            dr_ts_cnt = pd.read_sql_query(sql=thissql, con=dr_conn)
            sl.ix[i, 'DR Timestamp Count'] = len(dr_ts_cnt.index)
            #logging.debug(str(sl))
            dr_cur.close()
            dr_conn.close()
        except Exception as e:
            logging.warning('wsu_progress: Error thrown')
            logging.warning('Database related error for station_id ' + str(station_id) )
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next station')
            continue
    bool_list = sl.loc[:, 'DL Timestamp Count'] > 0
    sl.loc[bool_list,'Percent of DL in DR'] = sl.loc[bool_list,'DR Timestamp Count'] / sl.loc[bool_list,'DL Timestamp Count']
    logging.debug(str(sl))
    sl.to_csv(outfile,index=False, encoding='utf-8')

### Write out the 1st and 99th percentile of minimum and maximum daily temperatures respectively over each station
### and year
def wsu_min_max(data_folder, station_list_file):
    logging.info(
        'Calculating 1st percentile of daily min and 99th percentile of daily max temperatures for each station and year')
    sl = pd.read_csv(station_list_file)
    master_mn = None
    master_mx = None
    for this in sl.loc[:, 'station_id']:
        si = str(this)
        dr_db = os.path.join(os.getcwd(), 'sqlite', 'dr', 'dr_' + si + '.db')
        logging.debug('Now on ' + dr_db)
        dr_conn = sqlite3.connect(dr_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        dr_cur = dr_conn.cursor()
        tt = pd.read_sql('''

        WITH D AS (
        SELECT
            station_id
            ,date(timestamp) AS DT
            ,strftime('%Y',timestamp) AS YEAR
            ,station_name
            ,min(air_temp_f) AS MIN_TEMP_F
            ,max(air_temp_f) AS MAX_TEMP_F
        FROM dr_wsu_weather
        GROUP BY
            station_id, DT, station_name
        )

        SELECT
            station_id
            ,DT
            ,YEAR
            ,station_name
            ,MIN_TEMP_F
            ,MAX_TEMP_F
        FROM
            D
        ''', con=dr_conn)

        t = tt.groupby(['station_id', 'station_name', 'YEAR']).quantile([.01, .99])
        t = t.reset_index()
        mn = t.groupby(['station_id', 'station_name', 'YEAR']).min().reset_index()
        mx = t.groupby(['station_id', 'station_name', 'YEAR']).max().reset_index()
        mn = mn.loc[:, ['station_id', 'station_name', 'YEAR', 'MIN_TEMP_F']]
        mx = mx.loc[:, ['station_id', 'station_name', 'YEAR', 'MAX_TEMP_F']]
        master_mn = mn.append(master_mn)
        master_mx = mx.append(master_mx)
        dr_cur.close()
        dr_conn.close()


    mx_file = os.path.join(data_folder, 'Annual 99 Percentile Max Temp WSU Weather.csv')
    logging.info('Writing out ' + mx_file)
    master_mx.to_csv(mx_file, index=False, encoding='utf-8')


    mn_file = os.path.join(data_folder, 'Annual 1 Percentile Min Temp WSU Weather.csv')
    logging.info('Writing out ' + mn_file)
    master_mn.to_csv(mn_file, index=False, encoding='utf-8')

    return None