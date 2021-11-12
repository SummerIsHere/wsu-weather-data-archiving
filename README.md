# WSU Weather Data Archiving

## Purpose
This is a deprecated project that remains here solely for archival purposes. Fair warning, it is difficult to maintain and requires repeated runs for completeness.

Previously data had been pulled from the Washington State University (WSU) AgWeatherNet program to populate a report of climate change metrics. It remains an excellent source of weather data for Washington state for a variety of measures from across the state at 15 minute time intervals. However, the report does not need this level of detailed data, and due to the complexities of pulling data from [WSU AgWeatherNet](https://weather.wsu.edu/) as well as the availability of easier to pull, summarized data from US federal agencies, this data source was removed from the report.

This code and legacy data remains for anyone still interested in this particular data source.

The report related code has been moved to a new repository [https://github.com/summerishere/apocalypse-status-slim](https://github.com/summerishere/apocalypse-status-slim)

## Documentation
See the [wiki](https://github.com/SummerIsHere/apocalypse-status/wiki) for additional notes.

## Installation Instructions

1. Download and install the [Firefox web browser](https://www.mozilla.org/firefox/). Go to about About Firefox to check whether it is 32-bit or 64-bit. 64-bit recommended throughout this guide.
2. Install the individual [Anaconda distribution](https://www.anaconda.com/download/) of Python3. When you install, be sure to include the installation of Anaconda Navigator.
3. Open Anaconda Navigator with administrator privileges. Install selenium and pandas-datareader using Navigator.
4. Download the [latest release](https://github.com/mozilla/geckodriver/releases) of geckodriver matching your operating system and and the 32 or 64 bit version of your Firefox and unzip the binary into the relevant subfolder under the geckodriver_bins folder.
5. Install Microsoft [PowerBI](https://www.powerbi.com), a program to create reports and data visualizations.  It is required to open "Apocalypse Status Board.pbix". This program is Windows only, so if you are on another system, use VirtualBox, Parallels, etc to launch a Windows environment)

## How to run
1. Check that your default Python is Python 3 by typing "python --version" in the terminal
1. Open main.py in your favorite text editor. If you kept things in their default state, you won't need to modify anything but check it over to see if you need to change any variables
2. Run main.py script (and any other scripts) from the base directory of the repository. Do this by opening a terminal console, navigating to the top of the repository, and type "python main.py"
3. Errors will show up in the terminal, logging will be outputted to main_logging.txt
3. Other steps
4. Open up the dashboard in PowerBI

## Troubleshooting

1. You should always update to the latest Firefox, geckodriver, and selenium (https://anaconda.org/conda-forge/selenium). You may need to uninstall and reinstall the latest Anaconda completely due to package dependencies.
2. 
