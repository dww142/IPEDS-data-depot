'''
Script to download complete data files from the IPEDS Data Center and consolidate multiple years
of data for each file into a single consolidated Pandas dataframe.

Uses Pandas to read data files into data frames and concatenate the frames together into a single
frame with all of the various columns across years as they change in IPEDS files in a single frame.

That frame is inserted into an MS SQL Server database table where Views perform further 
cleansing, transformation, and integration of the data sets in each file. 

configure all options in the settings file including database connection information, 
desired surveys to download, start/end years for surveys.

'''
import os
import datetime as dt
import zipfile
import io
import json
import requests
import settings
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base


def extract_file(data, survey, file_code, survey_year, dictionary=False):
    '''
    Extract the latest data or dictionary file in the downloaded zip file to the data directory
    and return the path of the successfully extracted file to the caller. 
    '''
    print('extracting', ('dictionary...' if dictionary else '...'), end='...')
    extracted_file = ''
    data = zipfile.ZipFile(io.BytesIO(data.content))
    # if multiple files in zip, extract the last file in the list
    # Should represent the _RV suffixed file containing the latest revised data ONLY
    data_file = data.namelist()[len(data.namelist())-1]
    output_path = os.path.join(settings.DATA_DIRECTORY, survey, file_code, settings.DOWNLOAD_DATE)
    if dictionary:
        output_path = os.path.join(output_path, 'Dictionaries')
    else:
        output_path = os.path.join(output_path, 'Data')
    data.extract(data_file, path=output_path)
    if not dictionary:
        # extracted_file['year'] = survey_year
        # extracted_file['file'] = file_code
        # extracted_file['file_path'] = os.path.join(output_path, data_file)
        extracted_file = os.path.join(output_path, data_file)

    return extracted_file

def download_file_request(file_url):
    '''
    HORRIBLE HACK: keep repeating download request until successfull
    IPEDS Site is sometimes non-responsive on slow internet connections
    This repeats the download request 50 times; hasn't failed yet for 
    a file that is known to exist. 
    TODO: Find a more elegant way to handle errors; this seems stupid. 
    '''
    failed_attempts = 0
    data_req = None
    while data_req == None and failed_attempts < 50:
        try:
            data_req = requests.get(file_url, timeout=5)
        except:
            failed_attempts += 1
            print('Failed Attempts:', failed_attempts, file_url)
            data_req = None
        
    return data_req

def download_file(survey, file_code, survey_year, get_dictionary=False, get_data=True):
    '''
    Build the zip file url for the specified file and year
    optionally do the same for the dictionary for that year
    '''
    # format file name and create url
    formatter = settings.SURVEY_FILES[survey][file_code]
    data_file_name = formatter(survey_year)

    file_metadata = ''
    if get_data:
        data_file_url = os.path.join(settings.IPEDS_BASE_URL, data_file_name)
        data_request = download_file_request(data_file_url)

        file_metadata = ''
        if data_request.ok:
            file_metadata = extract_file(data_request, survey, file_code, survey_year)
            print('Downloaded Data', end='... ')
        else:
            file_metadata = 'File Not Found'
            print(file_metadata, end='...')

    if get_dictionary:
        dictionary_file_url = os.path.join(settings.IPEDS_BASE_URL, data_file_name[:-4]+'_Dict.zip')
        dictionary_request = download_file_request(dictionary_file_url)
        if dictionary_request.ok:
            extract_file(dictionary_request, survey, file_code, survey_year, dictionary=True)
            print('Downloaded Dictionary', end='... ')

    return file_metadata

def consolidate_survey_files(file_code_log, file_code, row_test=None):
    '''
    Consolidate all downloaded files in a given directory into a single Pandas dataframe
    Each year goes into a data frame, that is added to a list of frames, the list
    is then concatenated. The concatenation process takes care of aligning consistent
    fields between files and if fields are dropped or added across years, they are filled
    with empty strings when there is no data. 
    '''
    # initialize empty list to hold all data frames for specified file_code
    file_frames = []
    # loops through the metadata of successfully downloaded files
    for file_year in file_code_log:
        if file_code_log[file_year] != 'File Not Found':
            file_frame = pd.read_csv(file_code_log[file_year]
                                     , dtype=object
                                     , nrows=row_test
                                     , encoding=settings.READ_ENCODING
                                    )
            # Cleanse string placeholders formats from IPEDS numeric data columns
            file_frame = file_frame.replace(['.', ' .'], '')
            file_frame['SURVEY_YEAR'] = pd.Series(file_year, file_frame.index) #year not available..
            file_frame.columns = [col.upper().strip() for col in file_frame.columns]
            #DATA ISSUE IN IPEDS - the IPEDS source file transposed the imputation column and 
            # data column in this file for this year; swap column headings to correctly align:
            if file_year == 2008 and 'F2A20' in file_frame.columns:
                cols = []
                for c in file_frame.columns:
                    if c == 'XF2A20':
                        cols.append('F2A20')
                    elif c == 'F2A20':
                        cols.append('XF2A20')
                    else:
                        cols.append(c)
                file_frame.columns = cols
            file_frames.append(file_frame)
    # make sure there is actually data before attempting to concatenate
    if len(file_frames) > 0:
        return pd.concat(file_frames)


def __main__():
    '''
    TODO: Clean this up. Clarify steps in process

    Set download_surveys to define the data you want to get on this run
    set the download_surveys list to include the survey groups that you want to download
    better for a controlled run to only do 1 or 2 at a time
    each grou pconsists of multiple files
    survey files for each group are defined in the settings file
    
    ***Start Years vary by survey, e.g., Fall Enrollment data for 2015, means fall 2015, which
    is part of the 2015-2016 academic year. 
    Whereas completion data for 2015 is at the end of the year, which is the 2014-2015 academic year. 

    '''
    survey_year_range = settings.SURVEY_YEAR_RANGE
    surveys = settings.DOWNLOAD_SURVEY_FILE_LIST
    start_time = dt.datetime.now()
    #loop through the surveys you want to download
    for survey in surveys:        
        # loop through the files for that survey
        for file_code in surveys[survey]:
            log = {'DOWNLOAD_DATE' : settings.DOWNLOAD_DATE,
                   'START_YEAR' : settings.START_YEAR,
                   'END_YEAR' : settings.END_YEAR
            }
            log[survey] = {}
            log[survey][file_code] = {}

            #loop through the desired years, for that file:
            for survey_year in survey_year_range:
                print("Download Start: ", survey, file_code, survey_year, end='... ')
                # meta_result = download_file(survey, file_code, survey_year, get_dictionary=settings.GET_DICTIONARIES)
                meta_result = download_file(survey, file_code, survey_year, get_dictionary=True, get_data=False) #Dictionaries only
                print("Done.")
                log[survey][file_code][survey_year] = meta_result

            log_file = f'{survey}_{file_code}_{settings.DOWNLOAD_DATE}_log.json'
            # check if exists first; if no files downloaded, path will not exist for file code
            if os.path.exists(os.path.join(settings.DATA_DIRECTORY, survey, file_code, settings.DOWNLOAD_DATE)):
                with open(os.path.join(settings.DATA_DIRECTORY, survey, file_code, settings.DOWNLOAD_DATE, log_file), 'w') as survey_log:
                    json.dump(log, survey_log, indent=2)

    # download_log = {settings.DOWNLOAD_DATE : log}
    # print(json.dumps(download_log, indent=2))
    runtime = dt.datetime.now() - start_time
    print('Start:', start_time, 'End:', dt.datetime.now(), 'Elapsed:', runtime)

__main__()
