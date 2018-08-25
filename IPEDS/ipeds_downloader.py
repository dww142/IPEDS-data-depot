"""
Script to download complete data files from the IPEDS Data Center and consolidate multiple years
of data for each file into a single consolidated datafile.

ON EACH RUN:
    In the __main__() method - adjust the commented section defining the surveys to be
        downloaded on the current run, this is really meant to be run from the script with that
        section changing on each year; 
        NOTE that different files align with academic years differently - defined in comments
        in the main method. 

"""
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

ENGINE = sa.create_engine('mssql+pyodbc://sql_alchemy:sql_alchemy@LOCALHOST/SLDS_ETL?driver=SQL+Server+Native+Client+11.0')

#trying go figure out the best way to read/write data for importing to SQL Server
READ_ENCODING = 'ISO-8859-1' #'utf-8' #'ISO-8859-1'
WRITE_ENCODING = 'latin-1'
DATABASE_SCHEMA = 'IPEDS'

def table_column_metadata(file_code):
    '''
    get the column names and data types for the ETL column matching
    the IPEDS survey file provided
    '''
    # from sqlalchemy.ext.declarative import declarative_base
    base = declarative_base()
    base.metadata.reflect(ENGINE, schema=DATABASE_SCHEMA)
    cols = {}
    for c in base.metadata.tables[DATABASE_SCHEMA + '.tbl' + file_code].columns:
        cols[str(c.name)] = c.type
    return cols

def compare_file_table(file_code, file_frame):
    '''
    get the list of columns that are in the consolidated data frame
    that are NOT in the database table - these will be removed from the frame
    allowing for a clean insert and printed out so the table can be updated to 
    include these columns if needed
    '''
    table_columns = table_column_metadata(file_code)
    not_in_table = [c for c in file_frame.columns if c not in table_columns]
    # not_in_file = [c for c in table_columns if c not in file_frame.columns]
    return not_in_table

def clean_file_frame(file_frame, not_in_table):
    '''
    remove any columns from the file frame that are not
    in the target database table for the given file_code
    '''
    for frame_col in file_frame.columns:
        if frame_col in not_in_table:
            del consolidated_frame[frame_col]
    return not_in_table
            
    

def extract_file(data, survey, file_code, survey_year, dictionary=False):
    """Extract the latest data file in the downloaded zip file to the data directory
    and return a dictionary of metadata to the calling function.
    """
    print('extracting', ('dictionary...' if dictionary else '...'))
    extracted_file = ''
    data = zipfile.ZipFile(io.BytesIO(data.content))
    # if multiple files in zip, extract the latest revised data ONLY
    data_file = data.namelist()[len(data.namelist())-1]
    output_path = os.path.join(settings.DATA_DIRECTORY, survey, file_code)
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
    HORRIBLE HACK: keep repeating request until successfull
    while Ignoring exceptions...
    works for data files and dictionaries
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

def download_file(survey, file_code, survey_year, get_dictionary=False):
    """Build the zip file url for the specified file and year
    optionally do the same for the dictionary for that year
    """
    formatter = settings.SURVEY_FILES[survey][file_code]
    data_file_name = formatter(survey_year)
    data_file_url = os.path.join(settings.IPEDS_BASE_URL, data_file_name)

    data_request = download_file_request(data_file_url)

    file_metadata = ''
    if data_request.ok:
        file_metadata = extract_file(data_request, survey, file_code, survey_year)
        #only attemp to get dictionary if data request was successful
        print('downloaded')
        if get_dictionary:
            dictionary_file_url = os.path.join(settings.IPEDS_BASE_URL, data_file_name[:-4]+'_Dict.zip')
            # dictionary_request = requests.get(dictionary_file_url)
            dictionary_request = download_file_request(dictionary_file_url)
            if dictionary_request.ok:
                extract_file(dictionary_request, survey, file_code, survey_year, dictionary=True)
    else:
        file_metadata = 'File Not Found'
        print(file_metadata)
    return file_metadata

def consolidate_survey_files(file_code_log, row_test=None):
    """
    Consolidate all csv files in a given directory into a single Pandas dataframe
    Each year goes into a data frame, that is added to a list of frames, the list
    is then concatenated. The concatenation process takes care of aligning consistent
    fields between files and if fields are dropped or added across years, they are filled
    with empty strings when there is no data. 
    """
    file_frames = []
    for file_year in file_code_log:
        if file_code_log[file_year] != 'File Not Found':
            file_frame = pd.read_csv(file_code_log[file_year]
                                     , dtype=object
                                     , nrows=row_test
                                     , encoding=READ_ENCODING
                                    )
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
    if len(file_frames) > 0:
        return pd.concat(file_frames)


def __main__():
    """
    Set download_surveys to define the data you want to get on this run
    set the download_surveys list to include the survey groups that you want to download
    better for a controlled run to only do 1 or 2 at a time
    each grou pconsists of multiple files
    survey files for each group are defined in the settings file
    
    ***Start Years vary by survey, e.g., Fall Enrollment data for 2015, means fall 2015, which
    is part of the 2015-2016 academic year. 
    Whereas completion data for 2015 is at the end of the year, which is the 2014-2015 academic year. 

    """
    # download_surveys = ['AcademicLibraries'] #
    # download_surveys = ['EmployeesByAssignedPosition'] # 
    # download_surveys = ['InstructionalStaffSalaries'] # .

    ###### start_year = 2003 which is the 2002-2003 Academic Year for these file groups:
    # download_surveys = ['UnduplicatedEnrollment'] # 
    # download_surveys = ['Finance'] # 
    # download_surveys = ['GradRates'] # 
    # download_surveys = ['StudentFinAid'] # 2-27
    # download_surveys = ['Completions'] # 
    #or
    # download_surveys = ['Completions', 'StudentFinAid', 'GradRates', 'Finance', 'UnduplicatedEnrollment']

    ###### start_year = 2002 which is the 2002-2003 Academic Year for these 2 file groups:
    download_surveys = ['InstitutionCharacteristics'] # 2-27
    # download_surveys = ['FallEnrollment'] #
    # download_surveys = ['AdmissionsTestScores'] #
    #or
    # download_surveys = ['InstitutionCharacteristics', 'FallEnrollment', 'AdmissionsTestScores'] # run: 2017-7-24

    start_year = 2002
    end_year = 2017

    survey_year_range = range(start_year, end_year + 1)
    surveys = {k : settings.SURVEY_FILES[k] for k in download_surveys}
    # surveys = settings.SURVEY_FILES # this will download ALL files for ALL surveys...long run..

    start_time = dt.datetime.now()
    #loop through the surveys you want to download
    for survey in surveys:
        log = {'Download Date' : settings.DOWNLOAD_DATE,
               'Search Start Year' : start_year,
               'Search End Year' : end_year
              }
        log[survey] = {}
        
        # loop through the files for that survey
        for file_code in surveys[survey]:
            log[survey][file_code] = {}

            #loop through the desired years, for that file:
            for survey_year in survey_year_range:
                print("Download Start: ", survey, file_code, survey_year)
                meta_result = download_file(survey, file_code, survey_year, get_dictionary=True)
                print("Download Complete: ", meta_result)
                log[survey][file_code][survey_year] = meta_result
            
            #once you have all years, start consolidating the separate files:
            print('Start Consolidation')
            consolidated_file = consolidate_survey_files(log[survey][file_code])
            print('Consolidation Done.')

            ####### WRITE CONSOLIDATED FILE TO CSV
            # consolidated_file_name = '_'.join([file_code
            #                                    , 'Consolidated'
            #                                    , str(start_year) + '-' + str(end_year)
            #                                    , '.csv'])
            # consolidated_file.to_csv(os.path.join(settings.DATA_DIRECTORY, survey, file_code
            #                                       , consolidated_file_name)
            #                          , index=False
            #                          , encoding=WRITE_ENCODING
            #            
            #              )
            ####### WRITE CONSOLIDATED FILE TO SQL
            consolidated_file.to_sql('tbl'+file_code, ENGINE, schema='IPEDS', if_exists='replace', index=False)
            
            # print logging:
            print(file_code)
            print('\t', log[survey][file_code])
            # print('\t', len(consolidated_file.index), 'records inserted')

        log_file = survey+'_log.json'
        with open(os.path.join(settings.DATA_DIRECTORY, survey, log_file), 'w') as survey_log:
            json.dump(log, survey_log, indent=2)

    # download_log = {settings.DOWNLOAD_DATE : log}
    # print(json.dumps(download_log, indent=2))
    runtime = dt.datetime.now() - start_time
    print('Start:', start_time, 'End:', dt.datetime.now(), 'Elapsed:', runtime)



__main__()
