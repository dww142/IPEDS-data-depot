'''
'''
import os
import datetime as dtype
import json
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
import settings


def table_column_metadata(file_code):
    '''
    Get the column names and data types from an existing SQL Server database table
    for the specified IPEDS survey file. 
    '''
    # from sqlalchemy.ext.declarative import declarative_base
    base = declarative_base()
    base.metadata.reflect(settings.ENGINE, schema=settings.TARGET_SCHEMA[:-1])
    cols = {}
    if f'{settings.TARGET_SCHEMA}tbl{file_code}' in base.metadata.tables:
        for c in base.metadata.tables[f'{settings.TARGET_SCHEMA}tbl{file_code}'].columns:
            cols[str(c.name)] = c.type
    else:
        cols = None
    return cols

def compare_file_table(file_code, file_frame_columns):
    '''
    Find any columns that are in the provided Pandas data frame
    that are NOT in the existing database table. 
    
    **These columns can either be removed from the frame to allow a clean insert;
    Or - modify the table structure to account for the new columns. Account running
    the script must have permission to change structure of the table. 
    '''
    table_columns = table_column_metadata(file_code)
    if table_columns:
        not_in_table = [c for c in file_frame_columns if c not in table_columns]
    else:
        not_in_table = None
    # not_in_file = [c for c in table_columns if c not in file_frame.columns]
    return not_in_table

def del_current_year(file_year, file_code):
    '''
    Delete's data in an existing table for the specified year. 
    Verify table exists before calling
    '''
    del_year = f'DELETE FROM {settings.TARGET_SCHEMA}tbl{file_code} WHERE SURVEY_YEAR = {str(file_year)}'
    print(f'{file_year} data deleted from tbl{file_code}', end='...')
    settings.ENGINE.execute(del_year)

def table_exists(file_code):
    '''
    check if target table exists in the existing database
    '''
    sql = f"SELECT CASE WHEN EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tbl{file_code}') THEN 'True' ELSE 'False' END"
    result = settings.ENGINE.execute(sql)

    exists = result.first()

    if exists:
        table_status = True if exists[0] == 'True' else False

    return table_status

def add_missing_columns_to_table(missing_columns, file_code, ENGINE):
    '''
    If any columns exist in dataframe that are not in table
    adds VARCHAR(MAX) columns to table to account for data
    '''
    for new_col in missing_columns:
        col_length = 500
        if file_code == "HD": col_length=2000
        add_column_sql = f'ALTER TABLE {settings.TARGET_SCHEMA}tbl{file_code} ADD {new_col} VARCHAR({col_length}) NULL'
        settings.ENGINE.execute(add_column_sql)
        # print(add_column_sql)
            
def create_table(file_code, columns):
    '''
    Create the ETL DB table for the specified file code with varchar(500) columns. 
    Prevents the varchar(max) columns of the default write
    '''
    create_sql = f'CREATE TABLE {settings.TARGET_SCHEMA}tbl{file_code}(\n'
    for column in columns:
        if column.upper() in ['SURVEY_YEAR','UNITID']:
            create_sql += f'{column.upper()} INT not null, \n'
        else:
            if file_code == 'HD':
                create_sql += f'{column.upper()} varchar(2000) null, \n'
            else:
                create_sql += f'{column.upper()} varchar(500) null, \n'
    create_sql = create_sql[:-3] + '\n )'
    settings.ENGINE.execute(create_sql)
    
def write_years_to_table(file_code, file_code_log, row_test=None):
    '''
    Write one file to the existing table; check if all columns exist first, add if missing
    '''
    for file_year in file_code_log:
        if file_code_log[file_year] != 'File Not Found':
            file_frame = pd.read_csv(file_code_log[file_year]
                                     , dtype=object
                                     , nrows=row_test
                                     , encoding=settings.READ_ENCODING
                                     , index_col=False
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
            # 
            if table_exists(file_code):
                missing = compare_file_table(file_code, file_frame.columns)
                if missing:
                    add_missing_columns_to_table(missing, file_code, settings.ENGINE)
                del_current_year(file_year, file_code)
            else:
                create_table(file_code, file_frame.columns)
                missing = None
            file_frame.to_sql('tbl'+file_code, settings.ENGINE, schema=settings.TARGET_SCHEMA, if_exists='append', index=False)
            print(file_year, 'written.', 'Columns Added:', missing)

def __main__():
    '''
    Set file_code, survey, log_date
    '''
    surveys = settings.DOWNLOAD_SURVEY_FILE_LIST
    log_date = '2018-09-03'

    for survey in surveys:
        for file_code in surveys[survey]:
            log_file = f'{survey}_{file_code}_{log_date}_log.json'
            log_path = os.path.join(settings.DATA_DIRECTORY, survey, file_code, log_date, log_file)
            if os.path.exists(log_path):
                with open(os.path.join(log_path), 'r') as f:
                    log = json.load(f)
                    write_years_to_table(file_code, log[survey][file_code])
            else:
                print(f'No log file for {survey} {file_code} for {log_date}')

__main__()